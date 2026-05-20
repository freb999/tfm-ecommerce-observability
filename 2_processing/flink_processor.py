"""
================================================================================
 TFM — Arquitectura de Stream Processing para Detección de Anomalías en E-commerce
 Módulo : flink_processor.py
 Capa   : 2 — Procesamiento en Flujo e Ingeniería de Características
 Autores: Benjamín Romero Fonseca / Luis Canales Quiñones
 Versión: 2.0.0
 Python : 3.10.x
 Flink  : 1.18.1

 Descripción:
     Orquestador analítico basado en Apache Flink que consume eventos crudos
     desde Apache Kafka, ejecuta validación de esquemas, previene fallos por
     datos malformados y agrupa el tráfico en Ventanas de Volteo (Tumbling Windows)
     procesando métricas complejas por sesión para alimentar el modelo de ML.

 Uso:
     python 2_processing/flink_processor.py
     python 2_processing/flink_processor.py --window-size 15 --bootstrap-servers localhost:9094
================================================================================
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.watermark_strategy import WatermarkStrategy

# ──────────────────────────────────────────────────────────────────────────────
# Configuración del Sistema de Logging Empresarial
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Configuración Dinámica del Job (Inmutabilidad)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class JobConfig:
    """Mantiene los parámetros de configuración global del pipeline de Flink."""
    bootstrap_servers: str
    source_topic: str
    group_id: str
    window_size_seconds: int
    parallelism: int
    jar_name: str = "flink-sql-connector-kafka-3.0.2-1.18.jar"


# ──────────────────────────────────────────────────────────────────────────────
# Transformaciones y Funciones de Extracción (UDFs conceptuales)
# ──────────────────────────────────────────────────────────────────────────────
def safe_deserialize_and_map(json_str: str) -> tuple:
    """
    Deserializa de forma segura el JSON crudo proveniente de Kafka.
    Aplica un patrón de tolerancia a fallos: si el JSON está roto o faltan campos,
    captura la excepción, la registra en el log y emite una tupla de descarte
    evitando que el pipeline de procesamiento en tiempo real colapse.
    
    Retorna una tupla estructurada bajo el esquema intermedio de Flink.
    """
    try:
        payload = json.loads(json_str)
        
        # Extracción segura de tipos y normalización de variables analíticas
        session_id = str(payload.get("session_id", ""))
        user_id = str(payload.get("user_id", "anonymous"))
        ip_address = str(payload.get("ip_address", "0.0.0.0"))
        device = str(payload.get("device", "unknown"))
        event_type = str(payload.get("event_type", "page_view"))
        latency = int(payload.get("latency", 0))
        
        if not session_id:
            return ("CORRUPT_RECORD", "", "", "", 0, 0, 0, 0, 0, 0)
            
        # Contadores individuales para la agregación por tipos de evento
        is_page_view = 1 if event_type == "page_view" else 0
        is_cart_add = 1 if event_type == "add_to_cart" else 0
        is_checkout = 1 if event_type == "checkout_start" else 0
        is_purchase = 1 if event_type == "purchase" else 0
        
        return (
            session_id, user_id, ip_address, device,
            1,            # Contador general de eventos (clicks_total)
            latency,      # Latencia acumulada
            is_page_view, # Sumador específico page_view
            is_cart_add,  # Sumador específico add_to_cart
            is_checkout,  # Sumador específico checkout_start
            is_purchase   # Sumador específico purchase
        )
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as ex:
        logger.warning("Registro malformado ignorado en el flujo de entrada: %s. Detalle: %s", json_str, ex)
        return ("CORRUPT_RECORD", "", "", "", 0, 0, 0, 0, 0, 0)


def reduce_session_metrics(v1: tuple, v2: tuple) -> tuple:
    """
    Función de Reducción en Flujo. Suma de manera incremental los vectores
    numéricos de dos eventos pertenecientes a la misma ventana y sesión.
    Mantiene constantes las propiedades estructurales (IP, usuario, dispositivo).
    """
    return (
        v1[0], # session_id
        v1[1], # user_id
        v1[2], # ip_address
        v1[3], # device
        v1[4] + v2[4],   # clicks_total
        v1[5] + v2[5],   # latency_total
        v1[6] + v2[6],   # page_views_total
        v1[7] + v2[7],   # cart_adds_total
        v1[8] + v2[8],   # checkouts_total
        v1[9] + v2[9]    # purchases_total
    )


def build_final_feature_vector(reduced_tuple: tuple) -> str:
    """
    Formatea el resultado consolidado de la ventana y calcula métricas
    derivadas promedio. Genera el vector final de características en formato JSON,
    dejando el flujo listo para la Capa 3 (Detección de Anomalías).
    """
    (session_id, user_id, ip_address, device, clicks, total_lat, 
     p_views, c_adds, checkouts, purchases) = reduced_tuple
    
    # Prevenir división por cero de forma segura
    avg_latency = round(total_lat / clicks, 2) if clicks > 0 else 0.0
    
    feature_vector = {
        "session_id": session_id,
        "user_id": user_id,
        "ip_address": ip_address,
        "device": device,
        "feature_metrics": {
            "clicks_count": clicks,
            "avg_latency_ms": avg_latency,
            "page_views_count": p_views,
            "cart_adds_count": c_adds,
            "checkout_starts_count": checkouts,
            "purchases_count": purchases
        }
    }
    return json.dumps(feature_vector, ensure_ascii=False)


# ──────────────────────────────────────────────────────────────────────────────
# Arquitectura del Pipeline Principal (Engine)
# ──────────────────────────────────────────────────────────────────────────────
class ECommerceStreamProcessor:
    """Motor encargado de inicializar, orquestar y ejecutar el flujo analítico de Flink."""
    
    def __init__(self, config: JobConfig):
        self.config = config
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self._setup_environment()

    def _setup_environment(self) -> None:
        """Configura los parámetros del clúster Flink e inyecta dependencias Java."""
        self.env.set_parallelism(self.config.parallelism)
        
        # Localización dinámica y carga del conector JAR de Kafka
        base_dir = os.path.dirname(os.path.abspath(__file__))
        jar_absolute_path = os.path.join(base_dir, self.config.jar_name)
        
        if not os.path.exists(jar_absolute_path):
            logger.critical("No se encontró el archivo conector requerido: %s", jar_absolute_path)
            logger.critical("Por favor, valida el paso del Sprint 2 sobre descargas de conectores.")
            sys.exit(1)
            
        self.env.add_jars(f"file://{jar_absolute_path}")
        logger.info("Entorno Flink inicializado con el JAR cargado: %s", self.config.jar_name)

    def _build_kafka_source(self) -> KafkaSource:
        """Instancia el conector de entrada (Source) hacia el clúster de Kafka."""
        return KafkaSource.builder() \
            .set_bootstrap_servers(self.config.bootstrap_servers) \
            .set_topics(self.config.source_topic) \
            .set_group_id(self.config.group_id) \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

    def pipeline_orquestator(self) -> None:
        """Construye la topología del grafo de procesamiento de eventos dirigido."""
        logger.info("Construyendo grafo analítico de procesamiento en flujo...")
        
        # 1. Registro del Origen de datos (Consumer)
        kafka_source = self._build_kafka_source()
        raw_stream = self.env.from_source(
            kafka_source, 
            WatermarkStrategy.no_watermarks(), 
            "Kafka_ECommerce_Source"
        )
        
        # Definición estricta de tipos intermedios para optimizar la serialización JVM-Python
        intermediate_type_schema = Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT()
        ])

        # 2. Capa de Deserialización y Limpieza de Esquema
        parsed_stream = raw_stream.map(
            safe_deserialize_and_map, 
            output_type=intermediate_type_schema
        ).filter(lambda record: record[0] != "CORRUPT_RECORD")

        # 3. Operación de Enrutamiento y Agrupación por Clave Lógica (session_id)
        keyed_stream = parsed_stream.key_by(lambda record: record[0])

        # 4. Estrategia de Ventanas Temporales Fijas (Tumbling Windows)
        windowed_stream = keyed_stream.window(
            TumblingProcessingTimeWindows.of(Time.seconds(self.config.window_size_seconds))
        )

        # 5. Reducción e Inferencia Incremental de Atributos
        reduced_stream = windowed_stream.reduce(
            reduce_session_metrics,
            output_type=intermediate_type_schema
        )

        # 6. Formateo y Generación del Vector Final Corporativo
        final_feature_stream = reduced_stream.map(
            build_final_feature_vector, 
            output_type=Types.STRING()
        )

        # 7. Salida del Flujo de Datos (Sink de Monitoreo)
        final_feature_stream.print()
        
        # 8. Envío de la topología al clúster para su ejecución reactiva
        logger.info("Ecosistema preparado. Enviando Job al motor de Flink...")
        self.env.execute("TFM_ECommerce_Observability_Pipeline")


# ──────────────────────────────────────────────────────────────────────────────
# Punto de entrada de la aplicación
# ──────────────────────────────────────────────────────────────────────────────
def parse_arguments() -> argparse.Namespace:
    """Gestiona los argumentos de configuración por línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Capa 2: Motor Core Flink de Ingeniería de Atributos — TFM"
    )
    parser.add_argument(
        "--bootstrap-servers", default="localhost:9094",
        help="Instancias del broker de Kafka (default: localhost:9094)"
    )
    parser.add_argument(
        "--topic", default="ecommerce-events",
        help="Tópico origen del clickstream (default: ecommerce-events)"
    )
    parser.add_argument(
        "--group-id", default="tfm-analytics-group-prod",
        help="Consumer Group oficial del Job (default: tfm-analytics-group-prod)"
    )
    parser.add_argument(
        "--window-size", type=int, default=10,
        help="Ancho de la ventana temporal en segundos (default: 10)"
    )
    parser.add_argument(
        "--parallelism", type=int, default=1,
        help="Grado de paralelismo de procesamiento del Job (default: 1)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    
    # Inicialización del objeto inmutable de configuración corporativa
    job_config = JobConfig(
        bootstrap_servers=args.bootstrap_servers,
        source_topic=args.topic,
        group_id=args.group_id,
        window_size_seconds=args.window_size,
        parallelism=args.parallelism
    )
    
    # Instanciación y arranque del procesador
    processor = ECommerceStreamProcessor(job_config)
    processor.pipeline_orquestator()