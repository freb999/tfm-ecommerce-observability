"""
================================================================================
 TFM — Arquitectura de Stream Processing para Detección de Anomalías en E-commerce
 Módulo : flink_processor.py
 Capa   : 2 — Procesamiento en Flujo e Ingeniería de Características (Con Sinks Múltiples)
 Autores: Benjamín Romero Fonseca / Luis Canales Quiñones
 Versión: 2.1.0
 Python : 3.10.x
 Flink  : 1.18.1

 Descripción:
     Consume eventos crudos de Kafka, calcula vectores de características en 
     ventanas de 10 segundos y ejecuta una salida doble: imprime en consola para 
     auditoría local y publica en un nuevo topic de Kafka para el Isolation Forest.
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
from pyflink.datastream.connectors.kafka import (
    KafkaSource, 
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema
)
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.watermark_strategy import WatermarkStrategy

# ──────────────────────────────────────────────────────────────────────────────
# Configuración del Sistema de Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Configuración Dinámica del Job
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class JobConfig:
    bootstrap_servers: str
    source_topic: str
    sink_topic: str  # <- Nuevo: Topic destino para el modelo de ML
    group_id: str
    window_size_seconds: int
    parallelism: int
    jar_name: str = "flink-sql-connector-kafka-3.0.2-1.18.jar"


# ──────────────────────────────────────────────────────────────────────────────
# Transformaciones Lógicas
# ──────────────────────────────────────────────────────────────────────────────
def safe_deserialize_and_map(json_str: str) -> tuple:
    try:
        payload = json.loads(json_str)
        session_id = str(payload.get("session_id", ""))
        user_id = str(payload.get("user_id", "anonymous"))
        ip_address = str(payload.get("ip_address", "0.0.0.0"))
        device = str(payload.get("device", "unknown"))
        event_type = str(payload.get("event_type", "page_view"))
        latency = int(payload.get("latency", 0))
        
        if not session_id:
            return ("CORRUPT_RECORD", "", "", "", 0, 0, 0, 0, 0, 0)
            
        is_page_view = 1 if event_type == "page_view" else 0
        is_cart_add = 1 if event_type == "add_to_cart" else 0
        is_checkout = 1 if event_type == "checkout_start" else 0
        is_purchase = 1 if event_type == "purchase" else 0
        
        return (
            session_id, user_id, ip_address, device,
            1, latency, is_page_view, is_cart_add, is_checkout, is_purchase
        )
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as ex:
        logger.warning("Registro malformado ignorado: %s. Detalle: %s", json_str, ex)
        return ("CORRUPT_RECORD", "", "", "", 0, 0, 0, 0, 0, 0)


def reduce_session_metrics(v1: tuple, v2: tuple) -> tuple:
    return (
        v1[0], v1[1], v1[2], v1[3],
        v1[4] + v2[4], v1[5] + v2[5], v1[6] + v2[6], v1[7] + v2[7], v1[8] + v2[8], v1[9] + v2[9]
    )


def build_final_feature_vector(reduced_tuple: tuple) -> str:
    (session_id, user_id, ip_address, device, clicks, total_lat, 
     p_views, c_adds, checkouts, purchases) = reduced_tuple
    
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
# Arquitectura del Pipeline Principal
# ──────────────────────────────────────────────────────────────────────────────
class ECommerceStreamProcessor:
    
    def __init__(self, config: JobConfig):
        self.config = config
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self._setup_environment()

    def _setup_environment(self) -> None:
        self.env.set_parallelism(self.config.parallelism)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        jar_absolute_path = os.path.join(base_dir, self.config.jar_name)
        
        if not os.path.exists(jar_absolute_path):
            logger.critical("No se encontró el conector JAR: %s", jar_absolute_path)
            sys.exit(1)
            
        self.env.add_jars(f"file://{jar_absolute_path}")
        logger.info("Entorno Flink listo con JAR de Kafka cargado.")

    def _build_kafka_source(self) -> KafkaSource:
        return KafkaSource.builder() \
            .set_bootstrap_servers(self.config.bootstrap_servers) \
            .set_topics(self.config.source_topic) \
            .set_group_id(self.config.group_id) \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

    def _build_kafka_sink(self) -> KafkaSink:
        """
        <- Nuevo método: Construye el conector de salida hacia Kafka.
        Serializa las cadenas JSON de las características usando SimpleStringSchema.
        """
        return KafkaSink.builder() \
            .set_bootstrap_servers(self.config.bootstrap_servers) \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder() \
                    .set_topic(self.config.sink_topic) \
                    .set_value_serialization_schema(SimpleStringSchema()) \
                    .build()
            ) \
            .build()

    def pipeline_orquestator(self) -> None:
        logger.info("Construyendo topología analítica reactiva...")
        
        # 1. Source (Entrada de eventos de e-commerce)
        kafka_source = self._build_kafka_source()
        raw_stream = self.env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka_Source")
        
        intermediate_type_schema = Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT()
        ])

        # 2. Map & Filter (Limpieza de datos)
        parsed_stream = raw_stream.map(
            safe_deserialize_and_map, 
            output_type=intermediate_type_schema
        ).filter(lambda record: record[0] != "CORRUPT_RECORD")

        # 3. KeyBy (Agrupación lógica por sesión)
        keyed_stream = parsed_stream.key_by(lambda record: record[0])

        # 4. Window (Ventanas de procesamiento de 10 segundos)
        windowed_stream = keyed_stream.window(
            TumblingProcessingTimeWindows.of(Time.seconds(self.config.window_size_seconds))
        )

        # 5. Reduce (Agregación de métricas en memoria JVM)
        reduced_stream = windowed_stream.reduce(reduce_session_metrics, output_type=intermediate_type_schema)

        # 6. Map Final (Generación del Vector de Características JSON)
        final_feature_stream = reduced_stream.map(build_final_feature_vector, output_type=Types.STRING())

        # ──────────────────────────────────────────────────────────────────────
        # ⚡ SALIDA DOBLE (BIFURCACIÓN DEL FLUJO)
        # ──────────────────────────────────────────────────────────────────────
        
        # Salida A: Consola local para auditoría y desarrollo visual
        final_feature_stream.print()
        
        # Salida B: Inyección directa al nuevo topic de Kafka para el Isolation Forest
        kafka_sink = self._build_kafka_sink()
        final_feature_stream.sink_to(kafka_sink)
        
        # ──────────────────────────────────────────────────────────────────────
        
        logger.info("Topología configurada con salida doble (Consola + Kafka Sink). Ejecutando Job...")
        self.env.execute("TFM_ECommerce_Observability_Pipeline_With_Sink")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capa 2: Motor Core Flink de Ingeniería de Atributos — TFM")
    parser.add_argument("--bootstrap-servers", default="localhost:9094", help="Broker de Kafka")
    parser.add_argument("--source-topic", default="ecommerce-events", help="Topic de lectura cruda")
    parser.add_argument("--sink-topic", default="retail-feature-vectors", help="Topic de salida procesada")
    parser.add_argument("--group-id", default="tfm-analytics-group-prod", help="Consumer Group")
    parser.add_argument("--window-size", type=int, default=10, help="Ventana en segundos")
    parser.add_argument("--parallelism", type=int, default=1, help="Paralelismo")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    
    job_config = JobConfig(
        bootstrap_servers=args.bootstrap_servers,
        source_topic=args.source_topic,
        sink_topic=args.sink_topic, # Asignación del nuevo parámetro
        group_id=args.group_id,
        window_size_seconds=args.window_size,
        parallelism=args.parallelism
    )
    
    processor = ECommerceStreamProcessor(job_config)
    processor.pipeline_orquestator()