"""
================================================================================
 TFM — Arquitectura de Stream Processing para Detección de Anomalías en E-commerce
 Módulo : anomaly_detector.py
 Capa   : 3 — Inteligencia y Detección Híbrida (Reglas + Machine Learning)
 Autores: Benjamín Romero Fonseca / Luis Canales Quiñones
 Versión: 1.0.0
 Python : 3.10.x
 Deps   : scikit-learn kafka-python-ng numpy

 Descripción:
     Microservicio reactivo que consume vectores de características de Kafka.
     Aplica un enfoque híbrido: Reglas deterministas de negocio en paralelo con
     el algoritmo Isolation Forest de Machine Learning No Supervisado.
     Las anomalías detectadas son enviadas a un tópico exclusivo de alertas.
================================================================================
"""

import json
import logging
import sys
import os
import csv
import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from kafka import KafkaConsumer, KafkaProducer
from sklearn.ensemble import IsolationForest

# ──────────────────────────────────────────────────────────────────────────────
# Configuración de Logging Profesional
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(filename)s) — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Configuración del Servicio de Detección
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class DetectorConfig:
    bootstrap_servers: List[str] = ("localhost:9094",)
    source_topic: str = "retail-feature-vectors"
    alerts_topic: str = "ecommerce-alerts"
    group_id: str = "tfm-ml-detector-group"
    warmup_samples_required: int = 40  # Cantidad de ventanas para entrenar el ML


# ──────────────────────────────────────────────────────────────────────────────
# Core Engine: Detector Híbrido
# ──────────────────────────────────────────────────────────────────────────────
class HybridAnomalyDetector:
    """Engine encargado de fusionar reglas de negocio con inferencia de Machine Learning."""
    
    def __init__(self, config: DetectorConfig):
        self.config = config
        
        # Inicialización del algoritmo Isolation Forest (Contaminación estimada al 8%)
        self.model = IsolationForest(contamination=0.08, random_state=42)
        self.is_model_trained = False
        
        # Buffer en memoria para la fase de calibración inicial
        self.warmup_buffer: List[List[float]] = []
        
        # Conexiones de red Kafka
        self.consumer: Optional[KafkaConsumer] = None
        self.producer: Optional[KafkaProducer] = None

    def connect_infrastructure(self) -> None:
        """Establece los sockets de conexión con el clúster de mensajería."""
        try:
            self.consumer = KafkaConsumer(
                self.config.source_topic,
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.config.group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest'
            )
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Conexión exitosa a Kafka. Escuchando características en '%s'...", self.config.source_topic)
        except Exception as e:
            logger.critical("Fallo catastrófico al conectar con la infraestructura: %s", e)
            sys.exit(1)

    def _evaluate_business_rules(self, data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Capa Heurística: Evalúa reglas deterministas del negocio.
        Garantiza detección inmediata sin requerir procesamiento estadístico.
        """
        device = data.get("device", "unknown")
        metrics = data.get("feature_metrics", {})
        avg_latency = metrics.get("avg_latency_ms", 0.0)
        clicks = metrics.get("clicks_count", 0)
        
        # Regla 1: Identificación explícita de bots por firma de dispositivo
        if device == "bot_scraper":
            return True, "CRITICAL_RULE: Bot corporativo o scraper detectado por firma"
            
        # Regla 2: Degradación crítica del rendimiento de la plataforma
        if avg_latency > 2000.0:
            return True, "PERFORMANCE_RULE: Latencia crítica promedio superior a 2000ms"
            
        # Regla 3: Tasa de peticiones inhumanas en una sola ventana de 10s
        if clicks > 35:
            return True, "SECURITY_RULE: Denegación de servicio potencial (>35 clicks/10s)"
            
        return False, None

    def _extract_feature_vector(self, metrics: Dict) -> List[float]:
        """Transforma el payload JSON en el vector numérico plano exigido por Scikit-Learn."""
        return [
            float(metrics.get("clicks_count", 0)),
            float(metrics.get("avg_latency_ms", 0.0)),
            float(metrics.get("page_views_count", 0)),
            float(metrics.get("cart_adds_count", 0)),
            float(metrics.get("checkout_starts_count", 0)),
            float(metrics.get("purchases_count", 0))
        ]

    def process_stream(self) -> None:
        """Bucle infinito de procesamiento y clasificación reactiva en flujo."""
        logger.info("Iniciando análisis híbrido...")
        
        for message in self.consumer:
            payload = message.value
            session_id = payload.get("session_id")
            metrics = payload.get("feature_metrics", {})
            
            # 1. Evaluación por Capa de Reglas de Negocio
            rule_triggered, rule_reason = self._evaluate_business_rules(payload)
            
            # Extraer características para la capa de Machine Learning
            current_vector = self._extract_feature_vector(metrics)
            
            ml_triggered = False
            ml_score = 0.0
            
            # 2. Capa de Machine Learning (Isolation Forest)
            if not self.is_model_trained:
                # Fase de Calibración
                self.warmup_buffer.append(current_vector)
                samples_count = len(self.warmup_buffer)
                
                if samples_count % 5 == 0:
                    logger.info("Calibrando Isolation Forest: [%d/%d] vectores recolectados.", 
                                samples_count, self.config.warmup_samples_required)
                    
                if samples_count >= self.config.warmup_samples_required:
                    self.model.fit(self.warmup_buffer)
                    self.is_model_trained = True
                    self.warmup_buffer.clear() # Liberar memoria
                    logger.info("¡Fase de Calibración Completada! Isolation Forest activo en producción.")
            else:
                # Inferencia en tiempo real sobre el vector de 6 dimensiones
                vector_np = np.array([current_vector])
                prediction = self.model.predict(vector_np)[0] # Retorna 1 (normal) o -1 (anomalía)
                ml_score = float(self.model.score_samples(vector_np)[0])
                
                if prediction == -1:
                    ml_triggered = True

            # 3. Orquestación del estado de Alerta Híbrida
            if rule_triggered or ml_triggered:
                alert_payload = {
                    "timestamp": payload.get("timestamp"),
                    "session_id": session_id,
                    "user_id": payload.get("user_id"),
                    "ip_address": payload.get("ip_address"),
                    "device": payload.get("device"),
                    "metrics": metrics,
                    "detection_source": {
                        "rule_layer_triggered": rule_triggered,
                        "rule_reason": rule_reason,
                        "ml_layer_triggered": ml_triggered,
                        "ml_anomaly_score": round(ml_score, 4)
                    }
                }
                
                # Emitir Alerta estructurada hacia Kafka
                self.producer.send(self.config.alerts_topic, key=session_id.encode('utf-8'), value=alert_payload)
                
                # ── NUEVO: Persistencia en CSV para cumplir rúbrica del TFM ──
                os.makedirs("data", exist_ok=True)
                csv_file = "data/historical_alerts.csv"
                file_exists = os.path.isfile(csv_file)
                
                with open(csv_file, mode="a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        # Escribir la cabecera si el archivo es nuevo
                        writer.writerow(["Timestamp", "Session_ID", "User_ID", "Device", "Clicks", "Avg_Latency_ms", "Rule_Triggered", "ML_Triggered", "ML_Score"])
                    
                    # Escribir la fila con los datos de la alerta
                    writer.writerow([
                        payload.get("timestamp"),
                        session_id,
                        payload.get("user_id"),
                        payload.get("device"),
                        metrics.get("clicks_count"),
                        metrics.get("avg_latency_ms"),
                        rule_triggered,
                        ml_triggered,
                        round(ml_score, 4) if ml_triggered else 0.0
                    ])
                # ─────────────────────────────────────────────────────────────
                
                # Print formateado en consola para auditoría visual
                source_str = "REGLA" if rule_triggered else "MACHINE LEARNING"
                logger.warning("🚨 [ALERTA - %s] Sesión: %s | Clicks: %d | Latencia Media: %.2fms", 
                               source_str, session_id, metrics.get("clicks_count"), metrics.get("avg_latency_ms"))

    def close(self) -> None:
        if self.consumer: self.consumer.close()
        if self.producer: self.producer.close()
        logger.info("Conexiones del detector cerradas limpiamente.")


if __name__ == "__main__":
    detector_config = DetectorConfig()
    detector = HybridAnomalyDetector(detector_config)
    detector.connect_infrastructure()
    
    try:
        detector.process_stream()
    except KeyboardInterrupt:
        logger.info("Servicio detenido por el usuario.")
    finally:
        detector.close()
