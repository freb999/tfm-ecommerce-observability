"""
================================================================================
 TFM — Arquitectura de Stream Processing para Detección de Anomalías en E-commerce
 Módulo : faker_producer.py
 Capa   : 1 — Generación de Eventos Sintéticos
 Autores: Benjamín Romero Fonseca / Luis Canales Quiñones
 Versión: 1.1.0
 Python : 3.11+
 Deps   : faker==24.x  kafka-python-ng==2.2.x

 Instalación:
     pip install Faker kafka-python-ng

 Uso:
     python 1_ingestion/faker_producer.py
     python 1_ingestion/faker_producer.py --eps 100 --sessions 50 --anomaly-ratio 0.10
================================================================================
"""

import argparse
import json
import logging
import random
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

from faker import Faker
from kafka import KafkaProducer

# ──────────────────────────────────────────────────────────────────────────────
# Configuración de logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Constantes del dominio e-commerce
# ──────────────────────────────────────────────────────────────────────────────

# Grafo de transición normal: página_origen → [páginas_destino_posibles]
NAVIGATION_GRAPH: dict[str, list[str]] = {
    "/":                    ["/categorias/zapatillas", "/categorias/ropa", "/ofertas", "/buscar"],
    "/categorias/zapatillas": ["/producto/zap-001", "/producto/zap-002", "/producto/zap-003", "/buscar"],
    "/categorias/ropa":     ["/producto/ropa-010", "/producto/ropa-011", "/buscar"],
    "/ofertas":             ["/producto/zap-002", "/producto/ropa-010"],
    "/buscar":              ["/producto/zap-001", "/producto/zap-003", "/producto/ropa-011"],
    "/producto/zap-001":   ["/carrito", "/categorias/zapatillas"],
    "/producto/zap-002":   ["/carrito", "/producto/zap-001"],
    "/producto/zap-003":   ["/carrito", "/categorias/zapatillas"],
    "/producto/ropa-010":  ["/carrito", "/categorias/ropa"],
    "/producto/ropa-011":  ["/carrito", "/categorias/ropa"],
    "/carrito":            ["/checkout", "/categorias/zapatillas", "/"],
    "/checkout":           ["/confirmacion"],
    "/confirmacion":       [],  # nodo terminal
}

# ──────────────────────────────────────────────────────────────────────────────
# Esquema del evento (dataclass → JSON)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ClickstreamEvent:
    timestamp:  str
    session_id: str
    user_id:    str
    ip_address: str          # <- Nuevo: Crucial para ML
    device:     str          # <- Nuevo: Crucial para ML
    event_type: str
    page_url:   str
    latency:    int
    is_anomaly: bool = False
    anomaly_type: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)


# ──────────────────────────────────────────────────────────────────────────────
# Generadores de sesión
# ──────────────────────────────────────────────────────────────────────────────

class SessionGenerator:
    """
    Mantiene el estado de una sesión de usuario. 
    Garantiza que la IP y el dispositivo sean consistentes durante toda la navegación.
    """
    def __init__(self, faker: Faker, anomaly_ratio: float = 0.08):
        self.faker         = faker
        self.session_id    = str(uuid.uuid4())
        self.user_id       = f"usr_{random.randint(1, 99999):05d}"
        
        # Propiedades estáticas de la sesión (esenciales para Isolation Forest)
        self.ip_address    = faker.ipv4()
        self.device        = random.choice(["desktop", "mobile", "tablet"])
        
        self.current_page  = "/"
        self.event_count   = 0
        self.max_events    = random.randint(4, 15)

        self.is_anomaly    = random.random() < anomaly_ratio
        self.anomaly_type  = self._pick_anomaly_type() if self.is_anomaly else None

    def _pick_anomaly_type(self) -> str:
        return random.choice(["bot", "checkout_drop", "high_latency"])

    def _normal_latency(self) -> int:
        latency = int(random.gauss(120, 30))
        return max(20, min(latency, 800))

    def _next_page_normal(self) -> str:
        neighbors = NAVIGATION_GRAPH.get(self.current_page, [])
        return random.choice(neighbors) if neighbors else self.current_page

    def _generate_bot_event(self) -> ClickstreamEvent:
        bot_urls = ["/producto/zap-001", "/producto/zap-002", "/buscar"]
        return ClickstreamEvent(
            timestamp    = datetime.now(timezone.utc).isoformat(),
            session_id   = self.session_id,
            user_id      = self.user_id,
            ip_address   = self.ip_address,
            device       = "bot_scraper", # Marca clara para evaluación del modelo
            event_type   = "page_view",
            page_url     = random.choice(bot_urls),
            latency      = random.randint(1, 18),
            is_anomaly   = True,
            anomaly_type = "bot",
        )

    def _generate_checkout_drop_event(self) -> ClickstreamEvent:
        if self.current_page != "/checkout":
            self.current_page = "/carrito" if self.event_count < 2 else "/checkout"

        return ClickstreamEvent(
            timestamp    = datetime.now(timezone.utc).isoformat(),
            session_id   = self.session_id,
            user_id      = self.user_id,
            ip_address   = self.ip_address,
            device       = self.device,
            event_type   = "checkout_start",
            page_url     = self.current_page,
            latency      = self._normal_latency(),
            is_anomaly   = True,
            anomaly_type = "checkout_drop",
        )

    def _generate_high_latency_event(self) -> ClickstreamEvent:
        next_page = self._next_page_normal()
        self.current_page = next_page
        return ClickstreamEvent(
            timestamp    = datetime.now(timezone.utc).isoformat(),
            session_id   = self.session_id,
            user_id      = self.user_id,
            ip_address   = self.ip_address,
            device       = self.device,
            event_type   = "page_view",
            page_url     = next_page,
            latency      = random.randint(2000, 8000),
            is_anomaly   = True,
            anomaly_type = "high_latency",
        )

    def next_event(self) -> Optional[ClickstreamEvent]:
        if self.event_count >= self.max_events:
            return None 

        self.event_count += 1

        if self.is_anomaly:
            if self.anomaly_type == "bot": return self._generate_bot_event()
            if self.anomaly_type == "checkout_drop": return self._generate_checkout_drop_event()
            if self.anomaly_type == "high_latency": return self._generate_high_latency_event()

        next_page = self._next_page_normal()
        self.current_page = next_page

        event_type = "page_view"
        if "carrito" in next_page: event_type = "add_to_cart"
        elif "checkout" in next_page: event_type = "checkout_start"
        elif "confirmacion" in next_page: event_type = "purchase"
        elif "buscar" in next_page: event_type = "search"

        return ClickstreamEvent(
            timestamp  = datetime.now(timezone.utc).isoformat(),
            session_id = self.session_id,
            user_id    = self.user_id,
            ip_address = self.ip_address,
            device     = self.device,
            event_type = event_type,
            page_url   = next_page,
            latency    = self._normal_latency(),
        )

    @property
    def is_done(self) -> bool:
        return self.event_count >= self.max_events


# ──────────────────────────────────────────────────────────────────────────────
# Productor Kafka y Loop Principal
# ──────────────────────────────────────────────────────────────────────────────

def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        value_serializer  = lambda v: v.encode("utf-8"),
        key_serializer    = lambda k: k.encode("utf-8"),
        acks              = "all",
        retries           = 5,
        linger_ms         = 5,
        compression_type  = "gzip",
    )

def on_send_error(exc: Exception) -> None:
    log.error("Error al publicar evento en Kafka: %s", exc)

def run(bootstrap_servers: str, topic: str, events_per_second: int, concurrent_sessions: int, anomaly_ratio: float) -> None:
    faker = Faker()
    try:
        producer = build_producer(bootstrap_servers)
    except Exception as e:
        log.error(f"Error conectando a Kafka. ¿Están arriba los contenedores? Detalle: {e}")
        return

    sessions = [SessionGenerator(faker, anomaly_ratio) for _ in range(concurrent_sessions)]
    interval = 1.0 / events_per_second
    total_sent, anomalies_sent = 0, 0

    log.info("🚀 Iniciando generador | eps=%d | sesiones=%d | anomaly_ratio=%.2f | topic=%s", events_per_second, concurrent_sessions, anomaly_ratio, topic)
    log.info("Presiona Ctrl+C para detener.\n")

    try:
        while True:
            t_start = time.monotonic()
            session = random.choice(sessions)
            event   = session.next_event()

            if event is None or session.is_done:
                sessions[sessions.index(session)] = SessionGenerator(faker, anomaly_ratio)
                continue

            producer.send(topic, key=event.session_id, value=event.to_json()).add_errback(on_send_error)
            total_sent += 1
            if event.is_anomaly: anomalies_sent += 1

            if total_sent % 1000 == 0:
                log.info("Enviados: %d eventos | Anomalías: %d (%.1f%%)", total_sent, anomalies_sent, (anomalies_sent / total_sent * 100))

            sleep_t = interval - (time.monotonic() - t_start)
            if sleep_t > 0: time.sleep(sleep_t)

    except KeyboardInterrupt:
        log.info("🛑 Simulación detenida por el usuario.")
    finally:
        producer.flush()
        producer.close()
        log.info("Total enviado: %d eventos | Anomalías: %d", total_sent, anomalies_sent)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generador de clickstream sintético — TFM Stream Processing")
    parser.add_argument("--bootstrap-servers", default="localhost:9094", help="Dirección del broker Kafka (default: localhost:9094)")
    parser.add_argument("--topic", default="ecommerce-events", help="Topic de Kafka destino (default: ecommerce-events)")
    parser.add_argument("--eps", type=int, default=10, help="Eventos por segundo (default: 10)")
    parser.add_argument("--sessions", type=int, default=10, help="Sesiones concurrentes activas (default: 10)")
    parser.add_argument("--anomaly-ratio", type=float, default=0.08, help="Proporción de sesiones anómalas (default: 0.08)")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run(args.bootstrap_servers, args.topic, args.eps, args.sessions, args.anomaly_ratio)