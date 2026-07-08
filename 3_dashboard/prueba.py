"""
================================================================================
 TFM — Arquitectura de Stream Processing para Detección de Anomalías en E-commerce
 Módulo : app.py
 Capa   : 3 — Interfaz Gráfica y Monitoreo Analítico en Tiempo Real
 Autores: Benjamín Romero Fonseca / Luis Canales Quiñones
 Versión: 1.1.1 (Grafo Integrado + Recuperación de BD Histórica + Saneado)
================================================================================
"""

import json
import logging
import sys
import os
import sqlite3
import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer

import networkx as nx
import matplotlib
matplotlib.use("Agg")  # Backend sin ventana interactiva para Streamlit
import matplotlib.pyplot as plt

# ──────────────────────────────────────────────────────────────────────────────
# Configuración Visual y de Página de Alto Nivel
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Control Center — Anomaly Detection",
    page_icon="🏬",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
        .block-container { padding-top: 1.5rem; padding-bottom: 0rem; }
        h1 { color: #1E293B; font-weight: 800; letter-spacing: -0.05em; }
        .stMetric { background-color: #F8FAFC; padding: 1rem; border-radius: 0.5rem; border: 1px solid #E2E8F0; }
        div[data-testid="stMetricValue"] { color: #0F172A; font-size: 1.8rem; font-weight: 700; }
    </style>
""", unsafe_allow_html=True)

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Consumidor Kafka (Singleton)
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_kafka_consumer(bootstrap_servers: str, topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        consumer_timeout_ms=100  
    )

# ──────────────────────────────────────────────────────────────────────────────
# Lógica del Grafo de Navegación Topológico
# ──────────────────────────────────────────────────────────────────────────────
def normalizar_pagina(url: str) -> str:
    if url == "/":                       return "Inicio"
    if url.startswith("/categorias"):    return "Categorías"
    if url.startswith("/ofertas"):       return "Ofertas"
    if url.startswith("/buscar"):        return "Buscar"
    if url.startswith("/producto"):      return "Producto"
    if url.startswith("/carrito"):       return "Carrito"
    if url.startswith("/checkout"):      return "Checkout"
    if url.startswith("/confirmacion"):  return "Confirmación"
    return "Otro"

ARISTAS_ESPERADAS = {
    ("Inicio", "Categorías"), ("Inicio", "Ofertas"), ("Inicio", "Buscar"),
    ("Categorías", "Producto"), ("Categorías", "Buscar"),
    ("Ofertas", "Producto"), ("Buscar", "Producto"),
    ("Producto", "Carrito"), ("Producto", "Categorías"), ("Producto", "Producto"),
    ("Carrito", "Checkout"), ("Carrito", "Categorías"), ("Carrito", "Inicio"),
    ("Checkout", "Confirmación"), ("Confirmación", "Confirmación")
}

POSICIONES_NODOS = {
    "Inicio":       (0.0,  0.5),
    "Categorías":   (1.5,  1.2),
    "Buscar":       (1.5,  0.4),
    "Ofertas":      (1.5, -0.4),
    "Producto":     (3.0,  0.5),
    "Carrito":      (4.5,  0.5),
    "Checkout":     (6.0,  0.5),
    "Confirmación": (7.5,  0.5),
    "Otro":         (0.0, -0.8),
}

MAX_SESIONES_RASTREADAS = 2000

def actualizar_grafo_observado(consumer_eventos):
    lote = consumer_eventos.poll(timeout_ms=100)
    for tp, mensajes in lote.items():
        for msg in mensajes:
            evento = msg.value
            sesion = evento.get("session_id")
            pagina = normalizar_pagina(evento.get("page_url", ""))
            if not sesion:
                continue

            pagina_anterior = st.session_state.ultima_pagina_sesion.get(sesion)
            if pagina_anterior is not None:
                arista = (pagina_anterior, pagina)
                st.session_state.aristas_observadas[arista] = \
                    st.session_state.aristas_observadas.get(arista, 0) + 1

            st.session_state.ultima_pagina_sesion[sesion] = pagina

    exceso = len(st.session_state.ultima_pagina_sesion) - MAX_SESIONES_RASTREADAS
    if exceso > 0:
        for clave in list(st.session_state.ultima_pagina_sesion.keys())[:exceso]:
            del st.session_state.ultima_pagina_sesion[clave]

def renderizar_grafo_observado():
    aristas = st.session_state.aristas_observadas
    if not aristas:
        st.info("⏳ Esperando eventos en vivo para estructurar las aristas del Grafo...")
        return

    G = nx.DiGraph()
    for (origen, destino), peso in aristas.items():
        G.add_edge(origen, destino, weight=peso)

    pos = {n: POSICIONES_NODOS.get(n, (0.0, -0.8)) for n in G.nodes()}
    peso_max = max(peso for peso in aristas.values()) if aristas else 1

    fig, ax = plt.subplots(figsize=(12, 4.2))
    fig.patch.set_alpha(0.0)
    ax.set_facecolor('none')

    nx.draw_networkx_nodes(G, pos, ax=ax, node_size=2000, node_color="#1E293B", edgecolors="#0F172A")
    nx.draw_networkx_labels(G, pos, ax=ax, font_size=8, font_color="white", font_weight="bold")

    aristas_normales = [(u, v) for u, v in G.edges() if (u, v) in ARISTAS_ESPERADAS]
    aristas_anomalas = [(u, v) for u, v in G.edges() if (u, v) not in ARISTAS_ESPERADAS]

    def _anchos(lista):
        return [1 + 5 * G[u][v]["weight"] / peso_max for u, v in lista]

    if aristas_normales:
        nx.draw_networkx_edges(G, pos, ax=ax, edgelist=aristas_normales,
                               width=_anchos(aristas_normales), edge_color="#10B981",
                               arrowsize=14, connectionstyle="arc3,rad=0.12")
    if aristas_anomalas:
        nx.draw_networkx_edges(G, pos, ax=ax, edgelist=aristas_anomalas,
                               width=_anchos(aristas_anomalas), edge_color="#EF4444",
                               arrowsize=16, style="dashed", connectionstyle="arc3,rad=0.22")

    ax.set_axis_off()
    fig.tight_layout()
    st.pyplot(fig, use_container_width=True)
    plt.close(fig)

# ──────────────────────────────────────────────────────────────────────────────
# Inicialización y Recuperación de Base de Datos
# ──────────────────────────────────────────────────────────────────────────────
def initialize_session_state(max_buffer: int):
    if "alerts_history" not in st.session_state:
        st.session_state.alerts_history = []
        # CORRECCIÓN: Recuperar datos históricos de SQLite para evitar pantallas en blanco al iniciar
        db_path = "data/historical_alerts.db"
        if os.path.exists(db_path):
            try:
                conn = sqlite3.connect(db_path)
                query = f"SELECT raw_json FROM alerts ORDER BY id DESC LIMIT {max_buffer}"
                df_db = pd.read_sql_query(query, conn)
                conn.close()
                
                for _, row in df_db.iterrows():
                    alert = json.loads(row["raw_json"])
                    metrics = alert.get("metrics", {})
                    src = alert.get("detection_source", {})
                    st.session_state.alerts_history.append({
                        "Timestamp": alert.get("timestamp", datetime.now().isoformat()),
                        "Sesión ID": alert.get("session_id", "Unknown"),
                        "Usuario ID": alert.get("user_id", "Anonymous"),
                        "IP": alert.get("ip_address", "0.0.0.0"),
                        "Dispositivo": alert.get("device", "unknown"),
                        "Clicks": metrics.get("clicks_count", 0),
                        "Latencia Media (ms)": metrics.get("avg_latency_ms", 0.0),
                        "Page Views": metrics.get("page_views_count", 0),
                        "Cart Adds": metrics.get("cart_adds_count", 0),
                        "Purchases": metrics.get("purchases_count", 0),
                        "Capa Reglas": "🚨 Activada" if src.get("rule_layer_triggered") else "✅ Normal",
                        "Capa ML": "🚨 Anomalía" if src.get("ml_layer_triggered") else "✅ Normal",
                        "Motivo Regla": src.get("rule_reason") if src.get("rule_layer_triggered") else "N/A"
                    })
            except Exception as e:
                logger.error(f"Error cargando base de datos: {e}")

    if "total_processed" not in st.session_state:
        st.session_state.total_processed = len(st.session_state.alerts_history)
    if "ultima_pagina_sesion" not in st.session_state:
        st.session_state.ultima_pagina_sesion = {}
    if "aristas_observadas" not in st.session_state:
        st.session_state.aristas_observadas = {}

# ──────────────────────────────────────────────────────────────────────────────
# Orquestador UI Principal
# ──────────────────────────────────────────────────────────────────────────────
def main():
    st.sidebar.image("https://img.icons8.com/fluency/96/dashboard.png", width=60)
    st.sidebar.title("Infraestructura TFM")
    st.sidebar.markdown("---")
    
    broker = st.sidebar.text_input("Broker Kafka", value="localhost:9094")
    topic = st.sidebar.text_input("Tópico Alertas", value="ecommerce-alerts")
    max_buffer = st.sidebar.slider("Tope de Memoria (Eventos)", min_value=100, max_value=2000, value=500)
    
    initialize_session_state(max_buffer)
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Estado del Sistema:**")
    st.sidebar.success("📡 Conectado al Clúster Reactivo")
    
    if st.sidebar.button("🗑️ Limpiar Historial"):
        st.session_state.alerts_history.clear()
        st.session_state.ultima_pagina_sesion.clear()
        st.session_state.aristas_observadas.clear()
        st.rerun()

    st.title("🏬 Operaciones de E-Commerce & Observabilidad de Comportamiento")
    st.caption("Capa 3 — Interfaz Gráfica Avanzada (Métricas del Negocio + Grafo Dinámico de Navegación)")
    st.markdown("---")

    # Consumo Seguro de Alertas de Kafka
    try:
        consumer = get_kafka_consumer(broker, topic, "tfm-dashboard-viewer-group")
        new_messages = consumer.poll(timeout_ms=100)
        for tp, messages in new_messages.items():
            for msg in messages:
                alert = msg.value
                st.session_state.total_processed += 1
                metrics = alert.get("metrics", {})
                src = alert.get("detection_source", {})
                
                st.session_state.alerts_history.insert(0, {
                    "Timestamp": alert.get("timestamp", datetime.now().isoformat()),
                    "Sesión ID": alert.get("session_id", "Unknown"),
                    "Usuario ID": alert.get("user_id", "Anonymous"),
                    "IP": alert.get("ip_address", "0.0.0.0"),
                    "Dispositivo": alert.get("device", "unknown"),
                    "Clicks": metrics.get("clicks_count", 0),
                    "Latencia Media (ms)": metrics.get("avg_latency_ms", 0.0),
                    "Page Views": metrics.get("page_views_count", 0),
                    "Cart Adds": metrics.get("cart_adds_count", 0),
                    "Purchases": metrics.get("purchases_count", 0),
                    "Capa Reglas": "🚨 Activada" if src.get("rule_layer_triggered") else "✅ Normal",
                    "Capa ML": "🚨 Anomalía" if src.get("ml_layer_triggered") else "✅ Normal",
                    "Motivo Regla": src.get("rule_reason") if src.get("rule_layer_triggered") else "N/A"
                })
    except Exception as e:
        st.sidebar.error(f"Kafka Alertas Offline: {e}")

    # Consumo Seguro de Eventos Crudos para el Grafo
    try:
        consumer_eventos = get_kafka_consumer(broker, "ecommerce-events", "tfm-dashboard-graph-group")
        actualizar_grafo_observado(consumer_eventos)
    except Exception as e:
        st.sidebar.warning(f"Kafka Eventos Offline: {e}")

    # Forzar límites del slider
    if len(st.session_state.alerts_history) > max_buffer:
        st.session_state.alerts_history = st.session_state.alerts_history[:max_buffer]

    if not st.session_state.alerts_history:
        st.info("⏳ Esperando telemetría inicial... Enciende el pipeline de simulación.")
        time.sleep(2)
        st.rerun()
        return

    df = pd.DataFrame(st.session_state.alerts_history)
    df_charts = df.head(max_buffer)

    # KPIS Card superior
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Alertas en Ventana", f"🚨 {len(df_charts)}")
    kpi2.metric("Violación de Reglas", f"🛠️ {len(df_charts[df_charts['Capa Reglas'] == '🚨 Activada'])}")
    kpi3.metric("Detectado por IA", f"🧠 {len(df_charts[df_charts['Capa ML'] == '🚨 Anomalía'])}")
    kpi4.metric("Bots Identificados", f"🤖 {len(df_charts[df_charts['Dispositivo'] == 'bot_scraper'])}")
    st.markdown("---")

    # FILA 1: TUS DOS GRAFICOS ORIGINALES RESTAURADOS
    col_chart1, col_chart2 = st.columns(2)
    with col_chart1:
        st.subheader("📈 Intensidad de Clicks vs Latencia Media")
        fig_scatter = px.scatter(
            df_charts, x="Clicks", y="Latencia Media (ms)", color="Dispositivo",
            size="Clicks", hover_data=["Usuario ID", "Capa Reglas", "Capa ML"],
            color_discrete_sequence=px.colors.qualitative.Safe
        )
        fig_scatter.update_layout(margin=dict(l=0, r=0, t=30, b=0), height=320)
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col_chart2:
        st.subheader("📊 Distribución de Amenazas (Últimos Eventos)")
        device_counts = df_charts["Dispositivo"].value_counts().reset_index()
        device_counts.columns = ["Dispositivo", "Alertas"]
        fig_bar = px.bar(
            device_counts, x="Dispositivo", y="Alertas", color="Dispositivo",
            text_auto=True, color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_bar.update_layout(margin=dict(l=0, r=0, t=30, b=0), height=320, showlegend=False)
        st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("---")

    # FILA 2: EL NUEVO GRAFO ABAJO SIN ROMPER NADA
    st.subheader("🕸️ Grafo de Navegación Dirigido y Observado en Tiempo Real")
    renderizar_grafo_observado()
    st.markdown("---")

    # FILA 3: TABLA FORENSE COMPLETA
    st.subheader("📋 Consola de Inferencia Reactiva y Auditoría Forense")
    columns_to_show = ["Timestamp", "Sesión ID", "Usuario ID", "IP", "Dispositivo", "Clicks", "Latencia Media (ms)", "Capa Reglas", "Capa ML", "Motivo Regla"]
    st.dataframe(
        df[columns_to_show], use_container_width=True, hide_index=True,
        column_config={
            "Timestamp": st.column_config.TextColumn("Fecha/Hora (UTC)"),
            "Clicks": st.column_config.NumberColumn("Clicks total"),
            "Latencia Media (ms)": st.column_config.NumberColumn("Latencia μ")
        }
    )

    time.sleep(2)
    st.rerun()

if __name__ == "__main__":
    main()  # <-- ESTA LÍNEA ES LA QUE TU AMIGO HABÍA BORRADO POR ERROR Y EVITABA EL ARRANQUE