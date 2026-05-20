"""
================================================================================
 TFM — Arquitectura de Stream Processing para Detección de Anomalías en E-commerce
 Módulo : app.py
 Capa   : 3 — Interfaz Gráfica y Monitoreo Analítico en Tiempo Real
 Autores: Benjamín Romero Fonseca / Luis Canales Quiñones
 Versión: 1.0.1
 Python : 3.10.x
 Deps   : streamlit plotly pandas kafka-python-ng

 Uso:
     streamlit run 3_dashboard/app.py
================================================================================
"""

import json
import logging
import sys
import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer

# ──────────────────────────────────────────────────────────────────────────────
# Configuración Visual y de Página de Alto Nivel (Look & Feel Profesional)
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Control Center — Anomaly Detection",
    page_icon="🏬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inyección de estilos CSS personalizados para emular un centro de operaciones (Datadog Style)
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
# Inicialización de Recursos de Red Compartidos (Patrón Singleton en Streamlit)
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_kafka_consumer(bootstrap_servers: str, topic: str, group_id: str) -> KafkaConsumer:
    """Instancia un consumidor único de Kafka optimizado para no bloquear la UI."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        consumer_timeout_ms=100  # Pequeño timeout para liberar el hilo de Streamlit
    )

# ──────────────────────────────────────────────────────────────────────────────
# Orquestador de Datos y UI
# ──────────────────────────────────────────────────────────────────────────────
def initialize_session_state():
    """Mantiene el almacenamiento de datos persistente entre refrescos de pantalla."""
    if "alerts_history" not in st.session_state:
        st.session_state.alerts_history = []
    if "total_processed" not in st.session_state:
        st.session_state.total_processed = 0

def main():
    initialize_session_state()
    
    # ── Barra Lateral de Configuración (Controles de Infraestructura) ──────────
    st.sidebar.image("https://img.icons8.com/fluency/96/dashboard.png", width=60)
    st.sidebar.title("Infraestructura TFM")
    st.sidebar.markdown("---")
    
    broker = st.sidebar.text_input("Broker Kafka", value="localhost:9094")
    topic = st.sidebar.text_input("Tópico Alertas", value="ecommerce-alerts")
    
    max_buffer = st.sidebar.slider("Tope de Memoria (Eventos)", min_value=100, max_value=2000, value=500)
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Estado del Sistema:**")
    st.sidebar.success("📡 Conectado al Clúster Reactivo")
    
    if st.sidebar.button("🗑️ Limpiar Historial de Alertas"):
        st.session_state.alerts_history.clear()
        st.rerun()

    # ── Título Principal y Encabezado de la Tesis ──────────────────────────────
    st.title("🏬 Operaciones de E-Commerce & Observabilidad de Comportamiento")
    st.caption("Capa 3 — Interfaz Gráfica y Monitoreo Analítico (Apache Flink ⚙️ + Isolation Forest 🧠)")
    st.markdown("---")

    # Conectar de manera segura con el consumidor
    try:
        consumer = get_kafka_consumer(broker, topic, "tfm-dashboard-viewer-group")
    except Exception as e:
        st.error(f"Error crítico al conectar con Kafka: {e}")
        return

    # Leer ráfagas de datos desde Kafka antes de renderizar los gráficos
    new_messages = consumer.poll(timeout_ms=150)
    for tp, messages in new_messages.items():
        for msg in messages:
            alert = msg.value
            st.session_state.total_processed += 1
            
            # Formatear el payload plano para simplificar el análisis con Pandas
            metrics = alert.get("metrics", {})
            src = alert.get("detection_source", {})
            
            flat_alert = {
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
            }
            
            # Insertar al inicio de la lista para ver lo más nuevo primero
            st.session_state.alerts_history.insert(0, flat_alert)

    # Evitar desbordamiento de memoria recortando el historial según el slider de la barra lateral
    if len(st.session_state.alerts_history) > max_buffer:
        st.session_state.alerts_history = st.session_state.alerts_history[:max_buffer]

    # Convertir a DataFrame de Pandas si hay datos
    if not st.session_state.alerts_history:
        st.info("⏳ Esperando alertas entrantes desde el topic `ecommerce-alerts`... Pon a correr Flink y el Detector.")
        time.sleep(2)
        st.rerun()
        return

    df = pd.DataFrame(st.session_state.alerts_history)

    # ── KPIs del Negocio (Métricas Clave del Centro de Control) ─────────────────
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    
    total_alerts = len(df)
    rules_count = len(df[df["Capa Reglas"] == "🚨 Activada"])
    ml_count = len(df[df["Capa ML"] == "🚨 Anomalía"])
    bot_count = len(df[df["Dispositivo"] == "bot_scraper"])
    
    kpi1.metric("Alertas en Ventana", f"🚨 {total_alerts}", help="Total de sesiones marcadas en el buffer de memoria")
    kpi2.metric("Violación de Reglas", f"🛠️ {rules_count}", help="Sesiones capturadas por límites heurísticos deterministas")
    kpi3.metric("Detectado por IA (ML)", f"🧠 {ml_count}", help="Patrones estadísticos complejos aislados por Isolation Forest")
    kpi4.metric("Bots Bloqueados", f"🤖 {bot_count}", help="Sesiones cuya firma corresponde al bot corporativo")

    st.markdown("---")

    # ── Sección Gráfica Analítica (Dos Columnas Dinámicas) ──────────────────────
    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        st.subheader("📈 Intensidad de Clicks vs Latencia Media")
        # Gráfico de dispersión interactivo para mapear clústeres de comportamiento
        fig_scatter = px.scatter(
            df, 
            x="Clicks", 
            y="Latencia Media (ms)", 
            color="Dispositivo",
            size="Clicks",
            hover_data=["Usuario ID", "Capa Reglas", "Capa ML"],
            color_discrete_sequence=px.colors.qualitative.Safe # Corregido a paleta estándar accesible
        )
        fig_scatter.update_layout(margin=dict(l=0, r=0, t=30, b=0), height=350)
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col_chart2:
        st.subheader("📊 Distribución de Amenazas por Tipo de Dispositivo")
        device_counts = df["Dispositivo"].value_counts().reset_index()
        device_counts.columns = ["Dispositivo", "Alertas"]
        
        fig_bar = px.bar(
            device_counts, 
            x="Dispositivo", 
            y="Alertas", 
            color="Dispositivo",
            text_auto=True,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_bar.update_layout(margin=dict(l=0, r=0, t=30, b=0), height=350, showlegend=False)
        st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("---")

    # ── Tabla de Monitoreo Crítico en Vivo (Live Log Table) ─────────────────────
    st.subheader("📋 Consola de Inferencia Reactiva y Auditoría Forense")
    
    # Selección de columnas críticas para mantener la scannability de la tabla
    columns_to_show = [
        "Timestamp", "Sesión ID", "Usuario ID", "IP", "Dispositivo", 
        "Clicks", "Latencia Media (ms)", "Capa Reglas", "Capa ML", "Motivo Regla"
    ]
    
    # Formateo visual estético de la tabla de datos
    st.dataframe(
        df[columns_to_show],
        use_container_width=True,
        hide_index=True,
        column_config={
            "Timestamp": st.column_config.TextColumn("Fecha/Hora (UTC)"),
            "Clicks": st.column_config.NumberColumn("Clicks total"),
            "Latencia Media (ms)": st.column_config.NumberColumn("Latencia μ"),
            "Capa Reglas": st.column_config.TextColumn("Filtro Heurístico"),
            "Capa ML": st.column_config.TextColumn("Filtro Estadístico (IA)")
        }
    )

    # Hilo de autorefresco constante: Espera 2 segundos y vuelve a disparar la UI
    time.sleep(2)
    st.rerun()


if __name__ == "__main__":
    main()