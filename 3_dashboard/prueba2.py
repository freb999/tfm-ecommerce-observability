"""
================================================================================
 TFM — Arquitectura de Stream Processing para Detección de Anomalías en E-commerce
 Módulo : app.py
 Capa   : 3 — Interfaz Gráfica y Monitoreo Analítico en Tiempo Real
 Autores: Benjamín Romero Fonseca / Luis Canales Quiñones
 Versión: 2.0.0 (Persistencia DB + Sankey + PyVis + Matplotlib)
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
import streamlit.components.v1 as components
from kafka import KafkaConsumer

import networkx as nx
import matplotlib
matplotlib.use("Agg")  # Backend sin ventana interactiva para Streamlit
import matplotlib.pyplot as plt

try:
    from pyvis.network import Network
except ImportError:
    st.error("Falta instalar PyVis. Ejecuta: pip install pyvis")

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
# Lógica Base del Grafo de Navegación Topológico
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

# ──────────────────────────────────────────────────────────────────────────────
# Motores de Renderizado Visual
# ──────────────────────────────────────────────────────────────────────────────

def renderizar_matplotlib(aristas):
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

def renderizar_pyvis(aristas, umbral_ruido=1, solo_anomalias=False):
    if 'Network' not in globals():
        st.error("🚨 PyVis no está instalado. Ejecuta en la terminal: pip install pyvis")
        return

    net = Network(height="400px", width="100%", bgcolor="#ffffff", font_color="#1E293B", directed=True)
    net.toggle_physics(False)

    factor_escala = 150
    for nodo, (x_coord, y_coord) in POSICIONES_NODOS.items():
        pos_x = int((x_coord - 3.75) * factor_escala)
        pos_y = int(-y_coord * factor_escala)
        
        # Nodos "Trampa" vs Nodos normales
        if nodo == "Otro":
            color_nodo, forma, titulo_hover, tamano = "#64748B", "diamond", "Tráfico Sospechoso / Honeypot", 15
        else:
            color_nodo, forma, titulo_hover, tamano = "#1E293B", "dot", nodo, 20

        net.add_node(nodo, label=nodo, title=titulo_hover, color=color_nodo, shape=forma, size=tamano, x=pos_x, y=pos_y, physics=False)

    peso_max = max(aristas.values()) if aristas else 1
    
    # --- LA MAGIA DEL FILTRO ESTÁ AQUÍ ---
    for (origen, destino), peso in aristas.items():
        # 1. Filtro de ruido: Si tiene menos clics que el umbral, lo ignoramos
        if peso < umbral_ruido:
            continue
            
        es_normal = (origen, destino) in ARISTAS_ESPERADAS
        
        # 2. Filtro del botón del pánico: Si el usuario solo quiere ver anomalías, ocultamos las normales
        if solo_anomalias and es_normal:
            continue

        grosor = 1 + 9 * (peso / peso_max)
        color_linea = "#10B981" if es_normal else "#EF4444"
        
        net.add_edge(
            origen, destino, value=peso, width=grosor, color=color_linea, title=f"{peso} transiciones",
            smooth={"type": "curvedCW", "roundness": 0.2}
        )

    try:
        net.save_graph("grafo_vivo.html")
        with open("grafo_vivo.html", 'r', encoding='utf-8') as f:
            components.html(f.read(), height=415)
    except Exception as e:
        st.error(f"Error cargando PyVis: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Lógica de Actualización y Persistencia del Grafo (SQLite)
# ──────────────────────────────────────────────────────────────────────────────
def actualizar_grafo_observado(consumer_eventos):
    lote = consumer_eventos.poll(timeout_ms=100)
    if not lote:
        return

    # Conectar a la BD para guardar las aristas nuevas
    os.makedirs("data", exist_ok=True)
    db_path = "data/historical_alerts.db"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

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
                    
                    # Sumar a memoria RAM
                    nuevo_peso = st.session_state.aristas_observadas.get(arista, 0) + 1
                    st.session_state.aristas_observadas[arista] = nuevo_peso
                    
                    # PERSISTENCIA EN SQLITE (Guarda el estado del grafo en disco)
                    cursor.execute("""
                        INSERT INTO historial_navegacion (origen, destino, peso) 
                        VALUES (?, ?, ?)
                        ON CONFLICT(origen, destino) DO UPDATE SET peso = excluded.peso
                    """, (pagina_anterior, pagina, nuevo_peso))

                st.session_state.ultima_pagina_sesion[sesion] = pagina

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error escribiendo aristas en SQLite: {e}")

    exceso = len(st.session_state.ultima_pagina_sesion) - MAX_SESIONES_RASTREADAS
    if exceso > 0:
        for clave in list(st.session_state.ultima_pagina_sesion.keys())[:exceso]:
            del st.session_state.ultima_pagina_sesion[clave]


# ──────────────────────────────────────────────────────────────────────────────
# Inicialización y Recuperación de Base de Datos (Alertas + Grafo)
# ──────────────────────────────────────────────────────────────────────────────
def initialize_session_state(max_buffer: int):
    if "alerts_history" not in st.session_state: st.session_state.alerts_history = []
    if "ultima_pagina_sesion" not in st.session_state: st.session_state.ultima_pagina_sesion = {}
    if "aristas_observadas" not in st.session_state: st.session_state.aristas_observadas = {}

    os.makedirs("data", exist_ok=True)
    db_path = "data/historical_alerts.db"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Crear tabla del grafo si no existe
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS historial_navegacion (
                origen TEXT,
                destino TEXT,
                peso INTEGER,
                PRIMARY KEY (origen, destino)
            )
        ''')
        
        # Crear tabla de alertas por si es la primera vez
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                raw_json TEXT
            )
        ''')
        conn.commit()

        # 2. Cargar historial del Grafo (Persistencia)
        if not st.session_state.aristas_observadas:
            cursor.execute("SELECT origen, destino, peso FROM historial_navegacion")
            for origen, destino, peso in cursor.fetchall():
                st.session_state.aristas_observadas[(origen, destino)] = peso

        # 3. Cargar historial de Alertas
        if not st.session_state.alerts_history:
            cursor.execute(f"SELECT raw_json FROM alerts ORDER BY id DESC LIMIT {max_buffer}")
            for (raw_json,) in cursor.fetchall():
                alert = json.loads(raw_json)
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
                    "Capa Reglas": "🚨 Activada" if src.get("rule_layer_triggered") else "✅ Normal",
                    "Capa ML": "🚨 Anomalía" if src.get("ml_layer_triggered") else "✅ Normal",
                    "Motivo Regla": src.get("rule_reason") if src.get("rule_layer_triggered") else "N/A"
                })
        conn.close()
    except Exception as e:
        logger.error(f"Error inicializando base de datos: {e}")

    if "total_processed" not in st.session_state:
        st.session_state.total_processed = len(st.session_state.alerts_history)


# ──────────────────────────────────────────────────────────────────────────────
# Orquestador UI Principal
# ──────────────────────────────────────────────────────────────────────────────
def main():
    st.sidebar.image("https://img.icons8.com/fluency/96/dashboard.png", width=60)
    st.sidebar.title("Infraestructura TFM")
    st.sidebar.markdown("---")
    
    broker = st.sidebar.text_input("Broker Kafka", value="localhost:9094")
    topic = st.sidebar.text_input("Tópico Alertas", value="ecommerce-alerts")
    max_buffer = st.sidebar.slider("Tope de Memoria (Eventos)", min_value=100, max_value=10000, value=2000)
    
    st.sidebar.markdown("---")
    tipo_grafo = st.sidebar.selectbox(
        "👁️ Seleccionar Visualización de Grafo",
        ["Matplotlib (Mapa Estático)", "PyVis (Interactivo & Animado)"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("**🎛️ Filtros de Claridad Visual**")
    umbral_ruido = st.sidebar.slider("Ocultar flechas con menos de X clics", min_value=1, max_value=10000, value=1, help="Limpia la telaraña ocultando clics aislados.")
    solo_anomalias = st.sidebar.checkbox("🚨 Mostrar SOLO Anomalías (Rojo)", value=False)

    initialize_session_state(max_buffer)
    
    st.sidebar.markdown("---")
    st.sidebar.success("📡 Conectado al Clúster Reactivo")
    
    if st.sidebar.button("🗑️ Limpiar Memoria Gráfica"):
        st.session_state.alerts_history.clear()
        st.session_state.ultima_pagina_sesion.clear()
        st.session_state.aristas_observadas.clear()
        st.rerun()

    st.title("🏬 Operaciones de E-Commerce & Observabilidad de Comportamiento")
    st.caption("Capa 3 — Interfaz Gráfica Avanzada (Métricas del Negocio + Grafos Dinámicos Multipropósito)")
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

    if not st.session_state.alerts_history and not st.session_state.aristas_observadas:
        st.info("⏳ Esperando telemetría inicial... Enciende el pipeline de simulación.")
        time.sleep(2)
        st.rerun()
        return

    df = pd.DataFrame(st.session_state.alerts_history) if st.session_state.alerts_history else pd.DataFrame()
    df_charts = df.head(max_buffer) if not df.empty else df

    # KPIS Card superior
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Alertas en Ventana", f"🚨 {len(df_charts)}")
    if not df_charts.empty:
        kpi2.metric("Violación de Reglas", f"🛠️ {len(df_charts[df_charts['Capa Reglas'] == '🚨 Activada'])}")
        kpi3.metric("Detectado por IA", f"🧠 {len(df_charts[df_charts['Capa ML'] == '🚨 Anomalía'])}")
        kpi4.metric("Bots Identificados", f"🤖 {len(df_charts[df_charts['Dispositivo'] == 'bot_scraper'])}")
    else:
        kpi2.metric("Violación de Reglas", "🛠️ 0")
        kpi3.metric("Detectado por IA", "🧠 0")
        kpi4.metric("Bots Identificados", "🤖 0")
    st.markdown("---")

    # FILA 1: TUS DOS GRAFICOS ORIGINALES 
    col_chart1, col_chart2 = st.columns(2)
    with col_chart1:
        st.subheader("📈 Intensidad de Clicks vs Latencia Media")
        if not df_charts.empty:
            fig_scatter = px.scatter(
                df_charts, x="Clicks", y="Latencia Media (ms)", color="Dispositivo",
                size="Clicks", hover_data=["Usuario ID", "Capa Reglas", "Capa ML"],
                color_discrete_sequence=px.colors.qualitative.Safe
            )
            fig_scatter.update_layout(margin=dict(l=0, r=0, t=30, b=0), height=320)
            st.plotly_chart(fig_scatter, width="stretch")

    with col_chart2:
        st.subheader("📊 Distribución de Amenazas (Últimos Eventos)")
        if not df_charts.empty:
            device_counts = df_charts["Dispositivo"].value_counts().reset_index()
            device_counts.columns = ["Dispositivo", "Alertas"]
            fig_bar = px.bar(
                device_counts, x="Dispositivo", y="Alertas", color="Dispositivo",
                text_auto=True, color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_bar.update_layout(margin=dict(l=0, r=0, t=30, b=0), height=320, showlegend=False)
            st.plotly_chart(fig_bar, width="stretch")
    st.markdown("---")

    # FILA 2: MOTOR DE GRAFOS DINÁMICO SELECTIVO
    st.subheader("Grafo de Navegación Observado (Tiempo Real)")
    aristas = st.session_state.aristas_observadas
    
    if not aristas:
        st.info("⏳ Acumulando tráfico para generar visualización topológica...")
    else:
        if tipo_grafo == "Matplotlib (Mapa Estático)":
            renderizar_matplotlib(aristas)
        elif tipo_grafo == "PyVis (Interactivo & Animado)":
            renderizar_pyvis(aristas, umbral_ruido, solo_anomalias)
            
    st.markdown("---")

    # FILA 3: TABLA FORENSE 
    st.subheader("📋 Consola de Inferencia Reactiva y Auditoría Forense")
    if not df_charts.empty:
        columns_to_show = ["Timestamp", "Sesión ID", "Usuario ID", "IP", "Dispositivo", "Clicks", "Latencia Media (ms)", "Capa Reglas", "Capa ML", "Motivo Regla"]
        st.dataframe(
            df[columns_to_show], hide_index=True, width="stretch",
            column_config={
                "Timestamp": st.column_config.TextColumn("Fecha/Hora (UTC)"),
                "Clicks": st.column_config.NumberColumn("Clicks total"),
                "Latencia Media (ms)": st.column_config.NumberColumn("Latencia μ")
            }
        )

    time.sleep(2)
    st.rerun()

if __name__ == "__main__":
    main()