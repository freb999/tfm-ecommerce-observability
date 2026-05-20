#!/bin/bash
# Script de automatización local para el TFM — Arranque en cascada del pipeline

echo "🚀 Levantando Infraestructura de Streaming..."
source venv/bin/activate

# 1. Iniciar el Generador de Eventos en segundo plano
echo "🔹 Arrancando Ingesta (Faker Producer)..."
python 1_ingestion/faker_producer.py --eps 30 > logs_producer.txt 2>&1 &
PID_PRODUCER=$!

# 2. Dar una breve pausa para estabilizar la red de Kafka e iniciar Flink
sleep 3
echo "🔹 Arrancando Motor Core (Apache Flink Processor)..."
python 2_processing/flink_processor.py > logs_flink.txt 2>&1 &
PID_FLINK=$!

# 3. Iniciar el Detector de IA
sleep 5
echo "🔹 Arrancando Capa de Inteligencia (Anomaly Detector)..."
python 2_processing/anomaly_detector.py > logs_detector.txt 2>&1 &
PID_DETECTOR=$!

# 4. Lanzar la interfaz gráfica en primer plano
sleep 2
echo "📊 Desplegando Centro de Control (Streamlit Dashboard)..."
streamlit run 3_dashboard/app.py

# Capturar la interrupción (Ctrl+C) para apagar limpiamente todos los subprocesos de Python
trap ctrl_c INT
function ctrl_c() {
        echo "🛑 Apagando componentes analíticos..."
        kill $PID_PRODUCER $PID_FLINK $PID_DETECTOR
        exit 0
}