# tfm-ecommerce-observability
Código fuente del TFM: Arquitectura de Stream Processing para E-commerce

# Architecture of Stream Processing for Observability of User Behavior and Proactive Detection of Anomalies in E-commerce

Este repositorio contiene la implementación core del Trabajo de Fin de Máster (TFM). El ecosistema está diseñado bajo el patrón de **Arquitectura de Microservicios Orientada a Eventos (EDA)**, acoplando el procesamiento continuo de datos masivos en tiempo real con inteligencia artificial no supervisada para mitigar amenazas como bots scrapers, ataques de denegación de servicio (DoS) o degradaciones críticas de rendimiento en la plataforma.

---

## 🏗️ Arquitectura General del Sistema

El pipeline analítico está desacoplado y es completamente reactivo, dividiéndose en 4 capas operacionales:

1. **Capa de Ingesta (Ingestion Layer):** Un generador sintético multihilo (`Faker`) que emula el clickstream real de usuarios y bots concurrentes, inyectando eventos JSON directamente en Apache Kafka.
2. **Capa de Procesamiento (Core Stream Processing):** Un motor basado en **Apache Flink 1.18.1** que intercepta el flujo crudo, limpia el esquema y agrupa las interacciones en **Ventanas de Volteo (Tumbling Windows) de 10 segundos** por sesión para calcular vectores de características multidimensionales.
3. **Capa de Inteligencia (Anomaly Detection Layer):** Un microservicio híbrido en Python que consume de Kafka, evaluando reglas heurísticas de negocio en paralelo con un modelo predictivo de Machine Learning (**Isolation Forest**) con calibración dinámica inicial.
4. **Capa de Visualización (Observability Layer):** Un centro de control interactivo desarrollado en **Streamlit** y **Plotly** que consume el flujo de alertas finales de Kafka para su auditoría visual en tiempo real.

---

## 📂 Estructura del Repositorio

```text
├── 1_ingestion/
│   └── faker_producer.py       # Origen de datos (Productor Kafka)
├── 2_processing/
│   ├── flink_processor.py      # Core de procesamiento y ventanas temporales
│   ├── anomaly_detector.py     # Microservicio de IA (Isolation Forest + Reglas)
│   └── flink-sql-connector-kafka-3.0.2-1.18.jar # Conector oficial JVM
├── 3_dashboard/
│   └── app.py                  # Interfaz de observabilidad en Streamlit
├── requirements.txt            # Lista estricta de dependencias (Python 3.10)
├── start_pipeline.sh           # Script de automatización de arranque
└── README.md                   # Documentación técnica del proyecto
```

---

## 🛠️ Requisitos Previos e Instalación

Debido a restricciones estrictas de compatibilidad con las librerías de enlace y entornos de serialización de Apache Flink (`pemja` y `py4j`), el sistema debe ejecutarse obligatoriamente bajo un entorno controlado de **Python 3.10.x**.

### 1. Preparar el Entorno Virtual
Asegúrese de contar con Python 3.10 instalado en su sistema operativo. Posteriormente, inicialice y active un entorno virtual aislado para evitar conflictos de dependencias globales:

```bash
# Crear el entorno virtual usando la versión de Python 3.10
python3.10 -m venv venv

# Activar el entorno virtual (Linux/macOS)
source venv/bin/activate

# Activar el entorno virtual (Windows - PowerShell)
.\venv\Scripts\Activate.ps1
```

### 2. Instalar Dependencias Oficiales
Con la burbuja del entorno virtual activa, proceda a instalar las dependencias base del proyecto congeladas en el archivo de requerimientos:

```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

---

## 🚀 Guía de Despliegue y Ejecución

El pipeline analítico se puede inicializar de dos maneras dependiendo de las necesidades de visualización:

### Opción A: Despliegue Automatizado (Consola Limpia)
El repositorio incluye un script de automatización en Bash (`start_pipeline.sh`) que levanta de forma secuencial y coordinada todos los microservicios de Python, enviando las salidas de datos a segundo plano para no saturar la terminal:

```bash
chmod +x start_pipeline.sh
./start_pipeline.sh
```

### Opción B: Despliegue Manual (Auditoría Individual de Componentes)
Para depurar o examinar los flujos de texto de cada componente de manera independiente, abra 4 terminales o pestañas separadas, asegúrese de activar el entorno virtual en cada una de ellas y ejecute los scripts en este orden estricto:

* **Terminal 1 — Generación de Tráfico Web (Ingesta):**
  ```bash
  python 1_ingestion/faker_producer.py --eps 30
  ```
* **Terminal 2 — Procesamiento de Ventanas Temporales (Flink Core):**
  ```bash
  python 2_processing/flink_processor.py
  ```
* **Terminal 3 — Cerebro Predictivo del Sistema (IA Detector):**
  ```bash
  python 2_processing/anomaly_detector.py
  ```
* **Terminal 4 — Despliegue de la Interfaz de Usuario (Dashboard):**
  ```bash
  streamlit run 3_dashboard/app.py
  ```

---

## 🚰 Manual de Auditoría Forense y Verificación de Flujos

Para defender y validar la resiliencia e integridad de la arquitectura frente a un proceso de evaluación técnica, se exponen las siguientes herramientas de inspección y rastreo de datos en tiempo real:

### 1. Intercepción de la Ingesta Cruda (Tópico: `ecommerce-events`)
Para auditar que las interacciones crudas de los usuarios y bots están impactando el broker de mensajería correctamente, se puede "espiar" el canal de entrada ejecutando el consumidor de consola nativo desde el contenedor de Kafka:

```bash
docker exec -it tfm-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce-events --from-beginning
```
*(Nota: Si su arquitectura local utiliza Podman en lugar de Docker, reemplace el comando inicial por `podman exec`).*

### 2. Intercepción del Vector de Características (Tópico: `retail-feature-vectors`)
Para demostrar la efectividad de la Ingeniería de Atributos realizada por Apache Flink, inspeccione el canal donde se depositan las métricas consolidadas cada 10 segundos por sesión (clicks, latencia media, carritos, etc.):

```bash
docker exec -it tfm-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic retail-feature-vectors --from-beginning
```

### 3. Monitoreo Activo de la Toma de Decisiones del Modelo Híbrido
Al hacer uso del script automatizado (`./start_pipeline.sh`), la inferencia del modelo no supervisado se almacena de forma asíncrona. Puede abrir una consola paralela para auditar las razones exactas detrás de cada alerta levantada:

```bash
tail -f logs_detector.txt
```

* **Fase de Calibración (Muestras 1 a 40):** El archivo registrará el progreso del buffer en memoria: `Calibrando Isolation Forest: [25/40] vectores recolectados.`
* **Fase de Inferencia Activa (Muestras > 40):** El algoritmo activa la predicción en un espacio de 6 dimensiones numéricas, desglosando las amenazas bajo dos etiquetas:
  * `[ALERTA - REGLA]`: Alertas deterministas disparadas instantáneamente al violar umbrales lógicos del negocio (ej. latencias medias > 2000ms o firmas directas de bots).
  * `[ALERTA - MACHINE LEARNING]`: Anomalías sutiles detectadas de manera estadística. Identifica patrones de navegación sospechosos de usuarios con frecuencias de click moderadas que un sistema tradicional basado en reglas fijas ignoraría por completo.