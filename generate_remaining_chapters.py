import os

CAP_DIR = r'c:\Users\lgalv\Desktop\obsidian\libro\capitulos'
MIRROR_DIR = r'D:\estudio\libro\capitulos'

chapters = {
    "19-gobernanza-linaje-unity-catalog.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Unity Catalog]], [[Data Governance]] -->

# Capítulo 19: Gobernanza, Linaje y Unity Catalog en Lakehouse

> *"En arquitecturas analíticas a escala de petabytes, un lago de datos sin gobernanza se degrada inevitablemente en un pantano de datos (Data Swamp). La gobernanza no es una restricción burocrática; es el plano de control que otorga auditabilidad, linaje y seguridad homogénea a todo el ecosistema distribuido."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender los principios arquitectónicos del control de acceso unificado y la gobernanza en entornos Lakehouse.
* Implementar modelos de autorización basados en tres niveles de espacio de nombres (*catalog.schema.table*).
* Rastrear y auditar el linaje automático de datos (*Data Lineage*) a nivel de tabla y columna.
* Configurar políticas de enmascaramiento dinámico de datos y seguridad a nivel de fila y columna (*Row/Column Level Security*).

---

> **Definición 19.1 (Gobernanza Unificada y Unity Catalog)**
> Unity Catalog es el motor de gobernanza universal para entornos Lakehouse que centraliza la administración de permisos, auditoría y linaje de datos para archivos, tablas, volúmenes y modelos de machine learning sobre múltiples nubes. Opera desacoplando las credenciales de almacenamiento físico en la nube (S3, ADLS Gen2, GCS) del catálogo lógico accesible por los usuarios y motores de cómputo.

---

## 19.1 La Fragmentación Tradicional y la Necesidad de un Catálogo Unificado

Históricamente, las organizaciones enfrentaban un modelo de gobernanza fragmentado: las bases de datos relacionales administraban permisos mediante sentencias SQL `GRANT/REVOKE`, mientras que el almacenamiento masivo (Data Lake) dependía de políticas de IAM en la nube (*AWS IAM Roles*, *Azure RBAC*). Esta dualidad provocaba inconsistencias graves de seguridad, imposibilidad de auditar quién accedía a los datos y duplicación innecesaria de archivos para restringir acceso.

El paradigma moderno de Lakehouse unifica este control mediante un metastore centralizado y un modelo jerárquico de tres niveles:

```
[ Metastore Central ]
        │
   ┌────┴────┐
[ Catalog ] [ Catalog ]
   │
[ Schema / Database ]
   │
┌──┴───────────────────────────┐
[ Table ]  [ View ]  [ Volume ]
```

---

### 19.2 Profundización Técnica: Control de Acceso Basado en Roles (RBAC) y Enmascaramiento

La administración de privilegios en Unity Catalog se realiza mediante el estándar ANSI SQL sobre objetos securizables:

```sql
-- Creación de catálogo corporativo
CREATE CATALOG IF NOT EXISTS finanzas_corp;
USE CATALOG finanzas_corp;

CREATE SCHEMA IF NOT EXISTS auditoria;

-- Otorgar permisos a grupos de directorio activo (SCIM)
GRANT USAGE ON CATALOG finanzas_corp TO `analistas_riesgo`;
GRANT USAGE ON SCHEMA auditoria TO `analistas_riesgo`;
GRANT SELECT ON TABLE auditoria.transacciones_sospechosas TO `analistas_riesgo`;

-- Enmascaramiento dinámico de datos personales (PII)
CREATE OR REPLACE FUNCTION mask_tarjeta(numero STRING)
RETURN CASE WHEN is_account_group_member('oficiales_cumplimiento') THEN numero
            ELSE concat('XXXX-XXXX-XXXX-', right(numero, 4)) END;

ALTER TABLE auditoria.transacciones_sospechosas 
ALTER COLUMN pan_tarjeta SET MASK mask_tarjeta;
```

---

> **Consideraciones de Producción 19.1: Linaje de Datos Automatizado**
> Unity Catalog captura de manera nativa el linaje de ejecución analizando los planes de Catalyst generados por las sentencias SQL y código DataFrame. Esto permite rastrear automáticamente el impacto de cambios aguas abajo (*downstream impact analysis*) y verificar la procedencia de cualquier métrica regulatoria hasta su fuente cruda en la capa Bronze.

---

### 19.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Conceder Credenciales Directas de Almacenamiento a Usuarios o Aplicaciones**  
Asignar llaves de acceso o cadenas de conexión directas a buckets de S3 o contenedores ADLS elude completamente el motor de gobernanza. Toda lectura y escritura en producción debe canalizarse a través de credenciales de almacenamiento gestionadas por el catálogo.

**Anti-patrón 2: Catálogos Monolíticos Sin Segregación de Entornos**  
Crear un único catálogo para desarrollo, pruebas y producción dificulta la contención de incidentes y viola las normativas de segregación de funciones. El estándar exige catálogos independientes: `dev_catalog`, `qa_catalog` y `prod_catalog`.

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. ¿Cuál es la diferencia fundamental entre el Hive Metastore tradicional y un catálogo de gobernanza unificada como Unity Catalog?
2. Explica cómo la función `is_account_group_member()` permite implementar seguridad a nivel de fila (*Row-Level Security*) sin duplicar tablas físicas.
3. Diseña una consulta SQL para otorgar permisos de lectura únicamente sobre una vista agregada en el esquema `reportes_ejecutivos`.
""",

    "20-catalogacion-y-descubrimiento.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Data Catalog]], [[Data Discovery]] -->

# Capítulo 20: Catalogación y Descubrimiento de Activos de Datos

> *"El valor de los datos es proporcional a su accesibilidad comprensible. Un activo que no puede ser descubierto, comprendido o auditado por los ingenieros y consumidores del negocio es un activo computacionalmente inerte."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Diseñar estrategias de metadatos técnicos y de negocio para la catalogación empresarial.
* Diferenciar entre catálogos de metadatos pasivos y catálogos activos (*Active Metadata*).
* Integrar motores de búsqueda de datos y descubrimiento como OpenMetadata, DataHub o Alation.
* Automatizar la extracción de diccionarios de datos a partir de esquemas de tablas transaccionales.

---

> **Definición 20.1 (Metadatos Activos y Catálogo de Datos)**
> Un **Catálogo de Datos** es un inventario organizado de activos de información en una empresa que compila metadatos técnicos (esquemas, tipos de datos, particiones), operativos (frecuencia de actualización, tiempos de ejecución, volumen) y de negocio (glosario conceptual, propietarios, clasificación de confidencialidad). La catalogación **activa** no solo indexa metadatos estáticos, sino que alimenta alertas operacionales y ajusta dinámicamente políticas de optimización y acceso.

---

## 20.1 Arquitectura del Descubrimiento de Datos Moderno

En organizaciones con cientos de pipelines de datos y decenas de miles de tablas, encontrar la fuente autoritativa de una entidad (la "verdad dorada") es un desafío crítico. El descubrimiento de datos moderno se apoya en tres pilares:

```
┌────────────────────────────────────────────────────────┐
│                   CAPA DE CONSUMO                      │
│      Búsqueda Semántica · Glosario · Verificación      │
└──────────────────────────┬─────────────────────────────┘
                           │
┌──────────────────────────┴─────────────────────────────┐
│                 GRAFO DE METADATOS                     │
│    Relaciones Tabla-Columna · Linaje · Propietarios    │
└──────────────────────────┬─────────────────────────────┘
                           │
┌──────────────────────────┴─────────────────────────────┐
│               CONECTORES DE EXTRACCIÓN                 │
│      Spark · Delta Lake · dbt · Airflow · Kafka        │
└────────────────────────────────────────────────────────┘
```

---

### 20.2 Profundización Técnica: Automatización del Diccionario de Datos con dbt y Delta Lake

La mejor práctica de ingeniería consiste en gestionar los metadatos como código (*Metadata as Code*). Al documentar descripciones y pruebas a nivel de esquema en archivos YAML versionados, las herramientas de catalogación sincronizan automáticamente el catálogo corporativo:

```yaml
version: 2

models:
  - name: dim_clientes
    description: "Dimensión corporativa consolidada de clientes con estado de cuenta activo."
    columns:
      - name: cliente_id
        description: "Clave subrogada única del cliente."
        tests:
          - unique
          - not_null
      - name: email_contacto
        description: "Correo electrónico verificado. Clasificado como PII nivel 2."
        meta:
          confidencialidad: "restringido"
          propietario: "equipo_crm"
```

---

> **Consideraciones de Producción 20.1: Extracción Programática de Esquemas**
> En pipelines de Spark, la inspección programática de esquemas permite emitir eventos de cambio hacia el bus de metadatos en formato JSON Schema, facilitando la detección proactiva de roturas de contrato (*Data Contract Breaks*).

---

### 20.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Catálogos Manuales en Hojas de Cálculo**  
Mantener diccionarios de datos en documentos estáticos desvinculados del código garantiza su obsolescencia en semanas. La catalogación debe derivarse automáticamente del DDL y de los metadatos del pipeline.

**Anti-patrón 2: Catálogos sin Asignación de Propiedad (*Data Ownership*)**  
Un activo sin un equipo propietario claramente asignado (*Data Stewards*) carece de responsable ante incidentes de calidad o fallos de seguridad.

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. ¿Qué información compone la tríada de metadatos (técnicos, operativos y de negocio)?
2. Explica cómo la integración entre dbt y herramientas como OpenMetadata previene la desactualización del glosario de términos.
3. Define qué es un contrato de datos (*Data Contract*) y cómo interactúa con el catálogo de datos.
""",

    "21-fundamentos-streaming-kafka.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Kafka]], [[Streaming]] -->

# Capítulo 21: Fundamentos de Streaming y Apache Kafka

> *"El mundo operacional no emite datos en lotes nocturnos; los eventos ocurren de forma continua en tiempo real. Tratar datos en lotes es una aproximación forzada por limitaciones históricas de cómputo; el streaming representa el modelo natural de la realidad transaccional."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender la arquitectura desacoplada de publicación/suscripción basada en registros distribuidos (*commit log*).
* Dominar la semántica de particionamiento, replicación y balanceo en clústeres de Apache Kafka.
* Distinguir rigurosamente entre tiempo de evento (*Event Time*), tiempo de procesamiento (*Processing Time*) y tiempo de ingesta.
* Configurar productores y consumidores garantizando semántica de entrega confiable (*at-least-once* y *idempotent delivery*).

---

> **Definición 21.1 (Registro Distribuido Inmutable / Commit Log)**
> Apache Kafka es un sistema de almacenamiento distribuido diseñado como un registro de confirmación inmutable y ordenado (*append-only log*). Los eventos se escriben de manera secuencial al final del archivo y se indexan mediante un entero incremental denominado **offset**. Esta inmutabilidad garantiza lecturas concurrentes sin bloqueos y permite una retención temporal configurable.

---

## 21.1 Anatomía de Apache Kafka: Tópicos, Particiones y Consumidores

Kafka desacopla a los productores de los consumidores mediante una abstracción modular:

```
[ Productor ] ──> [ Tópico: telemetria_sensores ]
                       ├── Partición 0: [E0][E1][E2][E3]... ──> Consumidor A
                       ├── Partición 1: [E0][E1][E2][E3]... ──> Consumidor B
                       └── Partición 2: [E0][E1][E2][E3]... ──> Consumidor C
```

* **Tópico:** Categoría lógica a la que se envían los mensajes.
* **Partición:** Unidad física de paralelismo y escalabilidad horizontal. Dentro de una partición, el orden de los mensajes está estrictamente garantizado.
* **Grupos de Consumidores (*Consumer Groups*):** Conjunto de consumidores que cooperan para procesar en paralelo las particiones de un tópico, asignando cada partición a exactamente un consumidor del grupo.

---

### 21.2 Profundización Técnica: Productor Idempotente y Transaccional

Para evitar duplicación de eventos ante reintentos de red, Kafka implementa productores idempotentes asignando un identificador de productor (`PID`) y un número de secuencia a cada lote de mensajes:

```python
from kafka import KafkaProducer
import json

# Configuración de productor seguro de producción
producer = KafkaProducer(
    bootstrap_servers=['kafka-broker-1:9092', 'kafka-broker-2:9092'],
    acks='all',  # Esperar confirmación de todas las réplicas en sincronía (min.insync.replicas)
    retries=5,
    enable_idempotence=True,  # Garantiza que los reintentos no dupliquen mensajes
    max_in_flight_requests_per_connection=5,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

evento = {
    "sensor_id": "SN-8841",
    "temperatura": 78.4,
    "timestamp": "2026-10-11T20:15:00Z"
}

# Envío con clave para garantizar hashing a la misma partición
future = producer.send('telemetria_sensores', key=b'SN-8841', value=evento)
record_metadata = future.get(timeout=10)

print(f"Mensaje persistido en partición {record_metadata.partition} con offset {record_metadata.offset}")
producer.flush()
```

---

> **Consideraciones de Producción 21.1: Semántica Temporal en Sistemas Distribuidos**
> En streaming es vital diferenciar:
> 1. **Event Time:** El instante preciso en que el evento ocurrió en el dispositivo de origen.
> 2. **Ingestion Time:** El instante en que Kafka recibió el evento y le asignó offset.
> 3. **Processing Time:** El instante en que el motor de cómputo (ej. Spark) evalúa el evento.
> Todos los cálculos de negocio y agregaciones en ventanas (*windowing*) deben fundamentarse rigurosamente en **Event Time** para tolerar latencias y datos fuera de orden.

---

### 21.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Configurar `acks=0` o `acks=1` en Datos Financieros o Críticos**  
Utilizar confirmación relajada provoca pérdida irrecuperable de mensajes ante la caída inesperada del broker líder antes de replicar a los seguidores.

**Anti-patrón 2: Particionamiento Aleatorio Cuando se Requiere Orden Secuencial**  
Si se omite la clave del mensaje (`key=None`), Kafka distribuye mediante round-robin entre particiones, perdiendo la garantía de orden para eventos de una misma entidad (ej. actualizaciones de una misma cuenta bancaria).

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. ¿Cómo garantiza Apache Kafka el orden estricto de eventos y cuál es el alcance de dicha garantía?
2. Explica qué ocurre cuando el número de consumidores en un grupo excede el número de particiones del tópico.
3. Diseña una estrategia de particionado para una flota de 100,000 vehículos telemétricos asegurando procesamiento en orden y carga balanceada.
""",

    "22-spark-streaming-y-cdc.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Structured Streaming]], [[Data Quality en Streaming con Delta Lake]] -->

# Capítulo 22: Spark Structured Streaming y Change Data Capture (CDC)

> *"El motor unificado de Spark Structured Streaming redefine la computación en tiempo real bajo la premisa de tablas infinitas: el procesamiento en streaming es conceptualmente idéntico al procesamiento en batch sobre una tabla que recibe datos continuos."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender el modelo de datos de tabla sin fin (*Unbounded Table*) en Spark Structured Streaming.
* Implementar ingesta continua desde Apache Kafka hacia tablas transaccionales Delta Lake.
* Gestionar el progreso del flujo mediante puntos de control (*Checkpoints*) y escritura por adelantado (*WAL*).
* Procesar flujos de cambios operacionales (CDC) aplicando operaciones idempotentes de `MERGE INTO`.

---

> **Definición 22.1 (Modelo Unbounded Table y Structured Streaming)**
> Spark Structured Streaming es un motor de procesamiento de flujos escalable y tolerante a fallos construido sobre el motor Catalyst de Spark SQL. Modela el flujo entrante como una **tabla sin límites** a la que se añaden continuamente nuevas filas. El usuario expresa la consulta analítica utilizando la misma API declarativa de DataFrames empleada en procesos por lotes.

---

## 22.1 Arquitectura de Ingesta Continua y Checkpointing

Para ofrecer garantías de procesamiento de exactamente una vez (*Exactly-Once Processing*), Structured Streaming se fundamenta en dos componentes críticos:

```
[ Kafka Source ] ──> [ Micro-Batch / Continuous Engine ] ──> [ Delta Lake Sink ]
                                │
                      [ Checkpoint Directory ]
                      ├── commits / (metadatos de lotes completados)
                      ├── offsets / (registro del último offset procesado)
                      └── sources / (estado de las fuentes de datos)
```

Si el clúster o el Driver fallan a mitad de un micro-lote, al reiniciar la aplicación, Spark lee el directorio de checkpoint, restaura el offset exacto y reanuda el cómputo sin pérdida ni duplicación de datos.

---

### 22.2 Profundización Técnica: Ingesta de Kafka a Delta Lake con CDC

El patrón estándar de Change Data Capture (CDC) captura eventos `INSERT`, `UPDATE` y `DELETE` generados por herramientas como Debezium y los fusiona en la capa Silver de Lakehouse:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from delta.tables import DeltaTable

spark = SparkSession.builder \\
    .appName("Capitulo22-StreamingCDC") \\
    .getOrCreate()

# Esquema del mensaje CDC
schema_cdc = StructType([
    StructField("operacion", StringType(), False),  # 'I', 'U', 'D'
    StructField("cliente_id", StringType(), False),
    StructField("saldo_actual", DoubleType(), True),
    StructField("timestamp_evento", TimestampType(), False)
])

# 1. Lectura en streaming desde Kafka
df_stream = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \\
    .option("subscribe", "cdc_clientes") \\
    .option("startingOffsets", "latest") \\
    .load() \\
    .select(from_json(col("value").cast("string"), schema_cdc).alias("data")) \\
    .select("data.*")

# 2. Función de micro-lote para fusión idempotente en Delta Lake
def procesar_micro_lote(df_micro_lote, batch_id):
    delta_tabla = DeltaTable.forPath(spark, "lakehouse/silver/clientes")
    
    # Aplicar MERGE INTO transaccional
    delta_tabla.alias("target").merge(
        df_micro_lote.alias("source"),
        "target.cliente_id = source.cliente_id"
    ).whenMatchedUpdate(
        condition="source.operacion = 'U' AND source.timestamp_evento > target.timestamp_evento",
        set={"saldo_actual": "source.saldo_actual", "timestamp_evento": "source.timestamp_evento"}
    ).whenMatchedDelete(
        condition="source.operacion = 'D'"
    ).whenNotMatchedInsert(
        condition="source.operacion = 'I'",
        values={"cliente_id": "source.cliente_id", "saldo_actual": "source.saldo_actual", "timestamp_evento": "source.timestamp_evento"}
    ).execute()

# 3. Disparador de escritura continuo
query = df_stream.writeStream \\
    .foreachBatch(procesar_micro_lote) \\
    .option("checkpointLocation", "s3://checkpoints/cdc_clientes/") \\
    .trigger(processingTime="10 seconds") \\
    .start()

query.awaitTermination()
```

---

> **Consideraciones de Producción 22.1: Gestión de Marcas de Agua (Watermarking)**
> Al calcular agregaciones basadas en ventanas temporales (ej. transacciones por minuto), los datos retrasados por latencia de red pueden alterar ventanas pasadas. La instrucción `.withWatermark("timestamp_evento", "10 minutes")` define el límite temporal aceptable de retraso; transacciones que lleguen con más de 10 minutos de demora son descartadas para liberar memoria de estado del ejecutor.

---

### 22.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Compartir el Directorio de Checkpoint entre Consultas Distintas**  
Cada consulta de streaming debe poseer un directorio de checkpoint exclusivo en almacenamiento de objetos persistente. Reutilizar la misma ruta corrompe el registro de offsets y hace colapsar el pipeline.

**Anti-patrón 2: Operaciones No Deterministas en Micro-Lotes**  
Usar funciones no deterministas como `current_timestamp()` o `uuid()` en condiciones de unión o lógica de negocio produce resultados inconsistentes ante reintentos de micro-lotes por fallo de nodos.

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. ¿Cómo garantiza el directorio de checkpoint la recuperación transparente ante caídas del Driver en Structured Streaming?
2. Explica la diferencia entre los modos de salida (*Output Modes*): `Append`, `Complete` y `Update`.
3. ¿Por qué el método `foreachBatch` es la estrategia idónea para integrar flujos de Kafka con operaciones `MERGE` en Delta Lake?
""",

    "23-orquestacion-airflow-adf.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Airflow vs ADF]], [[Apache Airflow]] -->

# Capítulo 23: Orquestación de Flujos: Apache Airflow y Azure Data Factory

> *"Un clúster distribuido capaz de computar terabytes en segundos carece de utilidad productiva si sus tareas se ejecutan a ciegas. La orquestación es el sistema nervioso central que coordina dependencias, gestiona reintentos y garantiza la puntualidad del flujo de datos."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender el rol de los orquestadores de datos frente a los planificadores de tareas simples (*Cron*).
* Diseñar grafos acíclicos dirigidos (DAGs) en Apache Airflow bajo el paradigma de configuración como código.
* Evaluar las fortalezas y casos de uso entre Apache Airflow y Azure Data Factory (ADF).
* Implementar patrones de orquestación basados en sensores, reintentos exponenciales y alertas operativas.

---

> **Definición 23.1 (Orquestación de Pipelines de Datos)**
> La **orquestación de datos** es la automatización programática del ciclo de vida de los pipelines. Abarca la programación cronológica o por eventos, la resolución de dependencias entre tareas heterogéneas, la asignación de recursos en motores de cómputo externos, la gestión de reintentos ante fallos transitorios y la emisión de métricas de cumplimiento de acuerdos de nivel de servicio (SLA).

---

## 23.1 Comparativa Arquitectónica: Apache Airflow frente a Azure Data Factory

| Criterio de Evaluación | Apache Airflow | Azure Data Factory (ADF) |
| :--- | :--- | :--- |
| **Filosofía de Diseño** | Pipelines como código puro en Python (*Code-First*). | Orquestación visual declarativa basada en JSON (*Low-Code/UI-First*). |
| **Flexibilidad y Lógica Compleja** | Ilimitada; soporta bucles dinámicos, librerías personalizadas y SDKs. | Limitada por los bloques y actividades disponibles en la interfaz. |
| **Gestión de Infraestructura** | Requiere administración (Kubernetes, Celery, Astronomer, MWAA). | Servicio totalmente administrado (*Serverless / PaaS*) en Azure. |
| **Conectividad Híbrida / Multi-Nube** | Excelente en cualquier entorno (AWS, Azure, GCP, On-Premise). | Óptima integración con el ecosistema Azure; compleja fuera de él. |
| **Versionamiento y CI/CD** | Flujo estándar de Git, testing unitario con pytest y linting. | Exportación de plantillas ARM/Bicep mediante ramas de colaboración. |

---

### 23.2 Profundización Técnica: DAG de Producción en Apache Airflow

En entornos de producción, Airflow no ejecuta el cómputo pesado dentro de sus workers; actúa como un **director de orquesta ligero** que delega el procesamiento a Databricks, Spark en Kubernetes o Snowflake:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['alertas_datos@empresa.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True
}

with DAG(
    dag_id='pipeline_facturacion_diaria',
    default_args=default_args,
    description='Orquestación de capas Bronze, Silver y Gold de facturación',
    schedule_interval='0 4 * * *',  # Ejecución diaria a las 04:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['finanzas', 'medallon', 'databricks']
) as dag:

    inicio = EmptyOperator(task_id='inicio_pipeline')

    # 1. Tarea delegada: Ingesta Bronze
    ingesta_bronze = DatabricksSubmitRunOperator(
        task_id='ingestar_bronze_ventas',
        databricks_conn_id='databricks_default',
        new_cluster={
            'spark_version': '14.3.x-scala2.12',
            'node_type_id': 'Standard_D4ds_v5',
            'num_workers': 2
        },
        notebook_task={'notebook_path': '/Pipelines/01_Bronze_Ventas'}
    )

    # 2. Tarea delegada: Transformación Silver con validaciones
    transformacion_silver = DatabricksSubmitRunOperator(
        task_id='transformar_silver_ventas',
        databricks_conn_id='databricks_default',
        notebook_task={'notebook_path': '/Pipelines/02_Silver_Ventas'}
    )

    fin = EmptyOperator(task_id='fin_pipeline')

    # Definición formal del grafo de dependencias
    inicio >> ingesta_bronze >> transformacion_silver >> fin
```

---

> **Consideraciones de Producción 23.1: El Principio de Idempotencia en Orquestación**
> Toda tarea orquestada debe ser estrictamente **idempotente**: si un DAG se ejecuta tres veces para la misma fecha lógica (`logical_date` o `execution_date`), el resultado en las tablas de destino debe ser idéntico al de una única ejecución, sin crear duplicados ni cálculos inconsistentes.

---

### 23.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Procesar Transformaciones Pesadas dentro del Worker de Airflow**  
Ejecutar scripts que carguen DataFrames masivos en la memoria del nodo de Airflow causa desbordamientos de memoria (*OOM*) y bloquea la planificación de otras tareas. Airflow solo debe disparar y monitorear trabajos externos.

**Anti-patrón 2: Utilizar `catchup=True` Sin Considerar Carga en Fuentes Operacionales**  
Activar catchup en un DAG que estuvo inactivo durante meses generará cientos de ejecuciones simultáneas, saturando las bases de datos transaccionales de origen y consumiendo excesivos créditos cloud.

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. ¿Por qué los pipelines definidos como código en Airflow ofrecen mayor auditabilidad que los flujos visuales en ADF?
2. Explica la función de `logical_date` (antiguamente `execution_date`) en la garantía de idempotencia en Airflow.
3. Diseña una estrategia para orquestar la ejecución de un modelo dbt inmediatamente después de que finalice un micro-batch de Spark.
""",

    "24-dbt-modelado-e-idempotencia.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[dbt]], [[Idempotencia en Data Pipelines]], [[Anatomía de un Pipeline de Datos Moderno]] -->

# Capítulo 24: Transformación Modular con dbt e Idempotencia

> *"La capa de transformación en el Lakehouse no debe ser un laberinto de scripts aislados. dbt aplica las mejores prácticas del desarrollo de software —control de versiones, pruebas automatizadas, modularidad e integración continua— a las consultas analíticas de datos."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender la arquitectura de dbt (*data build tool*) y el cambio de paradigma de ETL a ELT.
* Diseñar modelos modulares de staging, intermediate y marts utilizando Jinja y macros.
* Implementar estrategias de materialización incremental idóneas para tablas masivas.
* Garantizar rigurosamente la propiedad de idempotencia en pipelines de transformación.

---

> **Definición 24.1 (Idempotencia en Ingeniería de Datos)**
> Una operación $f(x)$ se define como **idempotente** si la aplicación reiterada de dicha operación sobre un mismo estado produce exactamente el mismo resultado final:
> $$f(f(x)) = f(x)$$
> En pipelines analíticos, esto implica que reejecutar una transformación sobre una partición temporal o conjunto de datos determinado no genera registros duplicados ni altera los agregados históricos.

---

## 24.1 La Filosofía de dbt en el Ecosistema Lakehouse

dbt no extrae ni carga datos; opera exclusivamente sobre la capa de transformación dentro del almacén o Lakehouse (Snowflake, BigQuery, Databricks, Redshift). Transforma sentencias `SELECT` simples en tablas y vistas optimizadas:

```
[ Datos Crudos / Staging ] ──> [ Modelos Intermediate ] ──> [ Marts / Consumo ]
       stg_pedidos.sql               int_pedidos_agrupados.sql          fct_ventas_diarias.sql
```

Beneficios angulares de dbt:
1. **Linaje Automático mediante la función `ref()`:** Permite a dbt deducir el orden óptimo de compilación y ejecución del DAG.
2. **Pruebas de Calidad Declarativas:** Validación automática de unicidad, nulos, integridad referencial y rangos de negocio.
3. **Documentación Dinámica:** Generación de portales de linaje interactivos a partir del código fuente.

---

### 24.2 Profundización Técnica: Modelado Incremental e Idempotente en dbt

Para procesar únicamente los datos modificados desde la última ejecución sin escanear la tabla entera, dbt implementa materializaciones incrementales con claves de combinación (*unique_key*):

```sql
{{ config(
    materialized='incremental',
    unique_key='transaccion_id',
    incremental_strategy='merge',
    file_format='delta'
) }}

WITH eventos_recientes AS (
    SELECT 
        transaccion_id,
        cliente_id,
        monto_total,
        fecha_actualizacion,
        estado
    FROM {{ ref('stg_transacciones') }}
    
    {% if is_incremental() %}
        -- En ejecuciones incrementales, filtrar solo registros nuevos o modificados
        WHERE fecha_actualizacion > (SELECT coalesce(max(fecha_actualizacion), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT * FROM eventos_recientes
```

La estrategia `merge` garantiza que:
* Registros con un `transaccion_id` nuevo sean insertados.
* Registros cuyo `transaccion_id` ya exista sean actualizados con los valores recientes.
* Reintentos o reejecuciones del modelo produzcan el estado final consistente sin duplicaciones.

---

> **Consideraciones de Producción 24.1: Patrón de Sobrescritura de Partición Dinámica**
> En cargas por lotes particionadas por fecha, la alternativa más veloz y segura para garantizar idempotencia es el reemplazo atómico de partición (*Partition Overwrite*). En lugar de eliminar y reinsertar registros de forma secuencial, el motor reemplaza el bloque de archivos de la fecha en una única transacción de commit.

---

### 24.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Uso de `INSERT INTO` Ciego sin Clave Única ni Limpieza Previa**  
Ejecutar inserciones acumulativas sin verificar la existencia de los registros produce duplicaciones masivas cada vez que el pipeline se reintenta tras un fallo en red.

**Anti-patrón 2: Consultar Tablas Físicas sin Utilizar la Función `{{ ref(...) }}`**  
Hacer referencia directa a nombres rígidos (`db.schema.tabla`) dentro de los modelos dbt rompe el grafo de dependencias, impidiendo que dbt orqueste el linaje y ejecute las pruebas en el orden correcto.

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. ¿Por qué la idempotencia es un requisito innegociable en sistemas de datos con acuerdos de servicio (SLA) críticos?
2. Explica cómo la macro `is_incremental()` optimiza el consumo de cómputo en tablas con miles de millones de filas.
3. Define la diferencia entre las materializaciones `view`, `table`, `incremental` y `ephemeral` en dbt.
""",

    "25-infraestructura-como-codigo-terraform.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Terraform]], [[IaC]], [[Data Architecture]] -->

# Capítulo 25: Infraestructura como Código (IaC) con Terraform

> *"En la ingeniería de datos moderna, la infraestructura no se configura haciendo clic en consolas web. Si un clúster, un bucket de almacenamiento o una política de seguridad no están definidos en código versionado, el sistema no es reproducible ni confiable."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender los principios de Infraestructura como Código (IaC) y el ciclo de vida de Terraform (*Plan*, *Apply*, *Destroy*).
* Desplegar recursos analíticos cloud (buckets de almacenamiento, Lakehouse, catálogos y clústeres) de forma determinista.
* Gestionar estados distribuidos y bloqueos de concurrencia mediante *State Locking*.
* Modularizar arquitecturas analíticas separando entornos de desarrollo, pruebas y producción.

---

> **Definición 25.1 (Infraestructura como Código y Terraform)**
> **Terraform** es una herramienta de código abierto desarrollada por HashiCorp que permite definir, aprovisionar y gestionar infraestructura en la nube mediante un lenguaje declarativo de alto nivel denominado HCL (*HashiCorp Configuration Language*). Aplica el principio de idempotencia a la infraestructura: calcula las discrepancias entre el estado real y el deseado, aplicando exclusivamente las modificaciones necesarias.

---

## 25.1 Ciclo de Vida y Arquitectura de Terraform en Datos

El flujo de trabajo estándar en equipos de datos sigue un ciclo riguroso de cuatro etapas:

```
[ Código HCL (.tf) ] ──> [ terraform init ]  (Descarga proveedores: AWS, Azure, Databricks)
                             │
                     [ terraform plan ]  (Cálculo del grafo de dependencias y diff)
                             │
                     [ terraform apply ] (Llamadas a API cloud y actualización de estado)
                             │
                     [ terraform.tfstate ] (Almacenamiento remoto seguro con bloqueo)
```

---

### 25.2 Profundización Técnica: Aprovisionamiento de un Lakehouse con Terraform

El siguiente manifiesto en HCL despliega el almacenamiento seguro para un Lakehouse en Azure (ADLS Gen2) y configura un workspace de Databricks:

```hcl
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80.0"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "sttfstatecorp"
    container_name       = "tfstate"
    key                  = "data-platform.prod.tfstate"
  }
}

provider "azurerm" {
  features {}
}

# 1. Cuenta de almacenamiento para el Lakehouse con espacio de nombres jerárquico (HNS)
resource "azurerm_storage_account" "lakehouse_storage" {
  name                     = "stlakehousecorp01"
  resource_group_name      = "rg-datos-produccion"
  location                 = "eastus2"
  account_tier             = "Standard"
  account_replication_type = "ZRS" # Resiliencia entre zonas de disponibilidad
  is_hns_enabled           = true  # Habilita ADLS Gen2 para operaciones atómicas de directorio

  blob_properties {
    delete_retention_policy {
      days = 30
    }
  }

  tags = {
    Ambiente = "Produccion"
    Equipo   = "DataEngineering"
  }
}

# 2. Contenedores de almacenamiento para la Arquitectura Medallón
resource "azurerm_storage_data_lake_gen2_filesystem" "capas" {
  for_each           = toset(["bronze", "silver", "gold"])
  name               = each.key
  storage_account_id = azurerm_storage_account.lakehouse_storage.id
}
```

---

> **Consideraciones de Producción 25.1: Seguridad del Archivo de Estado (tfstate)**
> El archivo `terraform.tfstate` contiene información crítica del entorno e incluso credenciales o claves autogeneradas. En entornos productivos, es obligatorio almacenar el estado en un backend remoto con cifrado en reposo (ej. S3 con KMS o Azure Blob Storage con encriptación administrada) y utilizar bloqueo distribuido (DynamoDB o Azure Blob Leases) para evitar colisiones de despliegue simultáneo.

---

### 25.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Modificar Recursos Directamente en la Consola Cloud (*ClickOps*)**  
Hacer modificaciones manuales en la consola de AWS o Azure genera desviación de configuración (*Configuration Drift*), provocando que futuros despliegues de Terraform fallen o reviertan cambios críticos sin previo aviso.

**Anti-patrón 2: Almacenar Secretos y Llaves en Archivos de Terraform Versionados**  
Incrustar tokens de acceso o contraseñas en archivos `.tf` y subirlos a Git compromete la seguridad corporativa. Todos los secretos deben consumirse mediante Azure Key Vault, AWS Secrets Manager o HashiCorp Vault.

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. ¿Qué es la desviación de configuración (*Configuration Drift*) y cómo la detecta el comando `terraform plan`?
2. Explica la importancia de habilitar el espacio de nombres jerárquico (*Hierarchical Namespace*) en cuentas de almacenamiento analítico.
3. Diseña la estructura de carpetas para un proyecto de Terraform que gestione los ambientes `dev`, `staging` y `prod` utilizando módulos reutilizables.
""",

    "26-cicd-y-portabilidad-pipelines.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[CI/CD]], [[Portabilidad de Pipelines Spark]] -->

# Capítulo 26: Integración y Despliegue Continuo (CI/CD) para Pipelines

> *"Un pipeline de datos es software de misión crítica. Si el código analítico no cuenta con pruebas unitarias automatizadas, análisis estático y un flujo de despliegue determinista, cada paso a producción se convierte en una ruleta rusa de corrupción de datos."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender la aplicación de los principios DevOps a los activos de datos (*DataOps*).
* Diseñar pipelines de Integración Continua (CI) con validaciones léxicas, pruebas unitarias y linters.
* Implementar estrategias de Despliegue Continuo (CD) y promoción entre entornos (*dev*, *staging*, *prod*).
* Asegurar la portabilidad de aplicaciones Spark mediante parametrización desacoplada y contenedores Docker.

---

> **Definición 26.1 (DataOps y CI/CD en Ingeniería de Datos)**
> **DataOps** es una metodología ágil orientada al proceso que combina ingeniería de software, DevOps y control de calidad para reducir el ciclo de desarrollo analítico y elevar la confiabilidad de los datos. La **Integración Continua (CI)** valida automáticamente cada cambio mediante suites de pruebas automatizadas, mientras que el **Despliegue Continuo (CD)** promueve artefactos compilados (paquetes Wheel, plantillas dbt, DAGs) hacia entornos productivos sin intervención manual.

---

## 26.1 Anatomía de un Flujo de Trabajo CI/CD Moderno

```
[ Developer Branch ] ──> git push ──> [ CI Pipeline (GitHub Actions) ]
                                            ├── Linting (ruff, black, sqlfluff)
                                            ├── Pruebas Unitarias (pytest con Chispa)
                                            └── Validación de Contratos
                                                    │
                                            [ Pull Request Aprobado ]
                                                    │
                                            [ CD Pipeline (Producción) ]
                                            ├── Empaquetado (.whl / container)
                                            ├── Despliegue a Unity Catalog / Storage
                                            └── Notificación a Slack / PagerDuty
```

---

### 26.2 Profundización Técnica: Pruebas Unitarias en PySpark y Pipeline de GitHub Actions

Escribir pruebas unitarias sobre DataFrames distribuidos requiere librerías especializadas como `chispa` para comparar esquemas y contenido fila por fila:

```python
# test_transformaciones_clientes.py
import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from pipelines.clientes import estandarizar_clientes

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \\
        .master("local[2]") \\
        .appName("PruebasUnitariasSpark") \\
        .getOrCreate()

def test_estandarizar_clientes(spark):
    datos_origen = [("  luis galvis ", "ACTIVO"), ("ANA PEREZ", "inactivo")]
    df_entrada = spark.createDataFrame(datos_origen, ["nombre", "estado"])

    df_resultado = estandarizar_clientes(df_entrada)

    datos_esperados = [("LUIS GALVIS", "ACTIVO"), ("ANA PEREZ", "INACTIVO")]
    df_esperado = spark.createDataFrame(datos_esperados, ["nombre", "estado"])

    assert_df_equality(df_resultado, df_esperado, ignore_row_order=True)
```

Pipeline automatizado en GitHub Actions (`.github/workflows/ci.yml`):

```yaml
name: CI Data Engineering Pipeline

on:
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Configurar Python 3.10
        uses: actions/setup-python@v4
        with:
          python-version: "3.10"

      - name: Instalar Dependencias
        run: |
          pip install --upgrade pip
          pip install ruff pytest pyspark chispa

      - name: Análisis Estático y Linting
        run: ruff check .

      - name: Ejecución de Pruebas Unitarias
        run: pytest tests/ -v
```

---

> **Consideraciones de Producción 26.1: Portabilidad y Desacoplamiento de Rutas**
> Para que un pipeline sea portátil entre entornos (desarrollo local, clúster de prueba y producción), está prohibido hardcodear rutas físicas como `s3://bucket-prod/ventas.parquet`. Las rutas deben inyectarse mediante variables de entorno o archivos de configuración de entorno (`config.yaml`).

---

### 26.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Probar en Producción con Datos Reales sin Entorno de Staging**  
Ejecutar código no probado sobre tablas de producción expone a la organización a corrupción de métricas financieras y pérdidas económicas irreparables.

**Anti-patrón 2: Desplegar Jobs sin Tests de Regresión**  
Modificar la lógica de una transformación intermedia sin validar si rompe modelos de consumo aguas abajo genera incidentes silenciosos de calidad de datos.

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. ¿Cómo permite la librería `chispa` comparar dos DataFrames distribuidos tolerando variaciones en el orden físico de las filas?
2. Explica la diferencia entre pruebas unitarias (*Unit Tests*), pruebas de integración (*Integration Tests*) y pruebas de contratos de datos (*Contract Tests*).
3. Diseña una estrategia para promover un pipeline de Spark desde el entorno `dev` hasta `prod` asegurando que las credenciales de base de datos se roten automáticamente.
""",

    "27-observabilidad-monitoreo-logging.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Spark Logging and Monitoring]], [[Observability]] -->

# Capítulo 27: Observabilidad, Monitoreo y Logging en Sistemas Distribuidos

> *"La monitorización tradicional pregunta: '¿Está el servidor encendido?'. La observabilidad moderna responde: '¿Por qué falló la partición 842 del micro-lote hace 15 minutos en un clúster de 50 nodos?'. En entornos distribuidos, la observabilidad es la única defensa contra la degradación silenciosa."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender los tres pilares de la observabilidad en datos: Métricas, Logs estructurados y Trazas (*Tracing*).
* Interpretar la interfaz Spark UI para detectar cuellos de botella de memoria, shuffles excesivos y *Data Skew*.
* Configurar logging estructurado en formato JSON integrado con Log4j2 y sistemas de indexación centralizada.
* Establecer alertas proactivas basadas en acuerdos de nivel de servicio de datos (*Data SLAs*).

---

> **Definición 27.1 (Observabilidad en Plataformas de Datos)**
> La **observabilidad de datos** es la capacidad de inferir los estados internos y la salud integral de un sistema analítico a partir de sus salidas externas. Abarca no solo el estado de la infraestructura computacional (CPU, memoria, I/O), sino la salud y frescura de los datos mismos (volumen inusual, deriva de esquema, latencia de procesamiento).

---

## 27.1 Diagnóstico de Rendimiento mediante Spark UI

La interfaz web de Spark UI es el instrumento primario de diagnóstico en tiempo de ejecución. Los ingenieros deben dominar cuatro pestañas críticas:

```
[ Spark UI ]
    ├── Jobs:      Resumen de acciones disparadas y tiempos de respuesta globales.
    ├── Stages:    Desglose del DAG. Muestra shuffles, tareas por ejecutor y varianza temporal.
    ├── Storage:   Memoria consumida por DataFrames cacheados (Deserialized vs. Serialized).
    ├── Executors: Consumo de Garbage Collection (GC Time), Shuffle Spill (Memory / Disk).
    └── SQL / DF:  Planes de ejecución físicos detallados generados por Catalyst.
```

---

### 27.2 Profundización Técnica: Logging Estructurado con Log4j2 y PySpark

En aplicaciones distribuidas, escribir logs en texto plano con `print()` satura el Driver y dificulta la búsqueda automatizada. El estándar de ingeniería exige emitir logs en formato JSON estructurado:

```python
import json
import logging
from pyspark.sql import SparkSession

class StructuredLogger:
    def __init__(self, app_name):
        self.logger = logging.getLogger(app_name)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.app_name = app_name

    def info(self, mensaje, **kwargs):
        payload = {
            "nivel": "INFO",
            "aplicacion": self.app_name,
            "mensaje": mensaje,
            **kwargs
        }
        self.logger.info(json.dumps(payload))

# Inicialización en pipeline de Spark
spark = SparkSession.builder.appName("AuditoriaPipeline").getOrCreate()
log = StructuredLogger("AuditoriaPipeline")

log.info("Iniciando procesamiento de micro-lote", micro_lote_id=452, particiones=200)

df = spark.read.parquet("data/transacciones.parquet")
total_filas = df.count()

log.info("Cálculo completado exitosamente", registros_leidos=total_filas, duracion_segundos=14.2)
```

---

> **Consideraciones de Producción 27.1: Detección de GC Overhead en Ejecutores**
> En la pestaña *Executors* de Spark UI, si la métrica de tiempo de recolección de basura (*Task Deserialization / GC Time*) supera el 10% del tiempo total de la tarea, indica agotamiento de memoria heap en la JVM. La solución requiere ajustar `spark.memory.fraction`, reducir el tamaño de las particiones o migrar operaciones hacia Tungsten off-heap.

---

### 27.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Descargar Logs Masivos al Disco Local del Driver**  
Escribir logs de depuración (*DEBUG*) sin rotación en clústeres de alta concurrencia llena el disco del nodo Driver y provoca la muerte catastrófica de toda la aplicación.

**Anti-patrón 2: Alertamiento Basado Únicamente en Fallos Duros (*Hard Failures*)**  
Un pipeline puede ejecutarse sin errores técnicos (`exit code 0`) pero generar 0 filas o duplicar todos los valores. El sistema de observabilidad debe monitorear anomalías estadísticas en el volumen y frescura de los datos.

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. ¿Qué indicadores en la pestaña *Stages* de Spark UI evidencian la presencia de sesgo de datos (*Data Skew*)?
2. Explica la diferencia entre *Shuffle Read / Write* y *Shuffle Spill (Memory vs. Disk)*.
3. Diseña una regla de alertamiento para notificar si una tabla de streaming en Delta Lake no recibe registros nuevos en un intervalo de 30 minutos.
""",

    "28-calidad-datos-great-expectations.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Data Quality]], [[Great Expectations]] -->

# Capítulo 28: Calidad de Datos (Data Quality) y Validación Automatizada

> *"Procesar datos corruptos con alta velocidad computacional no es una ventaja competitiva; es la forma más rápida de tomar decisiones catastróficas. La calidad de datos no se inspecciona al final en los tableros de BI; se valida como una compuerta innegociable en cada etapa del pipeline."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender las seis dimensiones formales de la calidad de datos: Exactitud, Completitud, Consistencia, Validez, Puntualidad y Unicidad.
* Implementar contratos de datos declarativos mediante Great Expectations y Soda.
* Diseñar compuertas de calidad (*Quality Gates*) que impidan la propagación de datos corruptos de Bronze a Silver.
* Automatizar la generación de reportes de auditoría y documentación de calidad viva (*Data Docs*).

---

> **Definición 28.1 (Calidad de Datos y Great Expectations)**
> **Great Expectations (GX)** es el estándar de código abierto para la validación, documentación y perfilado de datos en ingeniería. Permite a los equipos declarar afirmaciones verificables denominadas **Expectations** sobre columnas, tablas y distribuciones estadísticas, ejecutándolas automáticamente contra DataFrames distribuidos en Spark, motores SQL o almacenes cloud.

---

## 28.1 Las 6 Dimensiones Universales de Calidad de Datos

```
┌────────────────────────────────────────────────────────┐
│         LAS 6 DIMENSIONES DE CALIDAD DE DATOS          │
├────────────────────┬───────────────────────────────────┤
│ 1. Completitud     │ ¿Faltan registros o existen nulos?│
│ 2. Unicidad        │ ¿Existen duplicados en las claves?│
│ 3. Validez         │ ¿Cumplen el formato y dominio?    │
│ 4. Exactitud       │ ¿Reflejan la realidad del negocio?│
│ 5. Consistencia    │ ¿Concuerdan entre sistemas?       │
│ 6. Puntualidad     │ ¿Están disponibles a tiempo?      │
└────────────────────┴───────────────────────────────────┘
```

---

### 28.2 Profundización Técnica: Compuerta de Calidad en PySpark con Great Expectations

En arquitecturas Medallón de producción, antes de escribir los datos en la capa Silver, el DataFrame debe atravesar una suite de validación estricta:

```python
import great_expectations as gx
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataQualityGate").getOrCreate()

# Carga de datos crudos (Capa Bronze)
df_ventas = spark.read.parquet("lakehouse/bronze/ventas_crudo.parquet")

# Conexión al contexto de Great Expectations
context = gx.get_context()
data_source = context.data_sources.add_spark("spark_datasource")
data_asset = data_source.add_dataframe_asset("ventas_asset")
batch_request = data_asset.build_batch_request(dataframe=df_ventas)

# Definición declarativa de expectativas de negocio
suite = context.suites.add(gx.ExpectationSuite(name="suite_ventas_silver"))

suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="transaccion_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="transaccion_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="monto_usd", min_value=0.01, max_value=500000.00))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="estado_pago", value_set=["APROBADO", "RECHAZADO", "PENDIENTE"]))

# Ejecución de la validación
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(name="val_ventas", data=batch_request, suite=suite)
)

resultado = validation_definition.run()

if not resultado.success:
    print("ALERTA CRÍTICA: Fallo en compuerta de calidad de datos. Cancelando promoción a Silver.")
    # Enviar alerta a Slack y desviar registros erróneos a cuarentena
    raise ValueError("Violación de contrato de calidad de datos.")
else:
    print("Validación exitosa. Promoviendo datos a la capa Silver.")
    df_ventas.write.format("delta").mode("append").save("lakehouse/silver/ventas")
```

---

> **Consideraciones de Producción 28.1: Patrón de Cuarentena de Datos**
> Si un micro-lote contiene 99% de datos válidos y 1% de registros con errores leves (ej. código postal mal formado), rechazar el 100% del lote detiene la operación del negocio. El patrón estándar consiste en segregar condicionalmente: los datos válidos continúan a Silver, mientras que las filas defectuosas se escriben en una tabla `silver_cuarentena` con metadatos del error para remediación posterior.

---

### 28.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Validaciones Mediante Consultas SQL Ad-Hoc Desconectadas**  
Escribir scripts artesanales con `SELECT COUNT(*) WHERE columna IS NULL` sin un marco estructurado genera deuda técnica y oculta el linaje de los fallos a los consumidores.

**Anti-patrón 2: Tratar las Validaciones como Pasos Opcionales en Horas Pico**  
Desactivar las compuertas de calidad para "acelerar el procesamiento" ante retrasos operacionales garantiza la contaminación de los modelos de Machine Learning y los reportes financieros.

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. Explica la diferencia entre una validación bloqueante (*Hard Gate*) y una validación informativa (*Soft Gate*).
2. ¿Cómo previene el patrón de cuarentena la detención total de un pipeline sin comprometer la integridad de la capa Silver?
3. Diseña una expectativa para validar que la columna `fecha_transaccion` contenga fechas pertenecientes al mes calendario actual.
""",

    "29-cloud-data-architecture-y-costos.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Cloud Architecture]], [[FinOps]] -->

# Capítulo 29: Cloud Data Architecture y Optimización de Costos en la Nube

> *"La nube ofrece escalabilidad infinita, pero esa misma elasticidad puede desencadenar facturas astronómicas si la arquitectura no está gobernada por principios de eficiencia computacional y FinOps."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Diseñar arquitecturas de datos modernas sobre los tres hiperescaladores principales: AWS, Azure y Google Cloud (GCP).
* Comprender los modelos de costos de almacenamiento de objetos, cómputo distribuido y transferencia de red (*Egress*).
* Aplicar estrategias de optimización FinOps para reducir hasta un 60% el costo de clústeres Spark y almacenes analíticos.
* Implementar políticas de ciclo de vida de almacenamiento (*Tiering*) y uso eficiente de instancias *Spot/Preemptible*.

---

> **Definición 29.1 (FinOps en Plataformas de Datos)**
> **FinOps** (*Financial Operations*) es la disciplina cultural y operativa que promueve la responsabilidad financiera compartida en el consumo de recursos cloud. En ingeniería de datos, articula el balance óptimo entre latencia, rendimiento y costo, asegurando que cada dólar invertido en infraestructura produzca un retorno cuantificable de valor analítico.

---

## 29.1 Mapeo de Servicios entre Hiperescaladores

```
┌──────────────────────────┬──────────────────┬──────────────────┬──────────────────┐
│ Componente Analítico     │ AWS              │ Microsoft Azure  │ Google Cloud     │
├──────────────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Almacenamiento de Objetos│ Amazon S3        │ ADLS Gen2        │ Cloud Storage    │
│ Lakehouse / Spark        │ EMR / Databricks │ Databricks / Synapse│ Dataproc / BigLake │
│ Almacén Serverless (DWH) │ Redshift Serverless│ Synapse Dedicated│ BigQuery         │
│ Mensajería de Streaming  │ Amazon MSK / Kinesis│ Event Hubs / Kafka│ Cloud Pub/Sub    │
│ Orquestación             │ MWAA (Airflow)   │ Azure Data Factory│ Cloud Composer   │
└──────────────────────────┴──────────────────┴──────────────────┴──────────────────┘
```

---

### 29.2 Profundización Técnica: Estrategias de Reducción de Costos en Producción

Las cuatro palancas primarias para optimizar el gasto en cómputo distribuido son:

1. **Uso de Instancias Spot / Preemptible:**  
   Los nodos Worker en clústeres Spark son efímeros y tolerantes a fallos. Emplear instancias Spot para los Workers reduce el costo de CPU hasta un 70-80% respecto a instancias On-Demand. El nodo Driver debe permanecer obligatoriamente en On-Demand para evitar la muerte del trabajo si la nube reclama la máquina.

2. **Supresión de Costos de Transferencia de Red (*Data Egress*):**  
   Mover datos entre distintas regiones de la nube o entre zonas de disponibilidad genera costos elevados por gigabyte. Todos los recursos del pipeline (almacenamiento, clúster y orquestador) deben coexistir estrictamente en la misma región cloud.

3. **Autoscaling y Apagado Automático (*Auto-Termination*):**  
   Configurar políticas agresivas de auto-terminación (ej. apagar clústeres interactivos tras 15 minutos de inactividad) previene cobros innecesarios por máquinas ociosas durante noches y fines de semana.

4. **Ciclo de Vida de Almacenamiento (*Storage Tiering*):**  
   Mover automáticamente archivos de la capa Bronze no consultados en 90 días a niveles fríos (*AWS S3 Glacier Instant Retrieval*, *Azure Cool/Archive Tier*) reduce el costo de almacenamiento a una fracción mínima.

---

> **Consideraciones de Producción 29.1: Compresión Columnar y Optimización de Archivos**
> Almacenar datos en Parquet o Delta Lake con compresión Snappy o ZSTD no solo reduce el volumen en disco hasta un 80% frente a CSV, sino que minimiza el I/O de red y acelera las consultas en motores con facturación por escaneo como BigQuery o Athena.

---

### 29.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Mantener Clústeres de Tamaño Fijo Encendidos 24/7 para Cargas por Lotes**  
Pagar por clústeres dedicados permanentes para procesar cargas que solo duran 2 horas al día desperdicia más del 90% del presupuesto operativo. Se deben emplear clústeres efímeros creados y destruidos por el orquestador (*Job Clusters*).

**Anti-patrón 2: Consultar Tablas Masivas con `SELECT *` en Motores Serverless**  
En motores como Snowflake o BigQuery, una consulta sin filtros ni proyecciones escanea terabytes enteros, generando sobrecostos inmediatos de cientos de dólares por una sola consulta mal estructurada.

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. ¿Por qué es seguro colocar ejecutores de Spark en instancias Spot pero arriesgado hacer lo mismo con el nodo Driver?
2. Explica cómo la compresión columnar impacta directamente el costo en servicios de consulta serverless.
3. Diseña una política de ciclo de vida de almacenamiento para una arquitectura Medallón con retención regulatoria de 5 años.
""",

    "30-system-design-y-preparacion-senior.md": """<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[System Design]], [[Data Engineer Senior]] -->

# Capítulo 30: System Design para Data Engineers y Preparación para Roles Senior

> *"Un ingeniero de datos junior aprende a utilizar herramientas; un ingeniero senior sabe cuándo NO utilizarlas. El verdadero dominio técnico consiste en balancear compromisos arquitectónicos (trade-offs) entre latencia, consistencia, costo y simplicidad operativa."*

---

### [AÑADIDO] Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Abordar problemas de diseño de sistemas de datos a gran escala (*Data System Design*) con una metodología estructurada.
* Dominar los compromisos fundamentales de la computación distribuida: Teorema CAP, PACELC y modelos de consistencia.
* Diseñar arquitecturas analíticas extremo a extremo para casos de uso empresariales reales.
* Consolidar el criterio profesional y las competencias de liderazgo técnico para alcanzar el nivel Senior.

---

> **Definición 30.1 (Data System Design)**
> **Data System Design** es la disciplina de diseñar arquitecturas de datos complejas, escalables, tolerantes a fallos y mantenibles para resolver problemas de negocio ambiguos. Requiere estimar volúmenes de datos, definir acuerdos de nivel de servicio (SLA/SLO), seleccionar motores de almacenamiento y procesamiento adecuados, y defender los compromisos de diseño frente a limitaciones financieras y operativas.

---

## 30.1 Metodología de 5 Pasos para Resolver Problemas de System Design

Ante un desafío de diseño (ej. *"Diseña una plataforma para procesar y alertar sobre 50 millones de transacciones financieras diarias en tiempo real"*), el ingeniero senior no empieza dibujando diagramas ni recomendando tecnologías. Aplica una estructura rigurosa:

```
[ PASO 1: Clarificación de Requisitos ]
   ├── Requisitos Funcionales: ¿Qué debe hacer el sistema?
   └── Requisitos No Funcionales: Volumen, Latencia, SLA, RPO/RTO.
                 │
[ PASO 2: Estimaciones de Escala (Back-of-the-Envelope) ]
   ├── Throughput: QPS (Queries per second), eventos por segundo.
   └── Almacenamiento: Gigabytes/día, terabytes/año, retención.
                 │
[ PASO 3: Diseño de Alto Nivel ]
   └── Diagrama de bloques: Ingesta ──> Cómputo ──> Almacenamiento ──> Servicio.
                 │
[ PASO 4: Inmersión Profunda en Componentes Críticos ]
   └── Manejo de fallos, particionamiento, semántica exactly-once, CDC.
                 │
[ PASO 5: Cuellos de Botella y Compromisos (Trade-offs) ]
   └── Costo vs. Latencia, consistencia eventual vs. ACID, escalabilidad futura.
```

---

### 30.2 Profundización Técnica: Estimaciones de Escala (Back-of-the-Envelope)

Un ingeniero senior respalda sus decisiones en cálculos matemáticos rápidos:

* **Escenario:** 50 millones de transacciones diarias.
* **Eventos por segundo promedio:**
  $$\text{QPS} = \frac{50\,000\,000 \text{ eventos}}{86\,400 \text{ segundos}} \approx 578 \text{ eventos/seg}$$
* **QPS pico (factor de 3x):**
  $$\text{QPS}_{\text{pico}} \approx 1\,736 \text{ eventos/seg}$$
* **Volumen de almacenamiento diario (asumiendo 1 KB por evento en JSON crudo):**
  $$\text{Volumen Diario} = 50\,000\,000 \times 1 \text{ KB} = 50 \text{ GB/día}$$
* **Volumen anual comprimido en Parquet (ratio 5:1):**
  $$\text{Volumen Anual} \approx \frac{50 \text{ GB} \times 365}{5} \approx 3.65 \text{ TB/año}$$

**Conclusión Arquitectónica:**  
Con un pico de 1,736 eventos/segundo y 3.65 TB anuales, un clúster masivo de 50 nodos Spark es un sobrecosto injustificado. Un clúster pequeño de Kafka con 3 particiones y un pipeline en micro-lotes de Databricks con 2 ejecutores manejará holgadamente la carga a un costo mínimo.

---

> **Consideraciones de Producción 30.1: Criterio de Madurez de un Senior Data Engineer**
> Lo que distingue a un ingeniero senior no es la cantidad de tecnologías exóticas en su currículum, sino:
> 1. **Defensa de la Simplicidad:** Escoger la arquitectura más simple que cumpla los SLAs requeridos.
> 2. **Mentalidad de Fiabilidad:** Asumir que las redes fallarán, los esquemas mutarán y los discos se llenarán, diseñando idempotencia y alertas desde el día cero.
> 3. **Visión de Negocio:** Entender el impacto del dato en las decisiones corporativas y el retorno de inversión del cómputo cloud.

---

### 30.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Sobreingeniería (*Resume-Driven Development*)**  
Implementar clústeres complejos de streaming distribuido en tiempo real cuando el negocio únicamente consulta los datos una vez por semana en un reporte estático.

**Anti-patrón 2: Desestimar el Mantenimiento y la Operabilidad**  
Diseñar arquitecturas tan sofisticadas y frágiles que ningún otro ingeniero del equipo puede operarlas o corregir incidentes en guardias nocturnas.

---

## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos

1. ¿Cómo influyen las estimaciones rápidas de escala (*Back-of-the-Envelope*) en la elección entre procesamiento en tiempo real estricto y micro-lotes?
2. Explica el Teorema CAP y cómo se manifiesta en la arquitectura de almacenamiento de Delta Lake frente a Apache Cassandra.
3. En una entrevista de System Design, ¿cómo estructurarías la respuesta ante el requerimiento de diseñar un pipeline de detección de fraudes en tiempo real?
"""
}

print(f"Escribiendo {len(chapters)} capitulos restantes...")
for filename, content in chapters.items():
    filepath = os.path.join(CAP_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    if os.path.exists(MIRROR_DIR):
        with open(os.path.join(MIRROR_DIR, filename), 'w', encoding='utf-8') as f:
            f.write(content)
    print(f"  + Generado: {filename}")

print("Todos los capitulos 19 a 30 generados exitosamente!")
