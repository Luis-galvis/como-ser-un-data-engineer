<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[DuckDB]], [[In-Process OLAP]], [[Polars vs Spark]], [[Serverless Data]] -->

# Capítulo 2A: DuckDB y Cómputo In-Process: Cuándo No Usar Computación Distribuida [NÚCLEO EMPLEABILIDAD]

> *"Uno de los errores de diseño más costosos en la ingeniería de datos contemporánea es utilizar un cañón distribuido para cazar una mosca local. Durante una década, la industria convenció a los equipos de que cualquier análisis requería un clúster de Spark de ocho nodos. En la práctica, más del 90% de los conjuntos de datos analíticos caben en la memoria RAM de una sola máquina moderna. DuckDB demostró que el cómputo embebido, columnar y vectorizado en un solo nodo puede procesar decenas de gigabytes en segundos a una fracción del costo y complejidad de un clúster."*

---

## Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender la fundamentación arquitectónica de los motores analíticos en proceso (**In-Process OLAP**) frente a sistemas distribuidos masivos.
* Analizar los costos ocultos de la computación distribuida (serialización de red, sobrecosto de coordinación del Driver y latencia de shuffles).
* Ejecutar consultas SQL analíticas de alto rendimiento directamente sobre archivos **Parquet**, CSV y JSON sin cargarlos previamente en tablas físicas.
* Conectar DuckDB con almacenamiento de objetos en la nube (Amazon S3 / Azure ADLS) mediante la extensión `httpfs` con transferencias *Zero-Copy* hacia Apache Arrow y Pandas.
* Implementar canalizaciones analíticas Serverless de bajo costo utilizando **AWS Lambda** y DuckDB.
* Aplicar una matriz de decisión rigurosa para seleccionar entre **Pandas**, **Polars**, **DuckDB** y **Apache Spark**.

---

> **Definición 2A.1 (Base de Datos Analítica En-Proceso - In-Process OLAP)**
> DuckDB es un sistema de gestión de bases de datos relacionales enfocado en procesamiento analítico (OLAP), diseñado para ejecutarse **embebido dentro del mismo espacio de memoria del proceso anfitrión** (*In-Process*), análogo a lo que SQLite representa para cargas transaccionales (OLTP).
> 
> No requiere la instalación de un servidor independiente, carece de dependencias externas complejas y procesa datos mediante un **motor columnar vectorizado en C++** con paralelismo multihilo (*Morsel-Driven Parallelism*) y capacidad de procesamiento fuera de memoria (*Out-of-Core Execution*).

---

## 2A.1 El Mito del Big Data y el Costo Oculto de los Clústeres

A principios de la era de Hadoop y Spark, los servidores estándar disponían de 4 a 8 GB de RAM. Procesar 50 GB de datos forzaba obligatoriamente a distribuir la carga entre diez máquinas. 

Hoy, un computador portátil de desarrollo cuenta con 16 a 64 GB de memoria RAM, y una sola instancia en la nube (como una máquina `r6i.4xlarge` en AWS) ofrece **128 GB de RAM y 16 núcleos de CPU por menos de $1 USD por hora**.

```mermaid
graph TD
    subgraph ClusterDistribuido ["ENFOQUE DISTRIBUIDO (SPARK)"]
        D["Driver Node<br/>(Coordinación de Tareas)"]
        D -->|Red TCP/IP| W1["Worker 1 (JVM)"]
        D -->|Red TCP/IP| W2["Worker 2 (JVM)"]
        W1 <-->|SHUFFLE DE RED (Lento)| W2
    end

    subgraph EnfoqueInProcess ["ENFOQUE EN-PROCESO (DUCKDB)"]
        Proc["Proceso Python / CLI"]
        Proc --> Engine["Motor DuckDB en C++"]
        Engine --> CPU["Hilos de CPU Compartiendo la Misma RAM<br/>(Zero Network Latency / Zero Serialization)"]
    end

    style ClusterDistribuido fill:#ffffff,stroke:#4a5160,stroke-width:1px
    style EnfoqueInProcess fill:#1b2030,color:#ffffff,stroke:#1b2030,stroke-width:2px
    style Engine fill:#8a7a5c,color:#ffffff,stroke:#8a7a5c,stroke-width:2px
```

### El Impuesto de la Distribución (*The Distribution Tax*)
Cuando una consulta se ejecuta sobre un clúster distribuido, el motor debe asumir sobrecostos físicos inevitables:
1. **Serialización y Deserialización:** Convertir objetos de memoria a secuencias de bytes para transmitirlos por la red.
2. **Latencia de Red:** Transferir gigabytes de particiones intermedias entre switches de red durante un *Shuffle*.
3. **Sobrecosto de Coordinación:** El Driver debe compilar el DAG, programar miles de tareas en hilos remotos y gestionar fallos de comunicación.
4. **Tiempo de Arranque (*Cold Start*):** Un clúster de Spark o EMR tarda entre 3 y 8 minutos en aprovisionar sus máquinas virtuales antes de procesar el primer byte.

Si el volumen de datos a procesar cabe en la memoria de un solo nodo (menos de 100–200 GB), **un motor en proceso como DuckDB suele ser entre 3 y 10 veces más rápido que un clúster de Spark**, costando hasta un 95% menos en infraestructura cloud.

---

## 2A.2 Arquitectura Interna de DuckDB

DuckDB alcanza su rendimiento excepcional mediante tres pilares de ingeniería de sistemas:

### 1. Motor Columnar Vectorizado
A diferencia de los motores tradicionales que procesan una tupla a la vez mediante el modelo de iteradores de Volcán (*Volcano Iterator Model*), DuckDB procesa **vectores de datos contiguos en memoria** (típicamente fragmentos de 2.048 valores). Esto maximiza la utilización de la caché L1/L2 del procesador y permite al compilador emitir instrucciones SIMD (*Single Instruction, Multiple Data*).

### 2. Paralelismo Guiado por Porciones (*Morsel-Driven Parallelism*)
DuckDB divide el flujo de datos en pequeñas porciones (*morsels*) de aproximadamente 100.000 tuplas. Los hilos de trabajo de la CPU reclaman dinámicamente estas porciones de una cola compartida sin bloqueos pesados (*lock-free queues*), garantizando una distribución de carga homogénea entre todos los núcleos disponibles.

### 3. Procesamiento Fuera de Núcleo (*Out-of-Core Execution*)
Si una consulta analítica requiere procesar un archivo Parquet de 80 GB en una máquina con solo 16 GB de RAM física, muchos motores colapsan con excepciones de *OutOfMemory*. DuckDB cuenta con un gestor de buffers propio que vuelca bloques intermedios a disco temporalmente (*Disk Spilling Streaming*), permitiendo resolver consultas analíticas masivas que superan la memoria disponible sin interrumpir la ejecución.

---

## 2A.3 Consultas Directas sobre Parquet, CSV y S3

Una de las facultades más poderosas de DuckDB es la capacidad de consultar archivos crudos sin necesidad de crear tablas previas ni importar datos:

```sql
-- Consultar directamente archivos Parquet locales con SQL analítico estándar
SELECT 
    pais,
    categoria_producto,
    COUNT(transaccion_id) AS total_ventas,
    ROUND(SUM(monto_usd), 2) AS facturacion_total,
    ROUND(AVG(monto_usd), 2) AS ticket_promedio
FROM 'data/ventas.parquet'
WHERE estado_pago = 'COMPLETADO'
  AND fecha_transaccion >= '2026-01-01'
GROUP BY pais, categoria_producto
ORDER BY facturacion_total DESC
LIMIT 5;
```

### Integración en Python y Transferencia Zero-Copy hacia Arrow
DuckDB se integra de forma transparente con el ecosistema de Python. Puede leer DataFrames de Pandas o tablas de Apache Arrow y devolver resultados sin copiar datos en memoria (*Zero-Copy Memory Sharing*):

```python
import duckdb
import pyarrow.parquet as pq

# Conexión en memoria volátil (o persistente indicando una ruta de archivo)
conexion = duckdb.connect(database=":memory:")

# 1. Consulta SQL directa sobre múltiples archivos Parquet con patrones glob
consulta_sql = """
    SELECT 
        DATE_TRUNC('month', CAST(fecha_transaccion AS DATE)) AS mes,
        COUNT(DISTINCT cliente_id) AS clientes_unicos,
        SUM(monto_usd) AS volumen_total
    FROM 'data/ventas.parquet'
    GROUP BY 1
    ORDER BY 1 ASC
"""

# 2. Retorno directo como DataFrame de Pandas o Tabla Arrow
df_resultado_arrow = conexion.execute(consulta_sql).arrow()
df_resultado_pandas = conexion.execute(consulta_sql).df()

print(df_resultado_pandas)
```

### Consultas Federadas sobre Amazon S3 / Azure ADLS
Mediante la extensión oficial `httpfs`, DuckDB lee archivos remotos en almacenamiento de objetos en la nube, descargando únicamente los metadatos y las columnas requeridas (*Projection and Predicate Pushdown* sobre HTTP):

```python
import duckdb

con = duckdb.connect()

# Habilitar extensión HTTP/S3
con.execute("INSTALL httpfs; LOAD httpfs;")

# Configurar credenciales cloud seguras mediante el gestor de secretos de DuckDB
# (Lee automáticamente variables de entorno AWS_ACCESS_KEY_ID o roles IAM de la instancia)
con.execute("""
    CREATE SECRET secreto_s3 (
        TYPE S3,
        PROVIDER CREDENTIAL_CHAIN,
        REGION 'us-east-1'
    );
""")

# Consulta remota que descarga únicamente los bytes necesarios de S3
df_s3 = con.execute("""
    SELECT categoria, SUM(monto) AS total
    FROM 's3://mi-bucket-lakehouse/silver/pedidos/*.parquet'
    WHERE anio = 2026
    GROUP BY categoria
""").df()
```

---

## 2A.4 Matriz de Decisión: Pandas vs. Polars vs. DuckDB vs. Apache Spark

Para elegir la herramienta adecuada en un pipeline de datos, utilice la siguiente matriz técnica comparativa:

| Criterio | Pandas | Polars | DuckDB | Apache Spark |
| :--- | :--- | :--- | :--- | :--- |
| **Paradigma Primario** | DataFrames en memoria (Python). | DataFrames vectorizados en Rust. | Base de datos relacional SQL en C++. | Cómputo distribuido horizontal en clúster. |
| **Interfaz de Consulta** | API imperativa/métodos de Python. | API funcional de expresiones encadenadas. | **SQL Estándar ANSI** de primera clase. | PySpark DataFrame API y Spark SQL. |
| **Escalabilidad Máxima** | ~5–10 GB (limitado por RAM del nodo). | ~100–300 GB (Single-Node multihilo). | **~100–500 GB (Single-Node con Out-of-Core)**. | **Petabytes (Multi-Node horizontal)**. |
| **Manejo de Memoria** | Alto consumo (objetos Python, copias). | Excelente (Apache Arrow nativo en Rust). | Excelente (gestor de buffers con volcado a disco). | Tungsten on-heap/off-heap con JVM. |
| **Tiempo de Arranque** | < 1 segundo. | < 1 segundo. | **< 10 milisegundos**. | 3 a 7 minutos en clúster cloud. |
| **Caso de Uso Óptimo** | Análisis exploratorio rápido y ciencia de datos básica. | Transformaciones de alta velocidad en una máquina. | **Pipelines analíticos SQL, serverless, pruebas CI/CD.** | **Cargas masivas que exceden un solo servidor físico.** |

---

## 2A.5 Pipelines Serverless de Bajo Costo con AWS Lambda y DuckDB

Un patrón de arquitectura moderno consiste en ejecutar pipelines analíticos ligeros sobre **AWS Lambda** utilizando la capa precompilada de DuckDB. 

En lugar de mantener un clúster de EMR o Databricks encendido 24/7 (con costos mínimos de cientos de dólares al mes), una función Lambda con 2 GB de memoria ejecuta transformaciones de 5 GB de datos en 8 segundos por **menos de $0.0001 USD por ejecución**:

```python
# Handler de AWS Lambda para transformación de lotes diarios
import os
import duckdb

def lambda_handler(event, context):
    bucket = event.get("bucket", "empresa-datos-crudos")
    fecha_proceso = event.get("fecha", "2026-03-31")
    
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # Ingesta, filtrado, deduplicación y exportación directa de Parquet a Parquet
    query = f"""
        COPY (
            SELECT 
                transaccion_id,
                cliente_id,
                monto_usd,
                fecha_transaccion
            FROM 's3://{bucket}/raw/{fecha_proceso}/*.parquet'
            WHERE monto_usd > 0
            QUALIFY ROW_NUMBER() OVER (PARTITION BY transaccion_id ORDER BY fecha_transaccion DESC) = 1
        ) TO 's3://{bucket}/silver/transacciones_limpias_{fecha_proceso}.parquet' 
        (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    
    con.execute(query)
    return {"status": "SUCCESS", "fecha": fecha_proceso}
```

---

## 2A.6 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Forzar el Uso de Apache Spark para Datasets Menores a 50 Gigabytes**  
Crear un clúster de Spark para procesar una exportación diaria de 5 GB incurre en minutos de aprovisionamiento de máquinas, facturación mínima de nodos y sobrecosto de shuffling.  
*Corrección:* Utilice DuckDB o Polars en un contenedor ligero de Docker o una máquina virtual pequeña; el pipeline tardará segundos en lugar de minutos y costará centavos.

**Anti-patrón 2: Tratar a DuckDB como una Base de Datos Transaccional OLTP Concurrente**  
Intentar utilizar DuckDB como el backend de una aplicación web donde cientos de usuarios realizan transacciones concurrentes simultáneas (`INSERT` y `UPDATE` de milisegundos) provocará bloqueos de base de datos.  
*Corrección:* DuckDB está optimizado para lecturas analíticas masivas y cargas por lotes (OLAP); para concurrencia transaccional multi-usuario utilice PostgreSQL o MySQL.

---

## Preguntas de Revisión y Ejercicios Prácticos
**Ejercicio 2A.1 (Agregación Multi-Archivo y Detección de Anomalías con DuckDB):**  
Se tienen cientos de archivos mensuales particionados en disco: `data/particiones/ventas_*.parquet`.  
Escriba una consulta SQL pura ejecutable en DuckDB que lea todos los archivos Parquet en una sola pasada y obtenga:
1. El total de transacciones por categoría.
2. La facturación acumulada.
3. El porcentaje que representa cada categoría sobre el gran total nacional utilizando una función de ventana `SUM() OVER ()`.

**Solución 2A.1:**
```sql
WITH metricas_categoria AS (
    SELECT 
        categoria_producto,
        COUNT(transaccion_id) AS total_transacciones,
        SUM(monto_usd) AS facturacion_categoria
    FROM 'data/particiones/ventas_*.parquet'
    WHERE estado_pago = 'COMPLETADO'
    GROUP BY categoria_producto
)
SELECT 
    categoria_producto,
    total_transacciones,
    ROUND(facturacion_categoria, 2) AS facturacion_categoria,
    ROUND(
        (facturacion_categoria / SUM(facturacion_categoria) OVER ()) * 100, 
        2
    ) AS porcentaje_facturacion_nacional
FROM metricas_categoria
ORDER BY facturacion_categoria DESC;
```

---

## 2A.7 Resumen y Puntos Clave

* **Desmitificación de la escala:** La mayor parte de las cargas analíticas caben en la RAM de un solo servidor; el cómputo distribuido impone penalizaciones de red y coordinación que deben justificarse formalmente.
* **DuckDB como motor OLAP en proceso:** Ofrece ejecución columnar vectorizada en C++, paralelismo multihilo y capacidad fuera de núcleo (*Out-of-Core*) sin requerir servidores ni clústeres.
* **Acceso directo a formatos abiertos:** Permite ejecutar SQL analítico sobre archivos Parquet locales o en S3/ADLS con poda de columnas y predicados sin duplicación de datos.
* **Complementariedad con Spark:** DuckDB optimiza pipelines locales, serverless (Lambda) y pruebas en CI/CD; Apache Spark se reserva para volúmenes masivos que exceden la capacidad física de una máquina individual.

---

## Lecturas Recomendadas
1. **Raasveldt, M. & Mühleisen, H. (2019).** *DuckDB: An Embeddable Analytical Database.* Proceedings of the 2019 ACM SIGMOD International Conference on Management of Data.
2. **Kleppmann, M. (2017).** *Designing Data-Intensive Applications (Chapter 10: Batch Processing).* O'Reilly Media.
3. **DuckDB Foundation (2024).** *DuckDB Documentation: Performance Tuning, Extensions, and Python API.* DuckDB.org.
