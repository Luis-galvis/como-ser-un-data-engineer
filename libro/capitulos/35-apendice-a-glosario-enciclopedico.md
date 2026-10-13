<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Glossary]], [[Data Engineering Terms]], [[Acronyms]] -->

# Capítulo 35: Apéndice A — Glosario Enciclopédico de Términos y Siglas de Ingeniería de Datos (A-Z) [RECOMENDADO]

> *"La precisión en el lenguaje es el reflejo de la claridad en el diseño de sistemas. En la ingeniería de datos contemporánea, la ambigüedad conceptual entre términos aparentemente similares conduce a decisiones de arquitectura costosas y fallas sutiles en producción. Este glosario enciclopédico reúne y formaliza más de ochenta conceptos, teoremas, siglas y patrones indispensables para la práctica profesional y la literatura académica de la disciplina."*

---

### A

* **ACID (Atomicity, Consistency, Isolation, Durability):** Conjunto de propiedades que garantizan que las transacciones en una base de datos o formato de tabla abierta (como Delta Lake o Apache Iceberg) se procesen de manera confiable. La *Atomicidad* asegura que la transacción se ejecute por completo o falle sin efectos parciales; la *Consistencia* valida las reglas de integridad; el *Aislamiento* previene anomalías concurrentes; y la *Durabilidad* garantiza que los cambios confirmados persistan ante fallos del sistema.
* **Adaptive Query Execution (AQE):** Característica del optimizador Catalyst de Apache Spark (introducida en Spark 3.0) que re-optimiza dinámicamente los planes de ejecución físicos durante el tiempo de ejecución basándose en estadísticas reales recolectadas tras completar etapas intermedias de cómputo (*Shuffle Stages*), ajustando el número de particiones, convirtiendo *Sort Merge Joins* en *Broadcast Joins* y mitigando sesgos de datos (*Data Skew*).
* **Apache Arrow:** Formato de memoria columnar estándar y abierto para datos analíticos en memoria. Permite el intercambio de datos entre sistemas heterogéneos (como Spark, Python, Pandas, DuckDB y bases de datos) con costo computacional cero de serialización (*Zero-Copy Deserialization*).
* **Apache Avro:** Formato de serialización de datos orientado a filas, binario y compacto, que almacena el esquema en formato JSON junto a los datos o lo consulta en un registro de esquemas (*Schema Registry*). Es el estándar por excelencia para mensajería y streaming en Apache Kafka.
* **Auto Loader (`cloudFiles`):** Mecanismo de ingesta incremental optimizado en Databricks que detecta y procesa millones de archivos nuevos depositados en almacenamiento de objetos en la nube (S3, ADLS, GCS) de forma escalable mediante notificaciones de eventos asíncronas y seguimiento de esquemas evolutivos.

---

### B

* **B-Tree (Árbol B):** Estructura de datos de árbol balanceado auto-organizado ampliamente utilizada en motores de bases de datos relacionales OLTP (como PostgreSQL, MySQL y Oracle) para mantener datos ordenados y permitir búsquedas, inserciones y eliminaciones en tiempo logarítmico $\mathcal{O}(\log n)$, minimizando los accesos a disco por bloque.
* **Backpressure (Contrapresión):** Mecanismo de control en sistemas reactivos y de streaming (como Apache Flink, Spark Streaming y Reactive Streams) mediante el cual un consumidor o componente aguas abajo señaliza a los productores aguas arriba que ralenticen la emisión de mensajes cuando su capacidad de procesamiento se encuentra saturada, evitando desbordamientos de memoria (*OOM*).
* **Bloom Filter (Filtro de Bloom):** Estructura de datos probabilística eficiente en espacio utilizada para verificar si un elemento pertenece a un conjunto. Admite falsos positivos con una probabilidad controlada, pero nunca falsos negativos. Se emplea intensivamente en Parquet, Delta Lake y motores analíticos para omitir la lectura de archivos completos (*Data Skipping*).
* **Broadcast Hash Join (BHJ):** Algoritmo distribuido de cruce en el cual la tabla de menor tamaño se transmite íntegramente a todos los nodos ejecutores del clúster, permitiendo que cada ejecutor cruce su partición local sin necesidad de realizar una costosa redistribución de red (*Shuffle*).

---

### C

* **Capacitor:** Formato de archivo columnar interno de Google utilizado en BigQuery sobre Colossus. Precursor conceptual de Apache Parquet, diseñado para codificar de forma eficiente estructuras de datos anidadas y repetidas mediante técnicas avanzadas de compresión (RLE, diccionarios y mapas de bits).
* **Change Data Capture (CDC):** Patrón de arquitectura que identifica, captura y replica en tiempo real las modificaciones (inserciones, actualizaciones y eliminaciones) efectuadas sobre una base de datos relacional transaccional (leyendo directamente su *Write-Ahead Log* o *binlog*) hacia sistemas analíticos aguas abajo sin sobrecargar la base de datos de origen.
* **Chandy-Lamport (Algoritmo de):** Algoritmo distribuido que permite obtener una instantánea consistente global (*Global Distributed Snapshot*) del estado de un sistema distribuido asíncrono sin detener su ejecución. Es la base teórica del mecanismo de puntos de control (*Checkpoints*) con estado exactamente una vez de Apache Flink.
* **Clustering Key (Clave de Agrupamiento):** Conjunto de columnas definido sobre una tabla (en Snowflake o BigQuery) que determina el orden físico de colocación de los registros en las micro-particiones o bloques de almacenamiento, maximizando la poda de bloques en consultas analíticas.
* **Colossus:** El sistema de archivos distribuido de última generación de Google, sucesor de Google File System (GFS), que actúa como la base de almacenamiento masivo y desacoplado para BigQuery y Cloud Storage.
* **Common Table Expression (CTE):** Conjunto de resultados temporal con nombre definido dentro de la ejecución de una sola sentencia SQL mediante la cláusula `WITH`. Permite descomponer consultas complejas en pasos lógicos modulares y ejecutar recursión jerárquica.

---

### D

* **Data Contract (Contrato de Datos):** Acuerdo formal y verificable entre los productores de datos y los equipos consumidores que define el esquema de los datos, las garantías semánticas, los acuerdos de nivel de servicio (SLA) de frescura y las expectativas de calidad, frecuentemente expresado en especificaciones declarativas legibles por máquina (como ODCS).
* **Data Lakehouse:** Arquitectura de datos moderna que combina la flexibilidad, bajo costo y soporte de formatos abiertos del *Data Lake* con las garantías de gestión de transacciones ACID, gobernanza, seguridad y alto rendimiento analítico del *Data Warehouse*.
* **Data Mesh (Malla de Datos):** Paradigma sociotécnico y arquitectónico descentralizado propuesto por Zhamak Dehghani que organiza la analítica de datos alrededor de dominios de negocio orientados al producto (*Data as a Product*), gobernanza computacional federada e infraestructura de autoservicio.
* **Dead-Letter Queue / Topic (DLQ / DLT):** Cola o tema de mensajería secundario al cual se desvían automáticamente los mensajes que no pudieron ser procesados o deserializados exitosamente tras múltiples reintentos, evitando el bloqueo del flujo principal y permitiendo el análisis forense posterior.
* **Deletion Vector (Vector de Eliminación):** Técnica de optimización de almacenamiento en formatos de tabla abierta (Delta Lake y Apache Iceberg) que registra los registros eliminados o modificados mediante un mapa de bits (*Bitmap*) referenciado en metadatos, evitando reescribir inmediatamente archivos Parquet completos en operaciones DML.
* **Delta Live Tables (DLT):** Marco declarativo en Databricks para construir canalizaciones ETL confiables y mantenibles en SQL o Python, gestionando automáticamente la orquestación de grafos DAG, monitoreo de métricas y validación de expectativas de calidad (*Expectations*).
* **Dynamic Table:** Objeto declarativo en Snowflake que actualiza automáticamente su contenido de forma incremental a partir de tablas base según un período de frescura objetivo configurado (*Target Lag*), abstrayendo la creación manual de *Streams* y *Tasks*.
* **Dynamic Task Mapping:** Capacidad en Apache Airflow (a partir de la versión 2.3) que permite generar instancias de tareas en tiempo de ejecución de manera dinámica basándose en el volumen de datos o salidas producidas por tareas previas mediante `.expand()`.

---

### E

* **Elastic MapReduce (Amazon EMR):** Servicio gestionado en AWS para ejecutar marcos de computación distribuida de código abierto como Apache Spark, Hadoop, Hive, Flink y Trino sobre instancias elásticas de EC2, contenedores de EKS o entornos serverless.
* **Envelope Encryption (Cifrado de Sobre):** Práctica de seguridad criptográfica en la cual los datos se cifran con una clave de cifrado de datos local (DEK), y dicha DEK se cifra posteriormente con una clave maestra protegida en un servicio de gestión de claves de hardware (*KMS*).
* **Exactly-Once Semantics (EOS):** Garantía en sistemas de procesamiento de datos en tiempo real mediante la cual cada evento produce exactamente el mismo efecto sobre el estado interno y los sistemas de destino que si se hubiera procesado una única vez, neutralizando fallas de red y reintentos.

---

### F

* **Factless Fact Table (Tabla de Hechos Sin Hechos):** Tabla de hechos en modelado dimensional de Kimball que no contiene medidas cuantitativas numéricas, sino exclusivamente claves foráneas. Se utiliza para modelar la ocurrencia de eventos (como asistencia a un curso) o condiciones de cobertura (como productos en promoción).
* **Feature Store:** Repositorio centralizado que gestiona el ciclo de vida, almacenamiento y servicio de características de aprendizaje automático tanto en modo por lotes (*Offline*) para entrenamiento como en modo de baja latencia (*Online*) para inferencia, garantizando consistencia temporal (*Point-in-Time Correctness*).
* **FinOps (Financial Operations):** Disciplina cultural y operativa que promueve la responsabilidad compartida y la optimización continua del gasto financiero en la nube entre los equipos de ingeniería, finanzas y negocio.

---

### H

* **HyperLogLog (HLL):** Algoritmo probabilístico que estima la cardinalidad (*count distinct*) en conjuntos masivos con memoria constante mínima. El error estándar depende del número de registros $m = 2^b$ según $\sigma \approx 1.04 / \sqrt{m}$. Por ejemplo, con $b=10$ ($m=1024$) ocupa apenas $\approx 1.5$ KB, mientras que la implementación estándar de Redis y motores analíticos ($b=14, m=16.384$) consume **12 KB de memoria con un error estándar inferior al 0.81%**, evitando retener miles de millones de identificadores en memoria.

---

### I

* **Idempotencia:** Propiedad de una operación que produce exactamente el mismo resultado final sin importar si se ejecuta una o múltiples veces consecutivas con los mismos parámetros de entrada. Es el pilar fundamental para la tolerancia a fallos en pipelines de datos.
* **In-Process OLAP:** Motor analítico columnar vectorizado que se ejecuta embebido dentro del mismo proceso del programa cliente sin requerir la gestión de demonios de servidor independientes ni transferencias por red (ej. DuckDB).

---

### K

* **Kahn (Algoritmo de):** Algoritmo de ordenamiento topológico para grafos acíclicos dirigidos (DAGs) que opera calculando iterativamente los grados de entrada (*In-Degrees*) de los vértices y eliminando aquellos con grado cero, garantizando el orden de ejecución correcto y detectando ciclos dependientes.
* **KubernetesExecutor:** Mecanismo de ejecución en Apache Airflow que crea dinámicamente un Pod de Kubernetes aislado y efímero para cada tarea individual, asegurando aislamiento absoluto de dependencias y escalabilidad de recursos de cero a miles de tareas concurrentes.

---

### L

* **Lazy Evaluation (Evaluación Perezosa):** Estrategia de evaluación computacional en Apache Spark y motores similares mediante la cual las transformaciones no se ejecutan de inmediato al ser declaradas, sino que se acumulan en un grafo acíclico dirigido (DAG) que es evaluado y optimizado globalmente solo cuando una acción requiere materializar un resultado.
* **Liquid Clustering:** Técnica avanzada de agrupamiento dinámico en Delta Lake que reemplaza el particionamiento rígido basado en directorios de Hive y la complejidad del Z-Ordering por una reestructuración multidimensional flexible y adaptable a los cambios en los patrones de consulta.
* **Log-Structured Merge-tree (LSM-Tree):** Estructura de datos de almacenamiento persistente optimizada para escrituras masivas de alto rendimiento. Acumula las escrituras en memoria (*MemTable*) con un registro de confirmación previo (*WAL*) y descarga periódicamente archivos inmutables ordenados (*SSTables*) al disco, consolidándolos en segundo plano mediante compactaciones.

---

### M

* **Medallion Architecture (Arquitectura Medallón):** Patrón de diseño lógico de Lakehouse que estructura los datos en tres niveles progresivos de refinamiento y calidad: **Bronze** (datos crudos e inmutables del origen), **Silver** (datos limpios, tipados, deduplicados y conformados) y **Gold** (modelos dimensionales o agregados de negocio de consumo analítico).
* **Micro-partición:** Unidad fundamental de almacenamiento físico en Snowflake. Archivo inmutable, columnar y comprimido de 50 a 500 MB gestionado automáticamente por la capa de Cloud Services con estadísticas de metadatos integradas.
* **MVCC (Multi-Version Concurrency Control):** Método de control de concurrencia en motores de bases de datos (como PostgreSQL y formatos Lakehouse) que permite que múltiples transacciones lean y escriban simultáneamente sin bloquearse, manteniendo internamente múltiples versiones lógicas de los registros.

---

### O

* **OneLake:** El lago de datos lógico unificado y SaaS provisto por Microsoft Fabric. Centraliza todo el almacenamiento analítico empresarial bajo una única cuenta global utilizando el formato Delta Lake abierto y ofreciendo acceso sin copia mediante accesos directos (*Shortcuts*).
* **Out-of-Core Processing:** Capacidad de un motor de ejecución (como DuckDB) para procesar volúmenes de datos significativamente más grandes que la memoria RAM física disponible en una máquina mediante particionamiento eficiente y paginación controlada a disco.

---

### P

* **PACELC (Teorema):** Extensión formal del teorema CAP propuesta por Daniel Abadi. Establece que en un sistema distribuido con partición de red (**P**), se debe elegir entre Disponibilidad (**A**) y Consistencia (**C**); pero incluso en ausencia de particiones (**E**lse), se debe elegir entre Latencia (**L**) y Consistencia (**C**).
* **Photon Engine:** Motor de ejecución vectorizado de alto rendimiento desarrollado en C++ nativo por Databricks que reemplaza las etapas de ejecución JVM de Apache Spark, aprovechando instrucciones SIMD de hardware y acelerando sustancialmente las cargas SQL y de DataFrame.
* **Polaris Catalog (Apache Polaris):** Catálogo de metadatos de código abierto para Apache Iceberg basado en la especificación estándar REST, liberado por Snowflake para permitir interoperabilidad multi-motor entre Snowflake, Spark, Flink y Trino.
* **Pydantic:** Biblioteca de análisis y validación de datos en Python que utiliza anotaciones de tipo estándar (*Type Hints*) para garantizar esquemas estrictos y coerción de tipos en tiempo de ejecución.

---

### R

* **RAG (Retrieval-Augmented Generation):** Patrón de arquitectura que optimiza la generación de modelos de lenguaje grande (LLMs) recuperando documentos o fragmentos relevantes desde un almacén de datos estructurado o base de datos vectorial antes de emitir la respuesta, fundamentando el modelo en hechos verificables.
* **RocksDB:** Motor embebido de almacenamiento persistente clave-valor de alto rendimiento basado en LSM-Trees desarrollado por Meta. Utilizado como el backend de gestión de estado distribuido estándar en Apache Flink y Apache Spark Structured Streaming.

---

### S

* **Salting (Salado de Claves):** Técnica de ingeniería distribuida que consiste en concatenar un sufijo pseudoaleatorio o determinista a las claves con alta concentración de registros para distribuir uniformemente los datos a través de múltiples particiones de red, mitigando el sesgo de datos (*Data Skew*).
* **Slowly Changing Dimensions (SCD):** Patrón de diseño de modelado dimensional para gestionar la evolución temporal de los atributos descriptivos en tablas maestras. Destacan el **Tipo 1** (sobreescritura del valor actual sin retención histórica) y el **Tipo 2** (creación de una nueva fila histórica con marcas de tiempo `fecha_inicio`, `fecha_fin` y un indicador `es_actual`).
* **Snowpark:** Biblioteca y entorno de ejecución de Snowflake que permite construir aplicaciones analíticas complejas en Python, Scala y Java ejecutando transformaciones vectorizadas perezosas en los nodos del Virtual Warehouse.
* **SSTable (Sorted String Table):** Archivo inmutable en disco que contiene pares clave-valor ordenados secuencialmente, complementado con un bloque de índice y filtros de Bloom para búsquedas rápidas en arquitecturas LSM-Tree.

---

### T

* **Testcontainers:** Marco de pruebas de software que permite instanciar contenedores Docker reales (como PostgreSQL, Kafka o LocalStack) de forma efímera y programática dentro de las suites de prueba unitarias e integrales.
* **Three-Valued Logic (3VL):** Sistema formal de lógica utilizado en SQL donde las expresiones booleanas pueden evaluar a tres estados: `TRUE`, `FALSE` o `UNKNOWN` (resultante de operaciones con valores `NULL`), gobernando el comportamiento de filtros `WHERE` y condiciones de cruce.
* **Time Travel:** Capacidad de consultar el estado exacto de una tabla o conjunto de datos en un punto específico del pasado basándose en marcas de tiempo o identificadores de confirmación transaccional.
* **Tombstone (Lápida de Borrado):** Registro especial emitido en un flujo de streaming o sistema distribuido (como Kafka o Cassandra) con una clave de entidad y valor `null` que señaliza formalmente la eliminación del registro para que los compactadores y consumidores purguen el estado.

---

### U

* **Unity Catalog:** Capa universal de gobernanza y catálogo unificado en Databricks que proporciona linaje de datos de extremo a extremo, control de acceso unificado (ANSI SQL GRANTs), auditoría centralizada y descubrimiento de datos sobre tablas, volúmenes, modelos y notebooks.

---

### W

* **Watermark (Marca de Agua):** Métrica temporal móvil en motores de streaming que indica el progreso del tiempo de evento a través del flujo. Define formalmente el límite temporal hasta el cual el sistema asume que todos los eventos han sido recibidos, permitiendo cerrar ventanas y desechar o aislar datos tardíos (*Late Data*).
* **Write-Ahead Log (WAL):** Archivo de registro secuencial sólo de anexado (*Append-Only*) en el que se graban sincrónicamente los cambios en disco antes de aplicarlos a las páginas de datos en memoria o tablas de almacenamiento, garantizando durabilidad y recuperación ante caídas del sistema.

---

### Z

* **Z-Ordering (Curva de Morton):** Algoritmo que mapea datos multidimensionales en un espacio unidimensional conservando la localidad espacial, utilizado en Delta Lake y Apache Iceberg para ordenar registros contiguamente por múltiples columnas y acelerar el salto de datos en consultas que filtran por subconjuntos de dichas columnas.
