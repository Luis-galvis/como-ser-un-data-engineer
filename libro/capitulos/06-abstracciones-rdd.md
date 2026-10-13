<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[RDD]] -->

# Capítulo 6: Abstracciones de Cómputo: De RDDs a DataFrames [RECOMENDADO]

> *"El RDD es el ladrillo fundamental de la computación distribuida en Spark: una colección inmutable, particionada y tolerante a fallos. Sin embargo, su desconocimiento de los esquemas obligó a la industria a evolucionar hacia la API de DataFrames para desbloquear la optimización automática de Catalyst."*

---

## Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender la definición formal y las propiedades matemáticas de un **RDD** (*Resilient Distributed Dataset*).
* Describir los tres mecanismos fundamentales del RDD: Inmutabilidad, Particionado y Linaje (*Lineage Graph*).
* Explicar por qué los **DataFrames** superaron a los RDDs en analítica empresarial (conocimiento de esquemas y motor Tungsten).
* Identificar los escenarios especializados donde aún es pertinente descender a nivel de RDDs en producción.

---

> **Definición 6.1 (Cajas Opacas vs. Estanterías Etiquetadas)**
> Un RDD es como almacenar tus pertenencias en cajas de cartón completamente selladas y sin etiquetas: el transportista (Spark) sabe que hay 1.000 cajas y puede mover 100 en cada camión, pero no tiene ni idea de qué hay adentro. Si le pides buscar la edad de un cliente, tiene que abrir cada caja, deserializar el contenido y revisar a ciegas.
> 
> Un DataFrame es como colocar los datos en una estantería transparente de vidrio con divisiones milimétricas y etiquetas de tipos (un esquema relacional). Spark puede ver desde lejos que la columna 'edad' está en el tercer compartimento y leer directamente los enteros sin abrir el resto de los datos.

---

## 6.1 Fundamentos y Anatomía del RDD

Un **RDD** (*Resilient Distributed Dataset*) es la abstracción de datos fundamental y más básica en Apache Spark. Se define como una colección **inmutable, particionada y tolerante a fallos** de elementos que pueden ser procesados en paralelo en un clúster de computadoras.

Sus tres pilares fundamentales son:

1. **Resilient (Resiliente / Tolerante a fallos):** Si un nodo que contiene una partición de datos falla físicamente a mitad de un cómputo, Spark no necesita reiniciar todo el proceso desde cero. Utilizando el grafo de dependencias o linaje (*Lineage*), el motor recalcula exclusivamente la partición perdida en otro nodo.
2. **Distributed (Distribuido):** Los datos no residen en una sola máquina; están divididos en múltiples **particiones lógicas** dispersas en la memoria RAM y discos de los Worker Nodes del clúster.
3. **Dataset (Colección de datos):** Representa una colección de objetos fuertemente tipados (en Scala/Java) o estructuras genéricas en Python.

### Inmutabilidad y Linaje

Los RDDs son **estrictamente inmutables**: una vez creado un RDD, su contenido no puede modificarse en caliente. Para alterar datos, se aplica una transformación que genera un **nuevo RDD**. Esta propiedad garantiza:
* Ausencia de condiciones de carrera (*race conditions*) en ejecuciones paralelas multihilo.
* Facilidad de serialización y replicación en memoria.
* Registro exacto del grafo de derivación para tolerancia a fallos.

```mermaid
graph LR
    RDD0["RDD 0: Archivo Crudo
[Partición 1] [Partición 2]"] -->|Transformación map()| RDD1["RDD 1: Tuplas Parsed
[Partición 1] [Partición 2]"]
    RDD1 -->|Transformación filter()| RDD2["RDD 2: Filtrado Activo
[Partición 1] [Partición 2]"]
    RDD2 -->|Acción reduceByKey()| RDD3["RDD 3: Agregación Final
[Partición Resumen]"]
    style RDD0 fill:#fbf9f4,stroke:#4a5160,stroke-width:1px
    style RDD1 fill:#fbf9f4,stroke:#4a5160,stroke-width:1px
    style RDD2 fill:#fbf9f4,stroke:#4a5160,stroke-width:1px
    style RDD3 fill:#1b2030,color:#ffffff,stroke:#1b2030,stroke-width:2px
```

---


## 6.2 Profundización Técnica: ¿Por qué los DataFrames Reemplazaron a los RDDs?


A pesar de su elegancia matemática, los RDDs presentan limitaciones severas para analítica a gran escala:

1. **Caja Negra de Tipos:** Spark no comprende la estructura interna de los objetos dentro de un RDD. No sabe si contiene un string, una clase personalizada o un float; por ende, no puede optimizar expresiones relacionales.

2. **Sobrecarga de Serialización en Python:** En PySpark, cada elemento de un RDD debe serializarse con `pickle` de Python a la JVM y viceversa, degradando la velocidad de CPU dramáticamente.

3. **DataFrames al rescate:** Un **DataFrame** es conceptualmente un RDD de objetos `Row` acompañados de un **Esquema Estricto** (nombres y tipos de columnas). Conocer el esquema permite al motor **Catalyst** optimizar consultas y al motor **Tungsten** almacenar datos en formato binario compacto (*UnsafeRow*), reduciendo drásticamente la sobrecarga de objetos Java al gestionar memoria estructurada directamente en el heap (y opcionalmente fuera del heap con `spark.memory.offHeap.enabled`).

---

> **Consideraciones de Producción 6.1: Operaciones con RDDs vs. Conversión a DataFrames**
> Aunque en la inmensa mayoría de los casos analíticos utilizará DataFrames, descender a la API de RDDs es útil para procesar datos desestructurados (como archivos de texto crudos o logs de red sin delimitadores).

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("Capitulo06-RDDs-a-DataFrames") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# 1. Crear un RDD a partir de una lista cruda en memoria
rdd_crudo = sc.parallelize([
    ("prod_101", 15),
    ("prod_102", 40),
    ("prod_101", 35),
    ("prod_103", 20)
])

# 2. Transformaciones funcionales de bajo nivel sobre RDD
# reduceByKey suma los valores asociados a cada clave en paralelo
rdd_totales = rdd_crudo.reduceByKey(lambda x, y: x + y)
print("Resultado en RDD:", rdd_totales.collect())

# 3. Transición profesional: Convertir RDD a DataFrame con esquema estricto
esquema = StructType([
    StructField("producto_id", StringType(), False),
    StructField("cantidad_total", IntegerType(), False)
])

df_optimizado = spark.createDataFrame(rdd_totales, schema=esquema)
df_optimizado.show()
```

---

## 6.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Usar RDDs para Transformaciones Tabulares**  
Escribir transformaciones complejas con funciones lambda de Python sobre RDDs (ej. `rdd.map(lambda x: ...).filter(lambda x: ...)`) desactiva por completo el optimizador Catalyst y genera una penalización severa de rendimiento frente a la API de DataFrames nativa optimizada por Tungsten.

**Anti-patrón 2: Pérdida del linaje por sobreescritura incorrecta**  
Intentar mutar variables en bucles esperando que Spark actualice un RDD existente ignora el principio de inmutabilidad y puede generar estados inconsistentes en la lógica de negocio.

---

## Preguntas de Revisión y Ejercicios Prácticos
**Ejercicio 6.1 (Linaje vs. Replicación):**  
Si un nodo trabajador con 4 particiones de un RDD se destruye a mitad de un job, describe exactamente los pasos que toma Spark para recuperarse sin reiniciar la aplicación completa.

**Solución 6.1:**  
1. El Driver detecta la desconexión del ejecutor mediante el mecanismo de latidos (*heartbeats*).
2. El Driver consulta el **grafo de linaje** registrado en memoria para identificar exactamente qué transformaciones originaron esas 4 particiones específicas.
3. El Driver re-programa esas 4 tareas en otros ejecutores vivos disponibles.
4. Los nuevos ejecutores leen los datos originales de la fuente y re-ejecutan únicamente la cadena de transformaciones necesaria para regenerar esas 4 particiones, continuando el flujo sin interrupción para el usuario.

---

## 6.4 Resumen y Puntos Clave

* **El bloque constructivo:** RDD es la unidad atómica de tolerancia a fallos e inmutabilidad en Spark.
* **El salto a DataFrames:** Proporcionan esquemas estructurados, permitiendo que Catalyst y Tungsten optimicen el uso de CPU y gestionen memoria compacta binaria (on-heap por defecto y off-heap opcional).
* **Regla de oro:** Utilice DataFrames para todo procesamiento relacional y analítico; reserve RDDs únicamente para manipulación de streams de texto no estructurado o librerías legacy.

---

## Lecturas Recomendadas
1. **Zaharia, M. et al. (2012).** *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing.* NSDI '12.
2. **Chambers, B. & Zaharia, M. (2018).** *Spark: The Definitive Guide.* O'Reilly Media. (Capítulo 12: Resilient Distributed Datasets).
