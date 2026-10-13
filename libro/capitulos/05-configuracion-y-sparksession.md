<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[PySpark]], [[spark session]], [[conf spark]], [[SparkConf]] -->

# Capítulo 5: Entorno de Configuración y SparkSession [NÚCLEO EMPLEABILIDAD]

> *"SparkSession es el punto de entrada unificado al universo de Spark: permite crear y manipular DataFrames, ejecutar consultas SQL, cargar archivos Parquet o JSON y definir los parámetros de paralelismo y memoria que gobernarán el clúster."*

---

## Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender la evolución desde los contextos dispersos de Spark 1.x (`SparkContext`, `SQLContext`, `HiveContext`) hacia el punto de entrada unificado `SparkSession` de Spark 2.x y 3.x.
* Dominar el patrón de diseño Builder para inicializar y parametrizar aplicaciones PySpark de forma segura.
* Utilizar `SparkConf` para inyectar configuraciones dinámicas desde archivos JSON o variables de entorno.
* Configurar parámetros críticos de memoria, serialización y paralelismo antes de disparar cómputo en el clúster.

---

> **Definición 5.1 (La Llave Maestra del Edificio Inteligente)**
> En las versiones tempranas de Spark, operar el sistema era como llevar un manojo de cinco llaves metálicas pesadas: necesitabas la llave del ascensor (SparkContext), la llave de la sala de conferencias (SQLContext) y la llave del archivo histórico (HiveContext). Si olvidabas una, no podías trabajar.
> 
> SparkSession es una tarjeta digital de acceso unificado: una sola llave inteligente que te abre todas las puertas del edificio, conecta con el catálogo de datos, ejecuta SQL y lee cualquier formato de archivo con un único apretón de manos.

---

## 5.1 El Punto de Entrada Unificado: SparkSession

En Apache Spark, `SparkSession` actúa como el "corazón" programático de la aplicación. Introducida en Spark 2.0 y consolidada como el estándar indiscutible de la industria, unifica todas las capacidades del motor bajo una misma interfaz fluida.

Con una instancia de `SparkSession` puedes:
1. Crear y manipular DataFrames a partir de fuentes externas o colecciones nativas de Python.
2. Ejecutar código SQL directamente sobre los datos mediante `spark.sql()`.
3. Leer archivos en formatos columnares o estructurados (`parquet`, `json`, `csv`, `orc`, `delta`, `avro`).
4. Acceder y alterar dinámicamente configuraciones internas del runtime mediante `spark.conf`.

### El Patrón Builder

Debido a que una aplicación distribuida requiere configurar múltiples parámetros antes de que el motor inicie, SparkSession se construye utilizando el **patrón Builder**:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MiPipelineDeDatos") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

El método `.getOrCreate()` garantiza que si ya existe una sesión activa en la JVM (por ejemplo al trabajar en Databricks, Jupyter o EMR), Spark reutiliza la sesión existente en lugar de lanzar un segundo contexto conflictivo.

---


## 5.2 Profundización Técnica: SparkConf y el Puente Py4J


¿Cómo se comunica Python con un motor escrito en Scala y Java? PySpark utiliza una pasarela llamada **Py4J**:

1. Tu código Python invoca métodos en el Driver.

2. Py4J traduce cada llamada a un socket TCP local comunicándose con la JVM subyacente.

3. `SparkConf` es la clase que define los parámetros inmutables del clúster antes de que arranque la JVM. Parámetros como el tamaño del Driver (`spark.driver.memory`) o el master URL (`spark.master`) **deben definirse en SparkConf antes de inicializar la sesión**; una vez encendida la JVM, no pueden modificarse en caliente.


---

> **Consideraciones de Producción 5.1: Carga de Configuraciones desde JSON**
> En proyectos de ingeniería de producción nunca se deben quemar configuraciones en duro en el código. El patrón profesional consiste en separar la configuración en un archivo externo (JSON o YAML) e inyectarla dinámicamente a través de SparkConf.

```python
import json
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession

```python
# 1. Configuración de parámetros en SparkConf
conf = SparkConf()
conf.set("spark.app.name", "PipelineIngestaProduccion")
conf.set("spark.master", "local[*]")
conf.set("spark.sql.shuffle.partitions", "16")
conf.set("spark.sql.adaptive.enabled", "true")

# Nota técnica: KryoSerializer optimiza la serialización de RDDs y clases Java/Scala personalizadas;
# en la API de DataFrames, Spark utiliza internamente los codificadores binarios de Tungsten.
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# 2. Inicializar SparkSession aplicando el objeto SparkConf
spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()
```

---

## 5.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Intentar cambiar memoria en tiempo de ejecución**  
Escribir `spark.conf.set("spark.executor.memory", "8g")` después de llamar a `getOrCreate()` no produce ningún efecto. La memoria de los ejecutores es asignada por el Cluster Manager en el instante del arranque del proceso JVM. Toda asignación de hardware debe pasarse vía `spark-submit --executor-memory 8g` o en el `SparkConf` inicial.

**Anti-patrón 2: No cerrar la sesión al finalizar scripts por lotes**  
En pipelines orquestados por Airflow o scripts standalone, olvidar llamar a `spark.stop()` mantiene la aplicación activa en YARN o Kubernetes, consumiendo recursos y bloqueando cuotas de cómputo del clúster innecesariamente.

---

## Preguntas de Revisión y Ejercicios Prácticos
**Ejercicio 5.1 (Serialización Kryo frente a Codificadores de Tungsten):**  
¿Por qué configurar `spark.serializer = org.apache.spark.serializer.KryoSerializer` es crítico cuando se opera con RDDs o tipos personalizados, pero tiene un impacto menor en pipelines que utilizan exclusivamente la API DataFrame?

**Solución 5.1:**  
* **En la API DataFrame:** Spark emplea el motor **Tungsten**, el cual almacena y serializa los datos en formato binario compacto fuera del modelo de objetos tradicional de Java mediante codificadores (*Encoders*) y representaciones en memoria `UnsafeRow`. Por ello, las filas de un DataFrame no se serializan con Java ni con Kryo durante las operaciones analíticas estándar.
* **En RDDs y tipos no gestionados:** Cuando se manipulan colecciones de objetos Java/Scala arbitrarios o RDDs con `groupByKey` o `reduceByKey`, el serializador predeterminado de Java introduce una sobrecarga masiva de metadatos de clases. En este escenario, **Kryo** es esencial: serializa objetos binariamente de forma compacta (hasta 10 veces más rápido y con una reducción de tamaño del 50% al 75% frente a Java), descongestionando la memoria y la red durante la fase de shuffle.

---

## 5.4 Resumen y Puntos Clave

* **Punto de entrada único:** `SparkSession` unifica APIs estructuradas, ejecución SQL y conectividad de almacenamiento.
* **Inmutabilidad de arranque:** Configuraciones críticas de memoria y master se definen al inicio mediante `SparkConf` o flags de `spark-submit`.
* **Cierre disciplinado:** Toda aplicación por lotes debe finalizar con `spark.stop()` para liberar los recursos asignados por el clúster.

---

## Lecturas Recomendadas
1. **Documentación Oficial de Apache Spark:** *Spark Configuration Guide* (`https://spark.apache.org/docs/latest/configuration.html`).
2. **Damji, J. et al. (2020).** *Learning Spark (2nd Edition).* O'Reilly Media. (Capítulo 2: Downloading Apache Spark and Getting Started).
