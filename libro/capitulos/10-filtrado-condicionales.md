<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Filter]], [[Filter y Condicionales en DataFrames]] -->

# Capítulo 10: Filtrado y Expresiones Condicionales [NÚCLEO EMPLEABILIDAD]

> *"Filtrar temprano en el flujo de trabajo es la optimización número uno en computación distribuida: cada registro descartado antes de un shuffle o un join representa megabytes de datos que no saturarán las tarjetas de red ni la memoria RAM de los nodos ejecutores."*

---

## Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Dominar la sintaxis idiomática de `filter()` y `where()` mediante objetos de columna (`col`).
* Aplicar operadores lógicos a nivel de bits (`&`, `|`, `~`) y estructurar el encapsulado estricto con paréntesis.
* Construir lógica condicional de negocio avanzada con `when().otherwise()`, `isin()` y verificación de nulos.
* Explicar el funcionamiento interno de **Predicate Pushdown** en el optimizador Catalyst sobre capas de almacenamiento Parquet y Delta Lake.

---

> **Definición 10.1 (El Colador Inteligente en la Minería de Oro)**
> Imagina que estás buscando oro en un río caudaloso.
> 
> No tiene ningún sentido transportar toneladas de arena, piedras y lodo río abajo en camiones hasta la refinería para recién allí separar las pepitas de oro. Lo inteligente y económico es colocar una criba o colador de alta precisión justo en la orilla del río: descartas el 95% del material estéril de inmediato y solo transportas las partículas valiosas.
> 
> En PySpark, filtrar es ese colador en la orilla: previene que millones de filas inútiles viajen por la red o consuman memoria en operaciones pesadas de agregación y cruce.

---

## 10.1 Fundamentos y Sintaxis Idiomática

En el ecosistema de PySpark, `filter` es una transformación de estrecha dependencia (*Narrow Transformation*) que evalúa registros basándose en una condición lógica booleana. Dado que los DataFrames son **inmutables**, aplicar un filtro no modifica la colección original, sino que devuelve una nueva abstracción de DataFrame con los registros supervivientes.

En la API disponemos de dos métodos formalmente equivalentes: `filter()` y `where()`. Ambos compilan al mismo operador lógico relacional:

```python
from pyspark.sql.functions import col

# Ambas sentencias son 100% intercambiables
df_activos = df.filter(col("estado_cuenta") == "ACTIVO")
df_activos = df.where(col("estado_cuenta") == "ACTIVO")
```

### Operadores a Nivel de Bits y Precedencia

A diferencia de Python tradicional donde se utilizan las palabras reservadas `and`, `or` y `not`, en PySpark **es obligatorio utilizar los operadores a nivel de bits (`&`, `|`, `~`) y envolver cada condición individual entre paréntesis**:

```python
# CORRECTO: Paréntesis obligatorios en cada condición individual
df_colombia_vip = df.filter(
    (col("edad") >= 18) & 
    (col("pais") == "Colombia") & 
    (col("saldo") > 1000000)
)

# Negación lógica con la tilde de inversión ~
df_cuentas_limpias = df.filter(~col("cuenta_bloqueada"))
```

Si omites los paréntesis, Python evalúa el operador `&` antes que las comparaciones (`>`, `==`) y PySpark lanza `ValueError: Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions`. El mismo `ValueError` aparece si usas `and`, `or` o `not` con columnas.

---


## 10.2 Profundización Técnica: La Magia de Predicate Pushdown


Cuando aplicas un `filter()` sobre un DataFrame que lee desde archivos Parquet, Delta Lake o tablas particionadas, el optimizador **Catalyst** no lee todas las filas a memoria RAM para filtrarlas después.


En su lugar, **empuja el filtro hasta el lector de la fuente de datos** (*Predicate Pushdown*):

* Spark lee el pie de página (footer) de cada archivo Parquet.

* Compara tu condición contra las estadísticas `[min, max]` de cada Row Group.

* Si estás buscando `edad >= 18` y un Row Group indica `max(edad) = 15`, Spark **descarta el bloque entero sin leer ni descomprimir sus datos**. El ahorro real depende de la selectividad del filtro y de cuán ordenados estén físicamente los datos: con datos ordenados o particionados por esa columna puede ser enorme; con datos aleatorios, casi nulo.


---

> **Consideraciones de Producción 10.1: Condicionales de Negocio Complejos**
> Patrones idiomáticos para clasificación con when().otherwise(), filtrado por listas dinámicas con isin() y saneamiento de nulos:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("Capitulo10-FiltradoCondicionales") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet("data/clientes.parquet")

# 1. Clasificación categórica basada en múltiples condiciones (CASE WHEN)
df_segmentado = df.withColumn(
    "nivel_riesgo",
    when(col("puntaje_credito") >= 750, "BAJO")
    .when((col("puntaje_credito") >= 600) & (col("puntaje_credito") < 750), "MEDIO")
    .otherwise("ALTO")
)

# 2. Filtrado eficiente por colecciones con isin()
paises_foco = ["Colombia", "México", "Chile", "Perú"]
df_region = df_segmentado.filter(col("pais_origen").isin(paises_foco))

# 3. Control estricto de nulos y cadenas vacías
df_limpio = df_region.filter(
    col("email").isNotNull() & 
    (col("email") != "") & 
    (col("estado_cuenta") == "ACTIVO")
)

df_limpio.show(5)
```

---


## 10.3 Criterios de Fiabilidad y Anti-Patrones en Producción


**Anti-patrón 1: Dejar para el final un filtro que podía aplicarse antes**

Catalyst reordena muchos casos por ti: un filtro que depende solo de la **clave de agrupación**, como `df.groupBy("pais").agg(F.sum("monto").alias("total")).filter(F.col("pais") == "CO")`, se empuja automáticamente por debajo de la agregación (véase el Ejercicio 13.2) y no provoca un shuffle de todos los países. En cambio, no puede reordenar un filtro que depende de un **resultado agregado** (`.filter(F.col("total") > 1000)`, el equivalente de `HAVING`) ni atravesar UDFs no deterministas. Escribir el filtro de fila lo más temprano posible, `df.filter(F.col("pais") == "CO").groupBy("pais").agg(...)`, hace explícita la intención y no depende de que el optimizador pueda reordenarlo.


**Anti-patrón 2: Comparaciones de igualdad con Nulos mediante ==**

En SQL y PySpark, `col("x") == None` o `col("x") == "null"` no evalúa como True ante valores nulos (evalúa como Unknown/Null). Utiliza siempre los métodos canónicos `.isNull()` o `.isNotNull()`.


---


## Preguntas de Revisión y Ejercicios Prácticos
**Ejercicio 10.1 (Optimización de Predicado):**

Explica qué diferencia de ejecución física existe entre:

`df.filter((col("a") > 10) & (col("b") == "X"))` y

`df.filter(col("a") > 10).filter(col("b") == "X")`.

**Solución:**
**Respuesta:** En tiempo de ejecución física **son exactamente idénticos**. El optimizador Catalyst combina automáticamente múltiples filtros encadenados sucesivos en una única expresión de conjunción lógica booleana (*Filter Pushdown / Combining Operators*), por lo que puedes encadenar llamadas a `.filter()` para mayor legibilidad sin ninguna penalización de rendimiento.

---

## 10.4 Resumen y Puntos Clave

* **Criba temprana:** Filtrar al borde de la ingesta es la técnica más rentable para ahorrar costos de cómputo y red en la nube.
* **Operadores bitwise y paréntesis:** En PySpark siempre usa `&`, `|`, `~` con paréntesis en cada condición.
* **Predicate Pushdown:** Catalyst descarga el filtrado a la capa de archivos Parquet/Delta para saltarse la lectura de datos irrelevantes.

---

## Lecturas Recomendadas
1. **Chambers, B. & Zaharia, M. (2018).** *Spark: The Definitive Guide.* O'Reilly Media. (Capítulo 5: Basic Structured Operations).
2. **Databricks Documentation:** *Data Skipping for Delta Lake* (`https://docs.databricks.com/delta/data-skipping.html`).
