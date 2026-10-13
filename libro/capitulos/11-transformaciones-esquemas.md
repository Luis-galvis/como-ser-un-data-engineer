<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[with column y withcolum rename]], [[Funciones de Transformación en PySpark]], [[dataframes]] -->

# Capítulo 11: Transformaciones de Columnas y Operaciones con Esquemas [NÚCLEO EMPLEABILIDAD]

> *"El módulo pyspark.sql.functions es la caja de herramientas estándar del Data Engineer: funciones vectorizadas de C++ y Java diseñadas para operar a la velocidad de la luz en paralelo sin recurrir a lentas UDFs de Python."*

---

## Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Dominar `withColumn()` y `withColumnRenamed()` para generar nuevas columnas calculadas respetando la inmutabilidad de DataFrames.
* Aplicar funciones estándar de manipulación de texto (`upper`, `trim`, `concat`), fechas (`to_date`, `datediff`) y colecciones (`split`, `explode`).
* Conectar la generación de columnas con la evaluación perezosa y el motor Catalyst.
* Comprender la penalización de rendimiento de las UDFs de Python tradicionales y cómo evitarlas en producción.

---

> **Definición 11.1 (La Caja de Herramientas del Maestro Artesano)**
> Imagina que eres un cirujano o un relojero de alta precisión: necesitas herramientas específicas para cada maniobra: pinzas para extraer, bisturí para cortar, lupas para enfocar.
> 
> En PySpark, el módulo pyspark.sql.functions es precisamente esa caja de herramientas estándar: cientos de funciones optimizadas, implementadas en Scala/Java y ejecutadas dentro de la JVM del ejecutor, diseñadas para operar sobre millones de registros en paralelo sin tener que reinventar la rueda con bucles lentos de Python.

---

## 11.1 Creación y Renombrado de Columnas

Los métodos `withColumn` y `withColumnRenamed` nos ayudan a generar nuevas columnas calculadas o alterar esquemas en un DataFrame:

* Cada vez que los llamamos devuelven una **nueva instancia de DataFrame**, preservando la inmutabilidad y añadiendo un paso al grafo de ejecución perezosa (**DAG**).
* `withColumn(colName, colExpr)`: Añade una nueva columna calculada o reemplaza una existente si tienen el mismo nombre.
* `withColumnRenamed(existingName, newName)`: Simplemente renombra una columna sin alterar sus datos ni añadir lógica computacional.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, current_date

spark = SparkSession.builder.appName("Capitulo11-Transformaciones").getOrCreate()
df = spark.read.parquet("data/usuarios.parquet")

# 1. Crear columna con valor constante literal (lit)
df_dummy = df.withColumn("dummy_col", lit(1))

# 2. Calcular mayoría de edad a partir del año de nacimiento
df_evaluado = df.withColumn(
    "mayor_edad",
    (year(current_date()) - col("anio_nacimiento")) >= 18
)

# 3. Renombrar una columna para normalizar el esquema (el nuevo nombre debe seguir
#    describiendo lo que contiene: aquí sigue siendo un año, no una fecha)
df_renombrado = df_evaluado.withColumnRenamed("anio_nacimiento", "nacimiento_anio")
df_renombrado.show(5)
```

---


## 11.2 Profundización Técnica: UDFs de Python, Vectorización con Arrow y Funciones de Orden Superior

Muchos desarrolladores que provienen de Python tradicional recurren a `@udf` para cualquier transformación personalizada. **¡Alerta de Rendimiento!** Cuando se ejecuta una UDF estándar de Python:
1. La JVM empaqueta los registros en lotes serializados y los transmite a través de un socket TCP local hacia un proceso daemon de Python (`pyspark.daemon`).
2. El proceso de Python deserializa los datos y evalúa la función fila a fila, quedando completamente fuera de las optimizaciones de Catalyst y Tungsten.
3. El resultado vuelve a serializarse y cruza nuevamente el socket hacia la JVM, multiplicando el consumo de CPU y memoria.

### La Alternativa Vectorizada: Pandas UDFs con Apache Arrow
Cuando es imprescindible aplicar código de Python (por ejemplo, inferencia de modelos con Scikit-Learn o transformaciones con SciPy), se deben utilizar **Vectorized Pandas UDFs** (`@pandas_udf`), las cuales transfieren bloques de datos columnares directamente en memoria compartida mediante **Apache Arrow** con serialización de costo prácticamente nulo:

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def normalizar_monto_arrow(serie_monto: pd.Series) -> pd.Series:
    # Operación vectorizada sobre arrays de NumPy / Pandas en C/SIMD
    return (serie_monto - serie_monto.mean()) / (serie_monto.std() + 1e-9)

df_normalizado = df.withColumn("monto_std", normalizar_monto_arrow(col("monto_usd")))
```

---

> **Consideraciones de Producción 11.1: Manipulación Avanzada de Cadenas, Fechas y Arrays**
> Catálogo de transformaciones estandarizadas en producción con el alias canónico `F`:

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# 1. Normalización de cadenas de texto
df_texto = df.withColumn("nombre_limpio", F.upper(F.trim(col("nombre_crudo")))) \
             .withColumn("codigo_completo", F.concat(col("prefijo"), F.lit("-"), col("folio")))

# 2. Manejo riguroso de fechas
df_tiempo = df_texto.withColumn("fecha_evento", F.to_date(col("fecha_texto"), "yyyy-MM-dd")) \
                    .withColumn("dias_antiguedad", F.datediff(F.current_date(), col("fecha_evento")))

# 3. Desanidación de arrays con explode_outer() para preservar arrays vacíos o nulos
df_tags = df_tiempo.withColumn("etiquetas_array", F.split(col("etiquetas_str"), ","))
df_desanidado = df_tags.withColumn("etiqueta_individual", F.explode_outer(col("etiquetas_array")))

df_desanidado.show(5)
```

---

## 11.3 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Encadenar docenas de `withColumn` sucesivos**  
Llamar a `.withColumn()` 50 veces seguidas para agregar 50 columnas crea un árbol de Catalyst con 50 operadores de proyección anidados, ralentizando severamente la fase de análisis del Driver. Para transformaciones masivas, utilice `.withColumns({...})` con un diccionario de expresiones o una única sentencia `.select()` que proyecte todas las columnas en un solo paso.

**Anti-patrón 2: Usar literales escalares sin `F.lit()`**  
Si pasa un valor escalar como segundo argumento (`df.withColumn("tipo", "ACTIVO")`), Spark arrojará una excepción porque espera un objeto de tipo `Column`. Envuelva siempre los escalares con `F.lit("ACTIVO")`.

---

## Preguntas de Revisión y Ejercicios Prácticos
**Ejercicio 11.1 (Cálculo sobre Arrays: `explode` frente a Funciones de Orden Superior):**  
Si un DataFrame tiene una columna `historial_pagos` de tipo `ArrayType(DoubleType())`, escriba el código PySpark para calcular el pago promedio de cada cliente y compare la solución tradicional con `explode` frente al uso de **Higher-Order Functions** (`aggregate`).

**Solución 11.1:**  
1. *Solución tradicional con `explode` (y sus desventajas):*
```python
# Riesgo: explode() descarta filas si el array es vacío o NULL (requiere explode_outer).
# Además, groupBy("cliente_id") desencadena un SHUFFLE masivo innecesario por red.
df_con_shuffle = df.select(
    col("cliente_id"),
    F.explode_outer(col("historial_pagos")).alias("pago")
).groupBy("cliente_id").agg(F.avg("pago").alias("pago_promedio"))
```
2. *Solución óptima con Higher-Order Functions (Cero Shuffle y preserva registros):*
```python
# Con aggregate(), el cálculo se realiza en memoria fila por fila dentro de cada partición,
# eliminando el shuffle y preservando arrays vacíos o nulos mediante condiciones seguras:
df_optimo = df.withColumn(
    "pago_promedio",
    F.when(
        F.size(col("historial_pagos")) > 0,
        F.expr("aggregate(historial_pagos, 0.0D, (acc, x) -> acc + x) / size(historial_pagos)")
    ).otherwise(F.lit(None))
)
```
*Ventaja:* La solución con `aggregate()` procesa millones de filas en una sola pasada lineal $O(N)$ sin transferir un solo byte por la red del clúster.

---

## 11.4 Resumen y Puntos Clave

* **Inmutabilidad y linaje:** `withColumn` y `withColumns` generan nuevos nodos en el plan lógico sin mutar los datos existentes.
* **Vectorización con Arrow:** Las Pandas UDFs (`@pandas_udf`) transfieren bloques columnares a procesos de Python mediante memoria compartida Apache Arrow, superando las limitaciones de serialización de las UDFs estándar.
* **Funciones de orden superior:** Operadores como `aggregate`, `transform` y `filter` permiten procesar arrays directamente en memoria sin necesidad de aplanar con `explode` ni incurrir en shuffles.

---

## Lecturas Recomendadas
1. **Chambers, B. & Zaharia, M. (2018).** *Spark: The Definitive Guide.* O'Reilly Media. Capítulo 6: "Working with Different Types of Data".
2. **Apache Arrow Project (2024).** *In-Memory Analytics and Cross-Language Data Sharing.* Apache Software Foundation (`https://arrow.apache.org/`).
3. **Databricks Engineering (2024).** *Higher-Order Functions in Spark SQL.* Databricks Documentation (`https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html`).
