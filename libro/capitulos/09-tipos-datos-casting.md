<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Tipos de Datos y Casting en Spark]] -->

# Capítulo 9: Tipos de Datos y Casting Riguroso en Memoria [NÚCLEO EMPLEABILIDAD]

> *"Los tipos de datos son el contrato estricto de ingeniería entre tu lógica y el clúster: le indican al motor distribuido cuántos bytes reservar en la memoria RAM de los nodos ejecutores, cómo indexar columnas y cómo compilar el código de bajo nivel mediante el motor Tungsten."*

---

## Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Dominar el catálogo de tipos de datos en PySpark (`StructType`, `IntegerType`, `LongType`, `DecimalType`, `DateType`, `TimestampType`).
* Aplicar la regla de oro financiera: evitar `Float` y `Double` para cálculos monetarios a favor de `DecimalType`.
* Implementar técnicas de casting individual y en bloque dinámico mediante metadatos.
* Anticipar el comportamiento de un cast inválido (`NULL` silencioso o error) según el modo ANSI, y usar `try_cast` para controlarlo.

---

> **Definición 9.1 (La Aduana de la Fábrica de Alta Velocidad)**
> Imagina que estás construyendo una línea de producción automatizada en una fábrica de alta velocidad.
> 
> Si a un robot calibrado para recibir tornillos metálicos de exactamente 10 milímetros le envías un bloque de arcilla blanda, la maquinaria se detiene de golpe o el producto terminado sale completamente defectuoso.
> 
> En Apache Spark y PySpark, los tipos de datos son exactamente ese contrato de tolerancia milimétrica: le dicen al motor cuántos bytes reservar en memoria y cómo operar en microsegundos. Si dejas que el motor "adivine" tipos leyendo strings mal formateados (`inferSchema`), pagas una lectura adicional de los datos y arriesgas que el esquema inferido cambie de una ejecución a otra.

---

## 9.1 El Catálogo Esencial de Tipos en PySpark

Para trabajar con los estándares de la industria, siempre importamos los tipos de forma explícita desde `pyspark.sql.types`:

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DoubleType, DecimalType, BooleanType,
    DateType, TimestampType, ArrayType, MapType
)
```

### Tipos Numéricos y Cuándo Elegir Cada Uno

1. **`IntegerType` vs. `LongType`:**
   * Un entero estándar (`IntegerType`) ocupa **4 bytes** y almacena valores entre -2.147 millones y +2.147 millones.
   * Si tus identificadores transaccionales, números de factura o métricas de eventos superan esta cifra, debes usar `LongType` (**8 bytes**).
   * Si tus datos jamás superarán ese rango, utilizar `LongType` innecesariamente es desperdiciar la mitad de la memoria RAM del clúster en esas columnas.
2. **`DoubleType` vs. `DecimalType(precision, scale)` (Regla de Oro):**
   * **En aplicaciones financieras, contables o transaccionales jamás uses Float ni Double para dinero.**
   * Los tipos de punto flotante introducen errores microscópicos de redondeo binario (estándar IEEE 754) que acumulan discrepancias de centavos.
   * Para dinero y montos exactos se debe usar siempre `DecimalType(18, 2)` o `DecimalType(38, 4)`.
3. **`DateType`, `TimestampType` y `TimestampNTZType`:**
   * `DateType` almacena únicamente la fecha de calendario (`YYYY-MM-DD`).
   * `TimestampType` almacena un instante físico absoluto (microsegundos desde la época Unix en UTC). Al mostrarse o proyectarse a cadena, Spark lo renderiza en la zona horaria activa de la sesión (`spark.sql.session.timeZone`).
   * `TimestampNTZType` (*Timestamp Without TimeZone*, introducido en Spark 3.4 y consolidado en Spark 4.0): almacena la fecha y hora local exactamente como fue registrada (año, mes, día, hora, minuto, segundo), sin referencia a huso horario ni ajustes de horario de verano (*Daylight Saving Time*).
4. **El Tipo `VariantType` (Spark 4.0):**
   * En versiones previas, los datos JSON semiestructurados exigían o bien almacenar cadenas de texto sin tipar (`StringType`) o declarar jerarquías complejas y rígidas de `StructType` y `MapType`.
   * El tipo **`VARIANT`** almacena documentos semiestructurados en un formato binario compacto con soporte nativo para navegación directa de campos (`col("payload:cliente.nombre")`), triturado columnar en disco (*sub-column shredding*) y evolución libre de esquema sin penalización de rendimiento.

---


## 9.2 Profundización Técnica: El Motor Tungsten y la Memoria Off-Heap


Cuando Spark opera con DataFrames y tipos estrictos, no necesita crear un objeto Java por cada celda. El motor **Project Tungsten** codifica las filas en un formato binario compacto (*UnsafeRow*), que por defecto reside en el heap de la JVM pero se gestiona manualmente, sin sobrecarga de objetos. Opcionalmente puede ubicarse **fuera del heap** (*off-heap*) con `spark.memory.offHeap.enabled=true`, lo que reduce la presión sobre el recolector de basura:


* Un `IntegerType` ocupa 4 bytes contiguos dentro de la fila binaria, en lugar de un objeto `java.lang.Integer` que, con su encabezado y alineación, ocupa típicamente 16 bytes.

* Además, el compilador genera código bytecode en tiempo de ejecución (*Whole-Stage Code Generation*) pensado para aprovechar las cachés L1/L2 del procesador del nodo ejecutor.


---

> **Consideraciones de Producción 9.1: Casting Dinámico en Bloque**
> Cuando recibes tablas con decenas de columnas, declarar transformaciones manuales una a una ensucia el código. Un ingeniero senior automatiza el proceso mediante metadatos y sanitización previa:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Capitulo09-TiposDatosCasting") \
    .master("local[*]") \
    .getOrCreate()

# Cargar dataset con columnas en formato texto crudo
df = spark.read.csv("data/ventas_crudo.csv", header=True)

# 1. Metadatos para casting automatizado: columna -> tipo destino
campos_enteros = ["sucursal_id", "categoria_id", "cantidad_articulos"]
campos_monetarios = ["precio_unitario", "subtotal", "impuesto_iva", "total"]

casts = {c: F.col(c).cast("int") for c in campos_enteros}
casts.update({c: F.col(c).cast("decimal(18,2)") for c in campos_monetarios})

# 2. Una sola proyección con todos los casts (evita encadenar decenas de withColumn)
df_procesado = df.withColumns(casts)   # Spark 3.3+
# Equivalente en cualquier versión:
# df_procesado = df.select(*[casts.get(c, F.col(c)).alias(c) for c in df.columns])

# 3. Sanitización defensiva: try_cast devuelve NULL en vez de lanzar error
df_sanitizado = df_procesado.withColumn(
    "edad_limpia", F.expr("try_cast(edad_raw AS INT)")
)

df_sanitizado.printSchema()
```

Nota: encadenar `withColumn` dentro de un bucle genera un plan lógico cada vez más profundo y ralentiza el análisis de Catalyst; el Cap. 11 profundiza en este anti-patrón.

---


## 9.3 Criterios de Fiabilidad y Anti-Patrones en Producción


**Anti-patrón 1: Ignorar cómo se comporta un cast inválido**

Qué ocurre al convertir `'N/A'`, `'PENDIENTE'` o una cadena vacía a `IntegerType` **depende de la configuración `spark.sql.ansi.enabled`**:

| Modo | Valor por defecto en | `CAST('N/A' AS INT)` |
| :--- | :--- | :--- |
| ANSI desactivado | Spark 3.x | Devuelve `NULL` en silencio |
| ANSI activado | Spark 4.0 en adelante | Lanza un error (`CAST_INVALID_INPUT`) y detiene el job |

En Spark 3.x, si no supervisas el resultado con aserciones de calidad de datos, inyectarás millones de nulos invisibles en la capa Silver. En Spark 4.0 el mismo código falla de forma ruidosa, lo cual es más seguro pero puede romper pipelines migrados. La herramienta correcta en ambos modos es **`try_cast`** (SQL) o `F.expr("try_cast(col AS INT)")`, que devuelve `NULL` de forma explícita cuando la conversión no es posible, y después se cuentan o se enrutan esos nulos a cuarentena (Cap. 28). Activar `spark.sql.ansi.enabled=false` para recuperar el comportamiento antiguo debe ser una decisión consciente y documentada, no un parche.


**Anti-patrón 2: Inconsistencia de Tipos en Operaciones de Join**

Si unes dos DataFrames donde en una tabla `cliente_id` es `StringType` y en la otra es `IntegerType`, Spark inserta un *cast* implícito sobre una de las columnas. El join sigue funcionando, pero ese cast puede impedir el *pushdown* del predicado hacia la fuente o el *partition pruning*, y provoca comparaciones inesperadas (por ejemplo, ceros a la izquierda). Declara los tipos de las claves explícitamente antes del cruce.


---


## Preguntas de Revisión y Ejercicios Prácticos
**Ejercicio 9.1 (Redondeo de Punto Flotante):**

Demuestra mediante un script simple en PySpark por qué `DoubleType` produce errores de redondeo en operaciones contables acumuladas frente a `DecimalType(18,2)`.

**Solución:**
```python
from pyspark.sql.functions import lit, col

df_test = spark.range(1).select(
    (lit(0.1) + lit(0.2)).alias("suma_double"),
    (lit(0.1).cast("decimal(10,2)") + lit(0.2).cast("decimal(10,2)")).alias("suma_decimal")
)
df_test.show()
# suma_double: 0.30000000000000004  <-- Error de IEEE 754
# suma_decimal: 0.30                <-- Precisión exacta financiera
```

---

## 9.4 Resumen y Puntos Clave

* **Contrato de rendimiento:** El tipado explícito optimiza el footprint de memoria en Tungsten y evita fallos en los ejecutores.
* **Finanzas con Decimal:** Nunca uses `Float` o `Double` para transacciones monetarias; usa `DecimalType(precision, scale)`.
* **Casting defensivo:** Usa `try_cast` y valida los nulos resultantes; recuerda que en Spark 4.0 un cast inválido lanza error por defecto (ANSI) mientras que en 3.x devuelve `NULL`.

---

## Lecturas Recomendadas
1. **Goldberg, D. (1991).** *What Every Computer Scientist Should Know About Floating-Point Arithmetic.* ACM Computing Surveys.
2. **Databricks Engineering Blog:** *Deep Dive into Project Tungsten: Bringing Spark Closer to Bare Metal.*
