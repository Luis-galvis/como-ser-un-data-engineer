const fs = require('fs');
const path = require('path');
require('dotenv').config();

const VAULT_PATH = process.env.VAULT_PATH || 'D:\\estudio\\estudio';

const noteTiposDeDatos = `# Tipos de Datos y Casting en Spark

## 🧠 La Intuición: La Aduana de la Fábrica de Datos
Imagina que estás construyendo una línea de producción automatizada en una fábrica de alta velocidad. Si a un robot que espera tornillos metálicos de 10 milímetros le envías un bloque de arcilla blanda, la maquinaria se detiene o el producto terminado sale completamente defectuoso.

En **Apache Spark** y **PySpark**, los tipos de datos son exactamente ese contrato estricto de ingeniería. Le indican al motor distribuido exactamente cuántos bytes reservar en la memoria RAM de los nodos ejecutores, cómo indexar las columnas y cómo compilar el código de bajo nivel mediante el motor Tungsten.

Si permites que Spark "adivine" o infiera tipos a ciegas (por ejemplo leyendo archivos CSV o JSON con strings mal formateados), tu clúster consumirá el doble o triple de memoria y cualquier operación analítica posterior terminará fallando en producción.

---

## 🏗️ El Catálogo Esencial de Tipos en PySpark

Para trabajar con los estándares de la industria, siempre importamos los tipos de forma explícita desde \`pyspark.sql.types\`:

\`\`\`python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DoubleType, DecimalType, BooleanType,
    DateType, TimestampType, ArrayType, MapType
)
\`\`\`

### 1. Tipos Numéricos y Cuándo Elegir Cada Uno
- **\`IntegerType\` vs. \`LongType\`**: Un entero estándar (\`IntegerType\`) ocupa 4 bytes y almacena valores entre -2.147 millones y +2.147 millones. Si tus IDs transaccionales, números de factura o métricas de ventas pueden superar esta cifra, debes usar \`LongType\` (8 bytes). Si no la van a superar jamás, usar \`LongType\` innecesariamente es desperdiciar la mitad de tu memoria RAM en el clúster.
- **\`DoubleType\` vs. \`DecimalType(precision, scale)\`**: Esta es una de las reglas de oro de todo Data Engineer profesional: **en aplicaciones financieras, contables o transaccionales jamás uses Float ni Double para dinero**. Los tipos de punto flotante introducen errores microscópicos de redondeo binario (IEEE 754). Para dinero y montos exactos se debe usar siempre \`DecimalType(18, 2)\` o \`DecimalType(38, 4)\`.
- **\`DateType\` vs. \`TimestampType\`**: \`DateType\` almacena únicamente \`YYYY-MM-DD\`. \`TimestampType\` añade horas, minutos, segundos, fracciones de microsegundo y contexto de zona horaria.

---

## 🛠️ Técnicas de Casting en PySpark: De Básico a Senior

### Técnica 1: Uso de \`col().cast()\` en transformaciones individuales
Es el método idiomático y más legible para modificar o normalizar columnas:

\`\`\`python
from pyspark.sql.functions import col

df_casteado = df.withColumn("cliente_id", col("cliente_id").cast(IntegerType())) \\
                .withColumn("monto_transaccion", col("monto_transaccion").cast(DecimalType(14, 2))) \\
                .withColumn("fecha_operacion", col("fecha_texto").cast(DateType()))
\`\`\`

También puedes utilizar cadenas literales para definir el tipo de destino:
\`\`\`python
df_casteado = df.withColumn("cliente_id", col("cliente_id").cast("int")) \\
                .withColumn("monto_transaccion", col("monto_transaccion").cast("decimal(14,2)"))
\`\`\`

### Técnica 2: Casting en Bloque con Iteración Dinámica
Cuando recibes tablas con decenas de columnas, declarar transformaciones una a una ensucia el código y dificulta el mantenimiento. Un ingeniero senior automatiza el proceso mediante metadatos:

\`\`\`python
campos_enteros = ["sucursal_id", "categoria_id", "cantidad_articulos"]
campos_monetarios = ["precio_unitario", "subtotal", "impuesto_iva", "total"]

df_procesado = df

for campo in campos_enteros:
    df_procesado = df_procesado.withColumn(campo, col(campo).cast("int"))

for campo in campos_monetarios:
    df_procesado = df_procesado.withColumn(campo, col(campo).cast("decimal(18,2)"))
\`\`\`

---

## ⚠️ El Gran Peligro en Producción: Corrupción Silenciosa por Nulos
Cuando Spark intenta transformar un valor de texto que no cumple con el formato esperado (por ejemplo, intentar convertir \`'N/A'\`, \`'PENDIENTE'\` o una cadena vacía a \`IntegerType\`), **Spark no detiene el pipeline con un error**. En su lugar, convierte el valor en \`NULL\` silenciosamente.

Si no supervisas este comportamiento, puedes cargar a tu capa Silver o Gold millones de registros con datos faltantes sin darte cuenta.

### ¿Cómo protegerse con validaciones preventivas?
\`\`\`python
from pyspark.sql.functions import col, when

# Sanitizar antes de castear
df_sanitizado = df.withColumn(
    "edad_limpia",
    when(col("edad_raw").rlike("^[0-9]+$"), col("edad_raw").cast("int"))
    .otherwise(None)
)
\`\`\`

---

## 🎯 Consejos de Producción
1. **Define siempre el \`StructType\` al leer datos**: Evita usar \`inferSchema=True\` en fuentes masivas. La inferencia obliga a Spark a leer el archivo completo dos veces antes de comenzar.
2. **Consistencia de tipos en Joins**: Asegúrate de que las columnas clave de unión entre dos DataFrames tengan exactamente el mismo tipo de dato. Si una tabla tiene \`cliente_id\` como String y otra como Integer, Spark forzará un casting implícito o fallará el plan del Catalyst Optimizer.
`;

const noteFilterCondicionales = `# Filter y Condicionales en DataFrames

## 🧠 La Intuición: El Colador Inteligente en la Minería de Oro
Imagina que estás buscando oro en un río caudaloso. No tiene ningún sentido transportar toneladas de arena, piedras y lodo río abajo hasta la refinería para luego separarlo todo allí. Lo inteligente y económico es colocar una criba o colador justo en la orilla del río: descartas el 95% del material estéril de inmediato y solo transportas las partículas valiosas.

En el procesamiento distribuido con **PySpark**, filtrar temprano en el flujo de trabajo es la optimización número uno. Cada registro descartado antes de un \`groupBy\` o un \`join\` representa megabytes de datos que no tendrán que viajar por la red entre servidores ni saturar la memoria RAM de los nodos trabajadores.

---

## 🔍 Sintaxis Idiomática de Filtrado en PySpark

En PySpark disponemos de dos métodos equivalentes: \`filter()\` y \`where()\`. Ambos ejecutan exactamente la misma lógica bajo el capó.

\`\`\`python
from pyspark.sql.functions import col

# Sintaxis recomendada con objetos Columna
df_activos = df.filter(col("estado_cuenta") == "ACTIVO")

# Es exactamente lo mismo que:
df_activos = df.where(col("estado_cuenta") == "ACTIVO")
\`\`\`

---

## ⚡ Operadores Lógicos y Reglas Clave

A diferencia de Python puro donde usamos \`and\`, \`or\` y \`not\`, en PySpark **debemos usar operadores a nivel de bits (\`&\`, \`|\`, \`~\`) y envolver cada condición individual obligatoriamente entre paréntesis**:

\`\`\`python
# CORRECTO: Paréntesis en cada bloque de condición
df_filtrado = df.filter(
    (col("edad") >= 18) & 
    (col("pais") == "Colombia") & 
    (col("saldo") > 1000000)
)

# Negación lógica con la tilde ~
df_sin_bloqueo = df.filter(~col("cuenta_bloqueada"))
\`\`\`

---

## 🎯 Condicionales Avanzados: \`when()\`, \`otherwise()\` e \`isin()\`

### 1. La estructura \`when() / otherwise()\` (El CASE WHEN de PySpark)
Permite construir columnas derivadas basadas en múltiples condiciones de negocio:

\`\`\`python
from pyspark.sql.functions import col, when

df_categorizado = df.withColumn(
    "segmento_cliente",
    when(col("total_compras") >= 5000000, "PLATINUM")
    .when((col("total_compras") >= 1000000) & (col("total_compras") < 5000000), "GOLD")
    .otherwise("STANDARD")
)
\`\`\`

### 2. Filtrado por colecciones con \`isin()\`
\`\`\`python
paises_objetivo = ["Colombia", "México", "Chile", "Perú"]
df_region = df.filter(col("pais_origen").isin(paises_objetivo))
\`\`\`

### 3. Manejo riguroso de nulos: \`isNull()\` e \`isNotNull()\`
\`\`\`python
df_registros_validos = df.filter(col("email").isNotNull() & (col("email") != ""))
\`\`\`

---

## 🚀 La Magia de Catalyst: Predicate Pushdown
Cuando aplicas un \`filter()\` sobre un DataFrame que lee desde archivos Parquet, Delta Lake o bases de datos SQL, el optimizador **Catalyst** no lee todos los datos a la memoria para filtrarlos después.

En su lugar, **"empuja" el filtro directamente hasta la capa de almacenamiento** (\`Predicate Pushdown\`). Spark solo lee del disco los bloques de archivos que realmente contienen registros que cumplen tu condición, reduciendo el tiempo de lectura hasta en un 90%.
`;

const noteFuncionesTransformacion = `# Funciones de Transformación en PySpark

## 🧠 La Intuición: La Caja de Herramientas del Maestro Artesano
Imagina que eres un cirujano o un relojero de alta precisión: necesitas herramientas específicas para cada maniobra: pinzas para extraer, bisturí para cortar, lupas para enfocar. 

En **PySpark**, el módulo \`pyspark.sql.functions\` es precisamente esa caja de herramientas estándar de la industria. Contiene cientos de funciones ultra-optimizadas en C++ y Java diseñadas para operar a la velocidad de la luz sobre millones de registros en paralelo, sin que tengas que reinventar la rueda ni recurrir a bucles lentos de Python.

---

## 📦 Importación Estándar en la Industria

\`\`\`python
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, concat, upper, trim, to_date, current_date
\`\`\`

---

## 🧰 Las Familias de Funciones Imprescindibles

### 1. Transformación de Cadenas de Texto (String Manipulation)
\`\`\`python
df_limpio = df.withColumn("nombre_normalizado", F.upper(F.trim(col("nombre_crudo")))) \\
              .withColumn("correo_oficial", F.lower(F.trim(col("email")))) \\
              .withColumn("codigo_completo", F.concat(col("prefijo"), F.lit("-"), col("folio")))
\`\`\`

### 2. Manejo Profesional de Fechas y Tiempo
El tiempo es uno de los mayores desafíos en analítica de datos debido a zonas horarias y formatos inconsistentes:

\`\`\`python
df_temporal = df.withColumn("fecha_evento", F.to_date(col("fecha_texto"), "yyyy-MM-dd")) \\
                .withColumn("año_evento", F.year(col("fecha_evento"))) \\
                .withColumn("mes_evento", F.month(col("fecha_evento"))) \\
                .withColumn("dias_antiguedad", F.datediff(F.current_date(), col("fecha_evento")))
\`\`\`

### 3. Funciones Matemáticas y de Redondeo
\`\`\`python
df_calculos = df.withColumn("monto_redondeado", F.round(col("monto_bruto"), 2)) \\
                .withColumn("valor_absoluto", F.abs(col("diferencia")))
\`\`\`

### 4. Arrays, Listas y Desanidación (\`split\`, \`explode\`, \`array_contains\`)
\`\`\`python
# Separar una cadena en una lista
df_tags = df.withColumn("etiquetas_array", F.split(col("etiquetas_str"), ","))

# Expandir cada elemento del array en una fila separada
df_desanidado = df_tags.withColumn("etiqueta_individual", F.explode(col("etiquetas_array")))
\`\`\`

---

## 🚫 La Regla de Oro Senior: Evita las UDFs de Python
Muchos desarrolladores que vienen de Python tradicional tienen la tentación de usar \`@udf\` (User Defined Functions) para cualquier cálculo personalizado.

**¡Alerta de Rendimiento!** Cada vez que usas una UDF nativa de Python:
1. Spark tiene que serializar cada fila de la máquina virtual Java (JVM).
2. Enviarla por un socket a un proceso Python externo para que ejecute tu función.
3. Deserializar el resultado de vuelta a la JVM.

Este viaje de ida y vuelta destruye el rendimiento del clúster (puede ser entre 10 y 100 veces más lento). **Siempre busca resolver tu problema combinando las funciones nativas de \`pyspark.sql.functions\` antes de pensar en una UDF.**
`;

fs.writeFileSync(path.join(VAULT_PATH, 'Tipos de Datos y Casting en Spark.md'), noteTiposDeDatos, 'utf8');
fs.writeFileSync(path.join(VAULT_PATH, 'Filter y Condicionales en DataFrames.md'), noteFilterCondicionales, 'utf8');
fs.writeFileSync(path.join(VAULT_PATH, 'Funciones de Transformación en PySpark.md'), noteFuncionesTransformacion, 'utf8');

console.log('Notas enriquecidas con exito en:', VAULT_PATH);
