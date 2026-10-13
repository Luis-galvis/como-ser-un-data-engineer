<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: Joins Distribuidos, Estrategias de Cruce y Optimización de Shuffle -->
# Capítulo 12: Joins Distribuidos y Estrategias de Cruce [NÚCLEO EMPLEABILIDAD]

> *"El join distribuido es la prueba de fuego de cualquier arquitectura analítica: un cruce mal planificado satura la red del clúster con terabytes de shuffle y bloqueos por asimetría, mientras que una estrategia de join adecuada transforma consultas de horas en ejecuciones de segundos."*

---

## Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender la complejidad algorítmica y física de unir conjuntos de datos distribuidos en una arquitectura sin memoria compartida (*Shared-Nothing*).
* Comparar en profundidad las cuatro estrategias físicas de join en Apache Spark: **Broadcast Hash Join (BHJ)**, **Shuffle Sort-Merge Join (SMJ)**, **Shuffle Hash Join (SHJ)** y **Broadcast Nested Loop Join (BNLJ)**.
* Controlar la selección física de operadores mediante sugerencias del optimizador (*Join Hints*) en SQL y la API DataFrame.
* Diseñar e implementar la técnica de salado de claves (*Salting*) en PySpark para mitigar cuellos de botella severos por sesgo de datos (*Data Skew*).
* Evaluar las limitaciones de las tablas agrupadas en cubos (*Bucket Tables*) en formatos tradicionales frente a formatos abiertos de Lakehouse.
* Auditar planes físicos mediante `.explain()` distinguiendo `ShuffleExchangeExec` de `BroadcastExchangeExec`.

---

> **Definición 12.1 (Join Distribuido y Costo de Barajado / Shuffle)**
> En sistemas de computación distribuida compartida (*Shared-Nothing*), un **join distribuido** requiere que las tuplas de ambas tablas que comparten la misma clave de unión (*Join Key*) residan físicamente en la misma partición y en el mismo nodo ejecutor antes de evaluar la condición relacional.
> 
> Cuando las tablas no están pre-agrupadas ni particionadas por dicha clave, el motor debe ejecutar una fase masiva de redistribución de red denominada **Shuffle**. Durante el shuffle, cada ejecutor particiona sus datos locales por el valor hash de la clave y transmite fragmentos a través de la red hacia los ejecutores de destino. Esta fase involucra serialización en memoria, transferencias por sockets TCP y volcado temporal en discos locales, constituyendo la operación más intensiva en recursos de todo el pipeline.

---

## 12.1 Las Cuatro Estrategias Físicas de Join en Apache Spark

Cuando el optimizador Catalyst compila una operación relacional de cruce (`df_a.join(df_b, "cliente_id")`), evalúa las estadísticas de los datos, el tamaño estimado de las tablas y el tipo de predicado (equi-join con operador `=` o no-equi-join con `<`, `>`, `!=`) para seleccionar una de las siguientes estrategias físicas:

```
                                  [ TIPO DE CONDICIÓN ]
                                            │
                     ┌──────────────────────┴──────────────────────┐
                [ Equi-Join (=) ]                         [ No-Equi-Join (<, >, !=) ]
                     │                                             │
      ┌──────────────┴──────────────┐                              │
[ Una tabla cabe en memoria? ] [ Ambas tablas gigantes? ]          │
      │                                     │                      │
[ Broadcast Hash Join ]          [ Shuffle Sort-Merge Join ]   [ Broadcast Nested Loop ]
  (BroadcastExchange)               (Shuffle + Sort local)         (Producto cartesiano)
```

---

### 1. Broadcast Hash Join (BHJ - Máximo Rendimiento)
* **Mecanismo:** Si una de las dos tablas tiene un tamaño físico estimado inferior al umbral configurable (`spark.sql.autoBroadcastJoinThreshold`, establecido en 10 MB por defecto), el nodo Driver descarga los datos de la tabla pequeña, construye una tabla hash en memoria y la transmite por red (*broadcast*) a todos los ejecutores del clúster mediante un operador `BroadcastExchangeExec`.
* **Comportamiento en los Workers:** Cada ejecutor mantiene una copia local de la tabla hash de referencia en su memoria RAM. A medida que procesa sus particiones locales de la tabla masiva, consulta la tabla hash en tiempo constante $O(1)$ por fila.
* **Impacto en Rendimiento:** **Elimina el `ShuffleExchangeExec` sobre la tabla grande**. No se transmiten terabytes entre ejecutores ni se requiere ordenamiento en disco. La única transferencia de red es el envío unidireccional de la tabla pequeña desde el Driver hacia los Workers.

### 2. Shuffle Sort-Merge Join (SMJ - El Estándar Robusto)
* **Mecanismo:** Aplicable cuando ambas tablas superan el umbral de broadcast y la condición es una igualdad (*Equi-Join*). Opera en dos fases:
  1. **Fase de Shuffle (Redistribución por Hash):** Spark evalúa el hash de la clave de unión para cada fila y redistribuye los registros de ambas tablas mediante un operador `ShuffleExchangeExec`, garantizando que todas las filas con la misma clave lleguen a la misma partición física.
  2. **Fase de Ordenamiento y Fusión (Sort-Merge):** Cada ejecutor ordena localmente sus particiones por la clave de unión y avanza punteros simultáneos sobre ambas listas para identificar coincidencias en un solo pase lineal con complejidad temporal $O(N + M)$.
* **Impacto en Rendimiento:** Requiere I/O de red masivo y eventual volcado a disco (*Shuffle Spill*), pero es el algoritmo más escalable ante volúmenes arbitrariamente grandes sin desbordar la memoria de los ejecutores.

### 3. Shuffle Hash Join (SHJ)
* **Mecanismo:** Similar a SMJ en la fase de redistribución por red (`ShuffleExchangeExec`), pero en lugar de ordenar los datos en disco, el ejecutor construye una tabla hash en memoria con la partición de la tabla menor y recorre secuencialmente la partición de la tabla mayor.
* **Requisito y Riesgo:** Cada partición individual de la tabla menor debe caber holgadamente en la memoria heap del ejecutor; de lo contrario, el proceso colapsa con un error de memoria agotada (*OutOfMemoryError*). Está desactivado por omisión en favor de SMJ (`spark.sql.join.preferSortMergeJoin = true`).

### 4. Broadcast Nested Loop Join (BNLJ)
* **Mecanismo:** Se utiliza cuando el cruce no contiene condiciones de igualdad (por ejemplo, `df_a.monto > df_b.limite`) o cuando se ejecuta un producto cartesiano (`crossJoin`). Transmite una tabla y evalúa bucles anidados para cada registro. Su complejidad temporal es cuadrática $O(N \times M)$, por lo que debe evitarse rigurosamente en producción salvo en tablas de muy baja cardinalidad.

---

## 12.2 Join Hints en PySpark y Spark SQL

Cuando el optimizador Catalyst carece de estadísticas precisas o cuando el ingeniero conoce la volumetría de antemano, es posible forzar la estrategia física deseada mediante **Join Hints**:

### Sintaxis en la API DataFrame de PySpark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

spark = SparkSession.builder \
    .appName("Capitulo12-JoinsDistribuidos") \
    .getOrCreate()

df_ventas = spark.read.parquet("lakehouse/silver/ventas/")
df_paises = spark.read.parquet("lakehouse/silver/dim_paises/")

# 1. Sugerencia de Broadcast mediante función nativa o método hint()
df_enriquecido_bhj = df_ventas.join(
    broadcast(df_paises),
    df_ventas["codigo_pais"] == df_paises["codigo_pais"],
    "inner"
)

# 2. Forzar Shuffle Sort-Merge Join explícitamente
df_enriquecido_smj = df_ventas.hint("merge").join(
    df_paises,
    df_ventas["codigo_pais"] == df_paises["codigo_pais"],
    "inner"
)

# 3. Forzar Shuffle Hash Join
df_enriquecido_shj = df_ventas.hint("shuffle_hash").join(
    df_paises,
    df_ventas["codigo_pais"] == df_paises["codigo_pais"],
    "inner"
)
```

### Sintaxis en Spark SQL Declarativo

En sentencias SQL puras, las sugerencias se declaran dentro de comentarios de bloque especiales `/*+ ... */`:

```sql
-- 1. Forzar Broadcast Hash Join
SELECT /*+ BROADCAST(p) */ 
    v.transaccion_id, v.monto, p.nombre_pais
FROM ventas v
JOIN dim_paises p ON v.codigo_pais = p.codigo_pais;

-- 2. Forzar Shuffle Sort-Merge Join
SELECT /*+ MERGE(v, p) */ 
    v.transaccion_id, v.monto, p.nombre_pais
FROM ventas v
JOIN dim_paises p ON v.codigo_pais = p.codigo_pais;

-- 3. Forzar Shuffle Hash Join
SELECT /*+ SHUFFLE_HASH(p) */ 
    v.transaccion_id, v.monto, p.nombre_pais
FROM ventas v
JOIN dim_paises p ON v.codigo_pais = p.codigo_pais;

-- 4. Forzar Broadcast Nested Loop Join (para no-equi-joins con tabla derecha pequeña)
SELECT /*+ BROADCAST(p) */ 
    v.transaccion_id, v.monto, p.rango_min
FROM ventas v
JOIN rangos_comision p ON v.monto >= p.rango_min;

-- 5. Forzar Cartesian Product con Shuffle Replicate (para tablas grandes sin clave de igualdad)
SELECT /*+ SHUFFLE_REPLICATE_NL(p) */ 
    v.transaccion_id, v.monto, p.rango_min
FROM ventas v
JOIN rangos_comision p ON v.monto >= p.rango_min;
```

### Inspección del Plan Físico: `ShuffleExchangeExec` vs. `BroadcastExchangeExec`

Al inspeccionar el plan con `df_enriquecido_bhj.explain(mode="formatted")`, observe los operadores generados:

```
*(2) BroadcastHashJoin [codigo_pais#12], [codigo_pais#45], Inner, BuildRight
:- *(2) Project [transaccion_id#10, monto#11, codigo_pais#12]
:  +- *(2) Filter isnotnull(codigo_pais#12)
:     +- *(2) Scan parquet [transaccion_id#10,monto#11,codigo_pais#12]
+- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
   +- *(1) Project [codigo_pais#45, nombre_pais#46]
      +- *(1) Filter isnotnull(codigo_pais#45)
         +- *(1) Scan parquet [codigo_pais#45,nombre_pais#46]
```

Nótese que `BroadcastExchange` recopila la tabla derecha en el Driver y la distribuye a los ejecutores, mientras que la tabla izquierda (ventas) se escanea directamente en el Stage 2 sin ningún `ShuffleExchangeExec`.

---

## 12.3 Mitigación de Asimetría de Datos (Data Skew) en Uniones

El fenómeno de **Data Skew** se presenta cuando una porción desproporcionada de las filas comparte la misma clave de unión (por ejemplo, transacciones con cliente nulo o un cliente corporativo mayoritario). En un Sort-Merge Join, esa clave hash única es asignada a una sola partición de shuffle, sobrecargando a un ejecutor mientras los demás permanecen ociosos:

```
[ Partición 0: 50.000 filas ] ───────> Ejecutor 1 (Termina en 2 seg)
[ Partición 1: 50.000 filas ] ───────> Ejecutor 2 (Termina en 2 seg)
[ Partición 2: 80.000.000 filas (SKEW) ] ──> Ejecutor 3 (Tarda 45 min o falla por OOM)
```

### Solución 1: Adaptive Query Execution (AQE Skew Join)

Desde Apache Spark 3.0, el optimizador adaptativo detecta en tiempo de ejecución las particiones asimétricas tras la fase de shuffle y las divide automáticamente en subtareas más pequeñas:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")
```

### Solución 2: Técnica de Salting (Salado de Claves) para Cruces

Cuando el sesgo es extremo o AQE no logra balancear la carga de forma óptima, se aplica **Salting sobre el Join**:
1. A la tabla grande y sesgada se le añade una clave aleatoria (*salt*) entre $0$ y $K - 1$.
2. La tabla de búsqueda o dimensión se replica multiplicando cada una de sus filas por los mismos $K$ factores mediante una función de explosión (`explode`).
3. El join se ejecuta sobre la clave compuesta `(clave_original, salt)`, distribuyendo las filas asimétricas de manera uniforme entre $K$ particiones físicas distintas.

```python
from pyspark.sql import functions as F

K = 8  # Factor de dispersión (salting)

# 1. Tabla grande asimétrica (df_ventas con millones de compras del cliente 'C-000')
df_ventas_salted = df_ventas.withColumn(
    "salt",
    F.floor(F.rand() * K).cast("int")
)

# 2. Tabla de referencia (df_clientes): replicar cada registro K veces
df_clientes_replicated = df_clientes.withColumn(
    "salt_array",
    F.array([F.lit(i) for i in range(K)])
).withColumn(
    "salt",
    F.explode("salt_array")
).drop("salt_array")

# 3. Join libre de sesgo sobre la clave compuesta
df_ventas_balanceadas = df_ventas_salted.join(
    df_clientes_replicated,
    on=["cliente_id", "salt"],
    how="inner"
).drop("salt")
```

Este procedimiento garantiza que la clave sesgada se disperse equitativamente entre $K$ tareas de shuffle diferentes, eliminando por completo el cuello de botella de la tarea rezagada (*Straggler Task*).

---

> **Consideraciones de Producción 12.1: Bucketed Joins y su Compatibilidad**
> En tablas gestionadas tradicionales de Apache Hive o Apache Parquet (`saveAsTable`), es posible pre-ordenar y pre-agrupar registros mediante `bucketBy`:
> ```python
> df.write.bucketBy(32, "cliente_id").sortBy("cliente_id").saveAsTable("clientes_bucketed")
> ```
> Si dos tablas se unen por la misma clave y ambas comparten idéntico número de cubos, Spark realiza un **Bucket Sort-Merge Join sin Shuffle**.
> 
> *Aclaración Crítica de Producción:* `bucketBy` **no es compatible con rutas directas ni con el formato Delta Lake**. Si intenta invocar `.bucketBy()` al escribir una tabla Delta, el motor ignorará la directiva o arrojará una excepción. En arquitecturas modernas sobre Delta Lake, la técnica estándar para agrupar datos multidimensionalmente y evitar reordenamientos es **Liquid Clustering** (`CLUSTER BY cliente_id`).

---

## 12.4 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Aplicar `broadcast()` a DataFrames que Exceden la Capacidad del Driver**  
Forzar el broadcast de un DataFrame de gran volumen (por ejemplo, > 4 GB) obliga al nodo Driver a recolectar todas las particiones en su memoria heap local, provocando la interrupción del trabajo por `Driver OutOfMemoryError`. Reserve `broadcast()` estrictamente para tablas de dimensiones que quepan cómodamente en memoria (por lo general < 500 MB).

**Anti-patrón 2: Uniones Externas (`FULL OUTER JOIN`) sin Filtrado Previo de Particiones**  
Ejecutar un `FULL OUTER JOIN` sobre dos tablas transaccionales de escala masiva sin delimitar rangos de fechas produce una explosión de registros nulos en ambos lados y un barajado de datos innecesario que degrada el rendimiento de los ejecutores. Aplique siempre filtros de partición antes del cruce.

---

## Preguntas de Revisión y Ejercicios Prácticos
**Ejercicio 12.1 (Comparativa de Memoria entre BHJ y SMJ):**  
¿Cuál es la diferencia fundamental en el uso y distribución de memoria entre un Broadcast Hash Join y un Shuffle Sort-Merge Join?

**Solución 12.1:**
* En **Broadcast Hash Join (BHJ)**: El nodo Driver debe disponer de suficiente memoria heap para ensamblar la tabla completa y serializarla. Posteriormente, cada nodo ejecutor consume memoria adicional en su espacio de almacenamiento/ejecución para albergar la copia íntegra de la tabla hash. La tabla grande se procesa en streaming partición por partición sin requerir memoria extra para ordenamiento ni buffers de red de shuffle.
* En **Shuffle Sort-Merge Join (SMJ)**: El nodo Driver no almacena ningún dato de las tablas. La memoria se consume de manera distribuida en los ejecutores dentro de la región de memoria de ejecución (`Execution Memory`), donde se gestionan los buffers de red para el intercambio de shuffle y las estructuras de ordenamiento en memoria (*AppendOnlyMap* / *Sorter*). Si la memoria de ejecución se satura durante la fase de ordenamiento, los registros se vuelcan a disco local (*Spill*), garantizando estabilidad a expensas de mayor latencia de I/O.

---

**Ejercicio 12.2 (Salting sobre Operaciones de Cruce):**  
Explique por qué aplicar salting mediante un número aleatorio a la tabla sesgada obliga a replicar las filas de la tabla de referencia mediante `explode()`.

**Solución 12.2:**
Si la tabla grande contiene 10 millones de registros con la clave `cliente_id = 'C-000'` y a cada registro se le asigna un valor de `salt` aleatorio entre 0 y 3, existirán 4 combinaciones posibles de clave compuesta: `('C-000', 0)`, `('C-000', 1)`, `('C-000', 2)` y `('C-000', 3)`.
Si la tabla de clientes únicamente tuviera una fila con `cliente_id = 'C-000'`, solo coincidiría con los registros de ventas cuyo salt generado coincidiera con ese único registro, perdiendo el 75% de las transacciones legítimas.
Al replicar la fila del cliente en 4 copias idénticas con los salts 0, 1, 2 y 3 mediante `explode()`, se garantiza la semántica relacional completa: cada transacción encontrará su contraparte correspondiente, mientras que la carga de trabajo se reparte equitativamente en 4 particiones físicas de shuffle distintas.

---

**Ejercicio 12.3 (Restricciones de Broadcast en Operaciones `LEFT JOIN`):**  
En una consulta con un join de tipo `LEFT JOIN` (`df_izq.join(df_der, ..., "left")`), ¿cuál de las dos tablas puede ser transmitida mediante broadcast sin alterar la semántica relacional del resultado? Justifique la respuesta a nivel de planificación física.

**Solución 12.3:**
En un `LEFT JOIN`, **únicamente la tabla derecha (la tabla no preservada) puede ser transmitida mediante broadcast**.
*Justificación física:*
En la semántica de `LEFT JOIN`, todos los registros de la tabla izquierda deben preservarse obligatoriamente en el resultado final, existan o no coincidencias en la derecha. Si se intentara hacer broadcast de la tabla izquierda, los ejecutores que procesan fragmentos de la tabla derecha no podrían saber de manera independiente y sin coordinación global qué filas de la tabla izquierda no tuvieron coincidencias, lo que obligaría a un intercambio de datos adicional.
En cambio, al transmitir la tabla derecha como tabla hash a todos los ejecutores, cada tarea puede recorrer de forma autónoma su partición local de la tabla izquierda: si la clave existe en la tabla hash derecha, emite la fila unida; si no existe, emite la fila izquierda con valores `NULL` en los campos de la derecha, sin necesidad de transferir ningún dato de la tabla izquierda por la red.

---

## 12.5 Resumen y Puntos Clave

* **Criterio de selección:** El optimizador Catalyst elige Broadcast Hash Join cuando una de las tablas es inferior a `spark.sql.autoBroadcastJoinThreshold` (10 MB). Para tablas masivas con equi-joins, la estrategia predeterminada y más estable es Shuffle Sort-Merge Join.
* **Costos físicos:** BHJ elimina el costo de `ShuffleExchangeExec` en la tabla grande pero introduce `BroadcastExchangeExec` desde el Driver. SMJ introduce shuffles en ambas tablas y ordenamiento local en los Workers.
* **Join Hints:** Permiten forzar estrategias cuando las estadísticas de tabla no están disponibles. Se expresan con `.hint("broadcast")` en DataFrames o `/*+ BROADCAST(t) */` en SQL.
* **Manejo de sesgo (Skew):** Adaptive Query Execution (AQE) divide particiones asimétricas en tiempo de ejecución. Ante casos extremos, el salado de claves (*Salting*) dispersa la clave sesgada entre $K$ particiones mediante números aleatorios y duplicación con `explode`.
* **Bucketing vs. Lakehouse:** `bucketBy` permite eliminar shuffles en tablas tradicionales de Parquet/Hive, pero no opera sobre Delta Lake, donde se sustituye por Liquid Clustering.

---

## Lecturas Recomendadas
1. **Chambers, B., & Zaharia, M. (2018).** *Spark: The Definitive Guide: Big Data Processing Made Simple.* O'Reilly Media. Capítulo 8: "Joins".
2. **Armbrust, M. et al. (2015).** *Spark SQL: Relational Data Processing in Spark.* Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data, 1383-1394.
3. **Apache Spark Documentation (2024).** *Performance Tuning: Join Strategy Hints and Adaptive Query Execution.* Apache Software Foundation (`https://spark.apache.org/docs/latest/sql-performance-tuning.html`).
