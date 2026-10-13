<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Unity Catalog]], [[Data Governance]], [[Data Security]], [[Open Source Unity Catalog]] -->

# Capítulo 19: Gobernanza, Linaje y Unity Catalog en Lakehouse [RECOMENDADO]

> *"En arquitecturas analíticas a escala de petabytes, un lago de datos desprovisto de gobernanza se degrada inevitablemente en un pantano de datos (Data Swamp). La gobernanza moderna no es una traba burocrática; es el plano de control centralizado que garantiza auditabilidad, trazabilidad de linaje y políticas homogéneas de seguridad sobre múltiples motores y nubes."*

---

## Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender la arquitectura de gobernanza desacoplada de **Unity Catalog** (tanto en su servicio gestionado como en su especificación de código abierto bajo la Linux Foundation).
* Dominar la topología de **Metastores Regionales** y el espacio de nombres jerárquico de tres niveles (*Three-Level Namespace: catalog.schema.table*).
* Administrar credenciales de almacenamiento (*Storage Credentials*) y ubicaciones externas (*External Locations*) mediante Infraestructura como Código (IaC).
* Configurar control de acceso basado en roles (RBAC) utilizando los privilegios modernos `USE CATALOG` y `USE SCHEMA`.
* Diseñar políticas de seguridad avanzadas: filtros a nivel de fila (*Row Filters*) y enmascaramiento dinámico a nivel de columna (*Column Masking*) preservando la coherencia estricta de tipos de datos.
* Auditar el ciclo de vida de tablas gestionadas (borrado suave con `UNDROP`) y el linaje automático de datos (*Data Lineage*) generado por Catalyst.

---

> **Definición 19.1 (Gobernanza Unificada y Unity Catalog)**
>
> **Unity Catalog** es una plataforma de gobernanza universal de código abierto (donada por Databricks a la Linux Foundation en 2024) y disponible también como servicio totalmente administrado. 
> 
> Centraliza la administración de permisos, auditoría y linaje para tablas (Delta Lake, Apache Iceberg), volúmenes de archivos no estructurados y modelos de machine learning. Opera desacoplando por completo las credenciales físicas de almacenamiento en la nube (AWS IAM, Azure Managed Identities, GCP Service Accounts) del plano lógico de consulta accesible por motores analíticos heterogéneos (Spark, Trino, DuckDB, Presto).

---

## 19.1 Arquitectura de Metastores Regionales y el Espacio de Nombres

Un principio fundamental de la arquitectura en la nube es que **un Metastore de Unity Catalog es una entidad regional**: reside físicamente en una región cloud específica (por ejemplo, `us-east-1` o `eastus2`) y se asocia a los espacios de trabajo (*workspaces*) y redes desplegados en esa misma región para evitar latencia de red y costos de transferencia inter-región (*Egress*).

A nivel lógico, la plataforma organiza todos los activos bajo un estándar de tres niveles:

```
[ Cuenta Corporativa ]
       │
       ▼ (Asignación por región)
[ Metastore Regional (ej. us-east-1) ]
       │
       ├── [ Catálogo: dev ]
       ├── [ Catálogo: staging ]
       └── [ Catálogo: prod_lakehouse ]
                 │
                 ├── [ Esquema: finanzas ]
                 └── [ Esquema: logistica ]
                           │
                           ├── [ Tablas: Delta / Iceberg ]
                           ├── [ Vistas Lógicas ]
                           ├── [ Volúmenes: Archivos Crudos ]
                           └── [ Modelos: MLflow ]
```

* **Catálogo (`catalog`):** Primer nivel de aislamiento lógico. Se utiliza habitualmente para segregar entornos de ciclo de vida (`dev`, `staging`, `prod`) o grandes unidades de negocio autónomas.
* **Esquema / Base de Datos (`schema`):** Segundo nivel de organización que agrupa tablas y activos con afinidad funcional (ej. `contabilidad`, `rrhh`, `ventas`).
* **Objetos Securizables:** Tercer nivel donde residen las tablas estructuradas, vistas, volúmenes para archivos no estructurados (documentos, imágenes) y modelos analíticos.

---

## 19.2 Desacoplamiento de Almacenamiento: Credenciales y External Locations

Una de las innovaciones arquitectónicas clave de la gobernanza moderna es que **los usuarios y analistas jamás reciben credenciales IAM directas** hacia los buckets de almacenamiento en la nube.

El acceso físico se administra mediante dos abstracciones:

1. **Credencial de Almacenamiento (*Storage Credential*):** Encapsula un rol de AWS IAM o una Identidad Administrada de Azure que posee permisos en el almacenamiento cloud.
2. **Ubicación Externa (*External Location*):** Vincula una ruta URL de almacenamiento específica (ej. `s3://corp-lakehouse-prod/finanzas/`) con una credencial de almacenamiento autorizada.

En entornos de producción, estas entidades se aprovisionan de forma declarativa mediante **Terraform** utilizando el proveedor oficial `databricks`:

```hcl
# Aprovisionamiento de Storage Credential y External Location en Terraform
resource "databricks_storage_credential" "credencial_aws" {
  name = "credencial_iam_finanzas"
  aws_iam_role {
    role_arn = "arn:aws:iam::123456789012:role/UnityCatalogStorageRole"
  }
  comment = "Credencial IAM delegada para el lago de finanzas"
}

resource "databricks_external_location" "ubicacion_finanzas" {
  name            = "ext_loc_finanzas"
  url             = "s3://corp-lakehouse-prod/finanzas/"
  credential_name = databricks_storage_credential.credencial_aws.id
  comment         = "Ubicacion externa para datos de finanzas"
}
```

Equivalente en sintaxis DDL de SQL:

```sql
-- Declaración de credencial de almacenamiento en SQL nativo
CREATE STORAGE CREDENTIAL credencial_iam_finanzas
  AWS_IAM_ROLE (role_arn = 'arn:aws:iam::123456789012:role/UnityCatalogStorageRole');

CREATE EXTERNAL LOCATION ext_loc_finanzas
  URL 's3://corp-lakehouse-prod/finanzas/'
  WITH STORAGE CREDENTIAL credencial_iam_finanzas;
```

---

## 19.3 Control de Acceso con Privilegios Modernos: `USE CATALOG` y `USE SCHEMA`

En la especificación contemporánea de Unity Catalog, la navegación por la jerarquía se gobierna explícitamente mediante los privilegios `USE CATALOG` y `USE SCHEMA`:

```sql
-- 1. Contexto de ejecución
USE CATALOG prod_lakehouse;
CREATE SCHEMA IF NOT EXISTS contabilidad;

-- 2. Otorgar permisos de navegación jerárquica y lectura a los analistas
GRANT USE CATALOG ON CATALOG prod_lakehouse TO `grupo_analistas_riesgo`;
GRANT USE SCHEMA ON SCHEMA prod_lakehouse.contabilidad TO `grupo_analistas_riesgo`;
GRANT SELECT ON TABLE prod_lakehouse.contabilidad.transacciones_cuentas TO `grupo_analistas_riesgo`;

-- 3. Otorgar permisos de escritura a los pipelines de ingeniería
GRANT MODIFY, SELECT ON SCHEMA prod_lakehouse.contabilidad TO `service_principal_etl`;
```

---

## 19.4 Seguridad Avanzada: Row Filters y Column Masking con Tipado Estricto

Para cumplir normativas regulatorias internacionales (RGPD, HIPAA, PCI-DSS) sin duplicar datos en tablas separadas, Unity Catalog proporciona **enmascaramiento dinámico de columnas (*Column Masking*)** y **filtros a nivel de fila (*Row Filters*)**:

### Regla Fundamental de Enmascaramiento de Columnas
La función UDF que aplica la máscara **debe declarar explícitamente la cláusula `RETURNS <tipo>` y debe devolver exactamente el mismo tipo de datos primitivo de la columna que enmascara**. Si una columna es `DOUBLE`, la función debe devolver `DOUBLE` (por ejemplo, `NULL` o `0.0`); jamás puede devolver un `STRING` (`'CONFIDENCIAL'`), ya que causaría un error fatal de incompatibilidad de tipos durante la compilación de la consulta.

```sql
-- 1. Enmascaramiento dinámico para columna de tipo DOUBLE (Salarios)
CREATE OR REPLACE FUNCTION prod_lakehouse.contabilidad.mascara_salario_double(salario_val DOUBLE)
RETURNS DOUBLE
RETURN CASE 
    -- Solo miembros de gerencia ven el salario real
    WHEN is_account_group_member('gerentes_rrhh') THEN salario_val
    -- Los demás usuarios reciben NULL respetando el tipo numérico
    ELSE NULL
END;

ALTER TABLE prod_lakehouse.contabilidad.empleados
ALTER COLUMN salario_base_usd SET MASK prod_lakehouse.contabilidad.mascara_salario_double;

-- 2. Enmascaramiento dinámico para columna STRING (Número de Tarjeta)
CREATE OR REPLACE FUNCTION prod_lakehouse.contabilidad.mascara_pan_tarjeta(pan STRING)
RETURNS STRING
RETURN CASE 
    WHEN is_account_group_member('oficiales_cumplimiento') THEN pan
    ELSE concat('XXXX-XXXX-XXXX-', right(pan, 4))
END;

ALTER TABLE prod_lakehouse.contabilidad.transacciones_cuentas
ALTER COLUMN numero_tarjeta SET MASK prod_lakehouse.contabilidad.mascara_pan_tarjeta;

-- 3. Filtro a nivel de fila (Row Filter) para segregación por país
CREATE OR REPLACE FUNCTION prod_lakehouse.contabilidad.filtro_pais_empleado(pais STRING)
RETURNS BOOLEAN
RETURN CASE 
    WHEN is_account_group_member('auditores_globales') THEN true
    WHEN is_account_group_member('analistas_colombia') AND pais = 'CO' THEN true
    WHEN is_account_group_member('analistas_mexico') AND pais = 'MX' THEN true
    ELSE false
END;

ALTER TABLE prod_lakehouse.contabilidad.empleados
SET ROW FILTER prod_lakehouse.contabilidad.filtro_pais_empleado ON (pais);
```

---

## 19.5 Ciclo de Vida: Tablas Gestionadas vs. Externas y Recuperación con `UNDROP`

* **Tablas Gestionadas (*Managed Tables*):** Unity Catalog gobierna tanto los metadatos como el almacenamiento físico. Si un usuario autorizado ejecuta `DROP TABLE prod_lakehouse.contabilidad.empleados`, la tabla **no se destruye de inmediato físicamente**. Unity Catalog aplica un borrado suave con un **periodo de retención de 7 días**, permitiendo recuperarla intacta mediante:
  ```sql
  UNDROP TABLE prod_lakehouse.contabilidad.empleados;
  ```
  Una vez transcurridos los 7 días, un proceso de limpieza en segundo plano purga permanentemente los archivos en el almacenamiento cloud.
* **Tablas Externas (*External Tables*):** La tabla apunta a una ruta propia en un bucket mediante una *External Location*. Al ejecutar `DROP TABLE`, Unity Catalog elimina exclusivamente el registro de metadatos en el catálogo; **los archivos Parquet/Delta en el bucket cloud permanecen intactos**.

---

## 19.6 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Emplear Claves Secretas Hardcodeadas en Código de Pipelines**
Configurar cadenas de conexión directas o llaves IAM dentro de scripts elude completamente el motor de gobernanza. Todo acceso en producción debe canalizarse mediante credenciales y ubicaciones externas administradas por el catálogo.

**Anti-patrón 2: Catálogo Monolítico Único Sin Segregación de Entornos**
Crear un único catálogo denominado `empresa` y mezclar esquemas de pruebas (`test_ventas`) con producción (`prod_ventas`) en el mismo metastore sin permisos segregados facilita que errores accidentales sobrescriban tablas productivas. El estándar corporativo exige catálogos aislados: `dev`, `staging` y `prod`.

**Anti-patrón 3: Intentar Cambiar Tipos de Datos Dentro de Funciones de Enmascaramiento**
Intentar devolver una cadena de texto descriptiva (`'OCULTO'`) en una columna numérica viola el contrato de tipos de la tabla. Si los usuarios de negocio requieren una representación textual, debe construirse una vista semántica derivada sobre la tabla gobernada.

---

## Preguntas de Revisión y Ejercicios Prácticos
**Ejercicio 19.1 (Jerarquía de Privilegios en Tres Niveles):**  
Un analista junior intenta ejecutar `SELECT * FROM prod_lakehouse.contabilidad.reporte_diario`. El administrador le otorgó `GRANT SELECT ON TABLE prod_lakehouse.contabilidad.reporte_diario TO junior_user;`. Sin embargo, la consulta falla indicando un error de permisos denegados. ¿Por qué ocurre esto y qué sentencias SQL adicionales deben ejecutarse?

**Solución 19.1:**
En el modelo jerárquico de Unity Catalog, la autorización requiere permisos de navegación en **todos los niveles superiores** de la jerarquía antes de poder acceder al objeto final:
1. El usuario necesita permiso para atravesar el catálogo (`USE CATALOG`).
2. El usuario necesita permiso para atravesar el esquema (`USE SCHEMA`).

Sentencias requeridas para corregir el acceso:
```sql
GRANT USE CATALOG ON CATALOG prod_lakehouse TO `junior_user`;
GRANT USE SCHEMA ON SCHEMA prod_lakehouse.contabilidad TO `junior_user`;
```

---

**Ejercicio 19.2 (Diseño de Enmascaramiento Dinámico con Coherencia de Tipos):**  
Se requiere gobernar una tabla `prod_rrhh.empleados.nomina` que almacena `empleado_id STRING`, `nombre STRING`, `pais STRING` y `salario_base_usd DOUBLE`.
1. Escribe la función de enmascaramiento SQL formal respetando la coherencia estricta de tipos de datos, asegurando que los miembros del grupo `gerentes_rrhh` vean el salario real y todos los demás usuarios vean `NULL`.
2. Asocia la máscara a la columna correspondiente de la tabla.

**Solución 19.2:**
```sql
-- 1. Definición formal de la función respetando RETURNS DOUBLE
CREATE OR REPLACE FUNCTION prod_rrhh.empleados.mascara_salario_confidencial(salario_val DOUBLE)
RETURNS DOUBLE
RETURN CASE 
    WHEN is_account_group_member('gerentes_rrhh') THEN salario_val
    ELSE NULL
END;

-- 2. Aplicación de la máscara a la columna de tipo DOUBLE
ALTER TABLE prod_rrhh.empleados.nomina
ALTER COLUMN salario_base_usd SET MASK prod_rrhh.empleados.mascara_salario_confidencial;
```
*Fundamento:* Al declarar `RETURNS DOUBLE` y devolver `salario_val` o `NULL`, la consulta del usuario conserva la integridad del esquema analítico (la columna continúa siendo `DOUBLE`), evitando excepciones de tipo en herramientas de BI conectadas.

---

## 19.7 Resumen y Puntos Clave

* **Gobernanza unificada:** Unity Catalog unifica la administración de permisos, auditoría y linaje para tablas, archivos y modelos de machine learning sobre múltiples nubes y motores.
* **Metastores regionales:** Cada metastore reside en una región cloud específica y se asocia a los recursos de esa misma región para evitar sobrecostos de transferencia.
* **Privilegios modernos:** Se emplean `USE CATALOG` y `USE SCHEMA` para gobernar la navegación jerárquica obligatoria antes de conceder accesos granulares de lectura o modificación.
* **Enmascaramiento tipado:** Las funciones de enmascaramiento dinámico deben preservar estrictamente el tipo de dato de la columna protegida (`RETURNS <tipo>`).
* **Protección ante borrado accidental:** Las tablas gestionadas implementan un periodo de recuperación de 7 días mediante el comando `UNDROP TABLE`.

---

## Lecturas Recomendadas
1. **Databricks Engineering Documentation (2024).** *Unity Catalog Best Practices, Architecture and Privilege Model.* (`https://docs.databricks.com/data-governance/unity-catalog/index.html`).
2. **Linux Foundation (2024).** *Unity Catalog: Open Source Multi-Engine Data and AI Governance Specification.* Linux Foundation Projects (`https://unitycatalog.io/`).
3. **DAMA International (2017).** *DAMA-DMBOK: Data Management Body of Knowledge (2nd Edition).* Technics Publications.
