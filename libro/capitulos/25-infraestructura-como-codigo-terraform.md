<!-- NOTAS FUENTE ORIGINALES DE OBSIDIAN: [[Terraform]], [[Azure Databricks IaC]], [[DataOps]] -->

# Capítulo 25: Infraestructura como Código (IaC) con Terraform y OpenTofu [RECOMENDADO]

> *"En la ingeniería de datos moderna, la infraestructura no se configura mediante interfaces gráficas (*ClickOps*); se escribe, se versiona en Git y se audita con el mismo rigor que el código de producción. La Infraestructura como Código (IaC) convierte el aprovisionamiento de clústeres, almacenes de objetos y políticas de seguridad en un proceso determinista, auditable y libre de error humano."*

---

## Objetivos de Aprendizaje
Al finalizar este capítulo, el lector será capaz de:
* Comprender la filosofía declarativa y el ciclo de vida de la Infraestructura como Código (**IaC**) con Terraform y OpenTofu.
* Gestionar con seguridad el archivo de estado (`terraform.tfstate`), implementando bloqueo concurrente (*State Locking*) en almacenamiento de objetos.
* Aprovisionar una arquitectura Lakehouse completa en **Azure** con `azurerm` v4 y autenticación Entra ID (`storage_use_azuread = true`), así como sus equivalentes en **AWS** (S3, IAM, KMS).
* Configurar recursos de gobernanza de Unity Catalog y orquestación de tareas mediante el proveedor oficial `databricks`.
* Proteger recursos analíticos críticos contra destrucción accidental utilizando reglas de ciclo de vida (`prevent_destroy`, `ignore_changes`).
* Refactorizar y adoptar infraestructura preexistente mediante bloques declarativos `import` y `moved`.
* Estructurar repositorios de infraestructura desacoplando módulos reutilizables y aislando ambientes de desarrollo y producción.

---

> **Definición 25.1 (Infraestructura como Código e Idempotencia)**
>
> La **Infraestructura como Código (IaC)** es la práctica de ingeniería de software que modela el aprovisionamiento, configuración y ciclo de vida de redes, cómputo y almacenamiento mediante archivos de configuración legibles por humanos y procesables por máquinas.
> 
> Su propiedad fundamental es la **idempotencia**: ejecutar la misma definición de IaC repetidas veces sobre una plataforma en la nube debe converger exactamente al mismo estado de infraestructura, sin crear duplicados ni generar efectos secundarios imprevistos.

---

## 25.1 Ciclo de Vida: Plan, Apply y State Locking

Terraform y OpenTofu emplean el lenguaje declarativo **HCL (HashiCorp Configuration Language)**. El flujo operativo se articula en tres comandos deterministas:

* **`terraform init`:** Inicializa el directorio de trabajo, descarga los binarios de los proveedores requeridos (`azurerm`, `aws`, `databricks`) y configura la conexión con el backend remoto.
* **`terraform plan`:** Realiza una lectura de solo lectura contra las APIs del proveedor en la nube, compara los recursos reales contra los manifiestos locales y proyecta el plan de ejecución exacto (recursos a crear `+`, modificar `~` o destruir `-`).
* **`terraform apply`:** Ejecuta las llamadas a las APIs de la nube respetando el orden topológico de dependencias y actualiza el archivo de estado persistente.

### Gestión Segura del Archivo de Estado y Bloqueo Concurrente
El archivo `terraform.tfstate` es el mapa de verdad de la infraestructura. Si se corrompe o dos ingenieros aplican cambios simultáneamente, los recursos en la nube pueden colisionar o ser destruidos accidentalmente.

* **Secretos en Texto Plano:** Contraseñas de bases de datos, claves de acceso generadas y certificados SSL residen sin cifrar dentro del JSON de `terraform.tfstate`. **Está terminantemente prohibido incluir el archivo de estado en repositorios de Git**.
* **Bloqueos de Estado (*State Locking*):**
  * **En Azure:** Se utiliza un contenedor de Azure Blob Storage que aprovecha el mecanismo de *Blob Leases* nativo para bloquear el archivo durante la ejecución de un plan o apply.
  * **En AWS S3 (Terraform 1.10+ / OpenTofu):** Históricamente se requería una tabla DynamoDB dedicada para gestionar bloqueos. En versiones modernas, S3 soporta bloqueo nativo directo declarando `use_lockfile = true` en el bloque de backend.

---

## 25.2 Despliegue de un Lakehouse en Azure con `azurerm` v4 y Unity Catalog

El siguiente manifiesto productivo despliega una cuenta de almacenamiento ADLS Gen2 con espacio de nombres jerárquico habilitado, los tres contenedores de la Arquitectura Medallón, un espacio de trabajo administrado de **Azure Databricks**, y aprovisiona el catálogo gobernado en **Unity Catalog** utilizando el proveedor `databricks`:

```hcl
# main.tf
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0" # Operador pesimista que permite versiones 4.x compatibles
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-terraform-mgmt-prod"
    storage_account_name = "sttfstatelakehouseprod"
    container_name       = "tfstate-core"
    key                  = "lakehouse.prod.tfstate"
  }
}

provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id
  # Requisito crítico en azurerm v4: permite gestionar sistemas de archivos mediante RBAC de Entra ID
  # evitando errores HTTP 403 cuando shared_access_key_enabled = false
  storage_use_azuread = true
}

# 1. Grupo de recursos principal para la plataforma analítica
resource "azurerm_resource_group" "rg_datos" {
  name     = "rg-lakehouse-prod-eastus2"
  location = "eastus2"
  tags = {
    Ambiente   = "Produccion"
    Disciplina = "DataEngineering"
    FinOps     = "CentroCostos-Analitica"
  }
}

# 2. Cuenta de almacenamiento para el Lakehouse con ADLS Gen2 (HNS activo)
resource "azurerm_storage_account" "lakehouse_storage" {
  name                     = "stlakehouseprodeus201"
  resource_group_name      = azurerm_resource_group.rg_datos.name
  location                 = azurerm_resource_group.rg_datos.location
  account_tier             = "Standard"
  account_replication_type = "ZRS" # Almacenamiento redundante de zona para alta disponibilidad
  is_hns_enabled           = true  # Hierarchical Namespace obligatorio para Lakehouse

  min_tls_version           = "TLS1_2"
  shared_access_key_enabled = false # Obliga a autenticación por Entra ID / RBAC

  blob_properties {
    delete_retention_policy {
      days = 14
    }
  }

  lifecycle {
    # Protección de datos críticos contra borrados accidentales
    prevent_destroy = true
    ignore_changes  = [tags]
  }

  tags = azurerm_resource_group.rg_datos.tags
}

# 3. Contenedores de almacenamiento para las capas Medallón
resource "azurerm_storage_data_lake_gen2_filesystem" "capas_medallon" {
  for_each           = toset(["bronze", "silver", "gold"])
  name               = each.key
  storage_account_id = azurerm_storage_account.lakehouse_storage.id
}

# 4. Workspace administrado de Azure Databricks (SKU Premium para Unity Catalog)
resource "azurerm_databricks_workspace" "workspace_databricks" {
  name                        = "dbw-lakehouse-prod-eastus2"
  resource_group_name         = azurerm_resource_group.rg_datos.name
  location                    = azurerm_resource_group.rg_datos.location
  sku                         = "premium"
  managed_resource_group_name = "rg-lakehouse-prod-managed-infra"

  tags = azurerm_resource_group.rg_datos.tags
}

# 5. Proveedor de Databricks vinculado dinámicamente al Workspace aprovisionado
provider "databricks" {
  host = azurerm_databricks_workspace.workspace_databricks.workspace_url
}

# 6. Credencial de almacenamiento y Ubicación Externa para Unity Catalog
# Unity Catalog requiere gobernar el acceso mediante Azure Managed Identity
resource "databricks_storage_credential" "external_creds" {
  name = "credencial_adls_prod"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.ext_access_connector.id
  }
  comment = "Credencial administrada de Azure para acceso seguro al Lakehouse"
}

resource "databricks_external_location" "gold_location" {
  name            = "ubicacion_externa_gold"
  url             = "abfss://${azurerm_storage_data_lake_gen2_filesystem.capas_medallon["gold"].name}@${azurerm_storage_account.adls_lakehouse.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.external_creds.id
  comment         = "Punto de montaje gobernado por Unity Catalog para capa Gold"
}

# 7. Catálogo gobernado en Unity Catalog con URL abfss:// válida
resource "databricks_catalog" "corp_catalog" {
  name         = "catalogo_corporativo_prod"
  comment      = "Catálogo principal gobernado con Unity Catalog"
  storage_root = databricks_external_location.gold_location.url
  depends_on   = [databricks_external_location.gold_location]
  
  lifecycle {
    prevent_destroy = true
  }
}
```

---

## 25.3 Equivalentes en AWS: S3, KMS e IAM con Mínimo Privilegio

En entornos sobre Amazon Web Services (AWS), la persistencia del Lakehouse y la seguridad en reposo se modelan utilizando `aws_s3_bucket`, claves administradas por el cliente en `aws_kms_key` y roles de mínimo privilegio:

```hcl
# lakehouse_aws.tf
resource "aws_kms_key" "lakehouse_kms" {
  description             = "Clave KMS para cifrado del Lakehouse Medallón"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_s3_bucket" "lakehouse_gold" {
  bucket = "empresa-lakehouse-gold-prod"

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold_encryption" {
  bucket = aws_s3_bucket.lakehouse_gold.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.lakehouse_kms.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "gold_block_public" {
  bucket = aws_s3_bucket.lakehouse_gold.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
```

---

## 25.4 Refactorización y Adopción: Bloques `import` y `moved`

En organizaciones en crecimiento es habitual tener recursos analíticos creados previamente por consola (*ClickOps*) o requerir reorganizar módulos sin provocar la destrucción de las bases de datos.

### 1. Bloque `import` Declarativo (Terraform 1.5+)
A diferencia del comando interactivo tradicional (`terraform import`), el bloque declarativo `import` permite auditar la adopción en el código y verificarla en el plan:

```hcl
import {
  to = azurerm_storage_account.lakehouse_storage
  id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-datos/providers/Microsoft.Storage/storageAccounts/stlakehouseprodeus201"
}
```

### 2. Bloque `moved` para Refactorización sin Destrucción
Si traslada un recurso desde la raíz a un submódulo o renombra su identificador, Terraform interpretaría por omisión que debe destruir el recurso antiguo y crear uno nuevo. El bloque `moved` actualiza el puntero de estado internamente:

```hcl
# Actualiza el estado sin tocar la infraestructura física en la nube
moved {
  from = azurerm_storage_account.st_legacy
  to   = azurerm_storage_account.lakehouse_storage
}
```

---

## 25.5 Estructuración de Proyectos y Segregación de Entornos

La arquitectura empresarial exige **segregación física de directorios y backends de estado independientes**:

```
infraestructura-datos/
├── modulos/
│   ├── lakehouse_storage/      # Módulo reutilizable de almacenamiento ADLS Gen2 / S3
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── databricks_platform/    # Módulo reutilizable de Unity Catalog y Jobs
│       ├── main.tf
│       └── variables.tf
└── entornos/
    ├── dev/
    │   ├── main.tf             # Consume módulos fijando variables de dev
    │   ├── backend.tf          # Estado en: dev.lakehouse.tfstate
    │   └── terraform.tfvars
    └── prod/
        ├── main.tf             # Consume módulos fijando alta redundancia y Premium
        ├── backend.tf          # Estado en: prod.lakehouse.tfstate (permisos restringidos)
        └── terraform.tfvars
```

---

## 25.6 Criterios de Fiabilidad y Anti-Patrones en Producción

**Anti-patrón 1: Permitir ClickOps en Paralelo con Terraform**  
Si un administrador modifica las políticas de red directamente en el portal de Azure o AWS sin actualizar el código HCL, la siguiente ejecución de `terraform apply` revertirá silenciosamente esas modificaciones manuales o fallará debido a divergencias de estado (*Configuration Drift*). La consola web debe ser de solo lectura en producción.

**Anti-patrón 2: Declarar Credenciales Secretas en Archivos `.tfvars` Versionados**  
Incrustar tokens de acceso o contraseñas en archivos de variables y subirlos a repositorios de Git expone credenciales a toda la organización. Utilice variables de entorno inyectadas en el runner de CI/CD (`TF_VAR_db_password`) o proveedores dedicados de secretos (Azure Key Vault, AWS Secrets Manager o HashiCorp Vault).

**Anti-patrón 3: Operador de Versiones Inadecuado en Proveedores**  
Declarar `version = "~> 4.0.0"` restringe rígidamente la descarga a parches de la versión 4.0.x, impidiendo acceder a mejoras y correcciones de la serie 4.1+. Por el contrario, declarar `version = ">= 4.0"` arriesga la descarga de una futura versión mayor 5.0 con cambios incompatibles. El estándar óptimo es `version = "~> 4.0"`.

---

## Preguntas de Revisión y Ejercicios Prácticos
**Ejercicio 25.1 (Diagnóstico de Desviación de Configuración):**  
Un ingeniero de datos modifica manualmente el firewall de la cuenta de almacenamiento en el portal de Azure para autorizar una IP externa. Tres días después, otro miembro del equipo ejecuta un despliegue con `terraform apply` para agregar un nuevo contenedor. 
1. ¿Qué detectará `terraform plan` al inspeccionar la cuenta de almacenamiento?
2. ¿Qué ocurrirá con la regla de firewall creada manualmente tras la ejecución de `terraform apply`?

**Solución 25.1:**
1. *Detección:* Durante la fase de actualización (*refresh*), Terraform consulta el estado real en la nube y detecta una discrepancia entre el archivo local de HCL (que no declara la regla de IP) y el recurso real. El plan marcará la regla manual con un símbolo de supresión (`-`) como un cambio no planificado derivado de *Configuration Drift*.
2. *Consecuencia:* Al confirmar `terraform apply`, Terraform restaurará el estado declarado en el código y **eliminará la regla de IP manual**, revirtiendo la cuenta de almacenamiento a su configuración original y bloqueando la IP que había sido autorizada por fuera del código.

---

**Ejercicio 25.2 (Diseño de Almacenamiento Jerárquico en ADLS Gen2):**  
¿Por qué es indispensable configurar `is_hns_enabled = true` en cuentas de almacenamiento de Azure destinadas a actuar como Lakehouse para Spark y Delta Lake? ¿Qué impacto de rendimiento y atomicidad tiene esta propiedad?

**Solución 25.2:**
1. *Estructura Jerárquica Real:* En los sistemas de almacenamiento de objetos tradicionales (como S3 o Blob Storage estándar), los directorios no existen físicamente; son prefijos virtuales en el nombre de la clave del objeto (`/capa/tabla/particion/archivo.parquet`).
2. *Rendimiento y Atomicidad:*
   * Sin HNS, una operación como renombrar un directorio o mover una tabla requiere copiar individualmente cada uno de los archivos que lo componen y luego borrar los originales (operación de orden $O(N)$ lenta y no atómica).
   * Con **Hierarchical Namespace (HNS activo)**, ADLS Gen2 implementa directorios reales compatibles con POSIX: un renombrado de directorio es un cambio atómico de puntero de metadatos de orden $O(1)$ que toma milisegundos, independientemente de si el directorio contiene 10 archivos o 500.000 archivos Parquet, evitando bloqueos e inconsistencias durante transacciones de Spark.

---

## 25.7 Resumen y Puntos Clave

* **Declaratividad e Idempotencia:** Terraform y OpenTofu permiten definir plataformas analíticas reproducibles en código HCL, garantizando que el estado real converja deterministamente con el estado deseado.
* **Seguridad del Estado:** El archivo `terraform.tfstate` debe residir en un backend remoto cifrado con control de acceso estricto y bloqueos de concurrencia nativos, jamás en un repositorio Git.
* **Integración Azure y Unity Catalog:** El proveedor `azurerm` v4 requiere `storage_use_azuread = true` para crear sistemas de archivos con claves de acceso deshabilitadas. El proveedor `databricks` complementa la infraestructura desplegando catálogos y esquemas en Unity Catalog.
* **Ciclo de vida y adopción:** `lifecycle { prevent_destroy = true }` protege datos contra eliminaciones accidentales, mientras que los bloques `import` y `moved` permiten incorporar infraestructura existente y refactorizar sin tiempo de inactividad.

---

## Lecturas Recomendadas
1. **Brikman, Y. (2022).** *Terraform: Up & Running: Writing Infrastructure as Code* (3rd ed.). O'Reilly Media.
2. **OpenTofu Project Technical Steering Committee (2024).** *OpenTofu Documentation and State Management Specification.* Linux Foundation (`https://opentofu.org/docs/`).
3. **Databricks Engineering (2024).** *Databricks Terraform Provider: Managing Unity Catalog and Lakehouse Infrastructure.* HashiCorp Registry & Databricks Documentation (`https://registry.terraform.io/providers/databricks/databricks/latest/docs`).
