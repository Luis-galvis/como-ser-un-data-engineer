# Cómo ser un Data Engineer
### *El tratado integral y roadmap definitivo: de fundamentos a la empleabilidad internacional*

[![CI Pipeline Reproducible](https://github.com/Luis-galvis/como-ser-un-data-engineer/actions/workflows/ci.yml/badge.svg)](https://github.com/Luis-galvis/como-ser-un-data-engineer/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![Docker Compose v2](https://img.shields.io/badge/docker--compose-v2%2B-2496ED.svg)](https://docs.docker.com/compose/)
[![Tests Passing](https://img.shields.io/badge/tests-passing-brightgreen.svg)](tests/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Este es el repositorio acompañante oficial del libro **«Cómo ser un Data Engineer»** por Luis Galvis. Contiene el código fuente completo, el laboratorio local contenerizado, generadores deterministas de datos, datasets de estudio de mercado reales y la suite automatizada de pruebas para garantizar reproducibilidad absoluta.

---

## 📚 Estructura de Ediciones del Libro

El material está diseñado para acompañar tanto a principiantes como a ingenieros en transición hacia niveles avanzados:

| Edición | Alcance | Extensión | Archivo Compilado |
| :--- | :--- | :--- | :--- |
| **Tratado Completo** | 50 Capítulos (Cimientos $\to$ Lakehouse $\to$ Streaming $\to$ DataOps $\to$ System Design $\to$ Empleo) | 545 páginas | [`libro/libro-digital.pdf`](libro/libro-digital.pdf) |
| **Edición Núcleo (Fast-Track)** | 23 Capítulos clasificados como `[NÚCLEO EMPLEABILIDAD]` (Vía rápida a primer empleo) | ~260 páginas | [`libro/libro-edicion-nucleo.pdf`](libro/libro-edicion-nucleo.pdf) |

Todos los capítulos marcan explícitamente su nivel de prioridad para optimizar el tiempo de estudio:
* `[NÚCLEO EMPLEABILIDAD]`: Temas obligatorios que filtran en entrevistas técnicas (SQL, Python, Spark, Delta Lake, Airflow, dbt, Portafolio).
* `[RECOMENDADO]`: Herramientas y patrones que diferencian perfiles competitivos (Kafka, CI/CD, FinOps, Data Contracts).
* `[AVANZADO]`: Arquitecturas complejas para escalabilidad senior (System Design masivo, mTLS, Gobernanza, Datos para IA).

---

## 🏛️ Arquitectura del Laboratorio Local

El laboratorio recrea una plataforma de datos moderna de grado de producción en una sola máquina mediante Docker Compose:

```mermaid
graph TD
    subgraph Local_Lab ["ENTORNO LOCAL DE INGENIERÍA DE DATOS"]
        PG["PostgreSQL 16<br/>(Base Relacional OLTP)<br/>Puerto 5432"]
        MINIO["MinIO Object Storage<br/>(Data Lakehouse S3 Local)<br/>Puertos 9000 (API) / 9001 (UI)"]
        SPARK_M["Apache Spark 3.5.3 Master<br/>Puertos 8080 (UI) / 7077 (RPC)"]
        SPARK_W["Apache Spark 3.5.3 Worker<br/>(2 Cores, 2 GB RAM)"]
        DUCK["DuckDB In-Process OLAP<br/>(Consultas analíticas ultra-rápidas)"]
    end

    INIT["minio-init (Helper)<br/>Crea buckets: landing, bronze, silver, gold"] --> MINIO
    SPARK_M --> SPARK_W
    PG -->|Extracción CDC / JDBC| SPARK_M
    SPARK_M -->|Escritura Delta Lake / Parquet| MINIO
    DUCK -->|Lectura directa S3 / Parquet| MINIO

    style Local_Lab fill:#fbf9f4,stroke:#4a5160,stroke-width:1px
    style PG fill:#ffffff,stroke:#1b2030,stroke-width:2px
    style MINIO fill:#ffffff,stroke:#d9534f,stroke-width:2px
    style SPARK_M fill:#ffffff,stroke:#e67e22,stroke-width:2px
    style SPARK_W fill:#ffffff,stroke:#e67e22,stroke-width:2px
    style DUCK fill:#ffffff,stroke:#f1c40f,stroke-width:2px
```

---

## 🚀 Inicio Rápido (3 Comandos)

### 1. Clonar el repositorio y configurar variables de entorno
```bash
git clone https://github.com/Luis-galvis/como-ser-un-data-engineer.git
cd como-ser-un-data-engineer
cp .env.example .env
```

### 2. Levantar los servicios del laboratorio local
```bash
docker compose up -d
```
* Consola Web de MinIO: [http://localhost:9001](http://localhost:9001) (Usuario: `admin_minio`, Contraseña: `minio_secret_key_2026`)
* Web UI de Spark Master: [http://localhost:8080](http://localhost:8080)
* PostgreSQL: `localhost:5432` (Base: `data_engineer_db`)

### 3. Instalar dependencias y generar los datasets oficiales
```bash
# Con uv (recomendado):
uv pip install -e ".[dev]"
python scripts/generar_datos.py
pytest -v tests/

# O con pip estándar:
pip install -e ".[dev]"
python scripts/generar_datos.py
pytest -v tests/
```

---

## 📊 Estudio de Mercado Real: 50 Ofertas de Empleo Verificadas

A diferencia de guías que presentan porcentajes sin soporte, el libro fundamenta todas sus recomendaciones en un análisis empírico de **50 avisos de empleo de Data Engineer relevados en el primer trimestre de 2026** (Colombia, LATAM y Posiciones Remotas internacionales pagadas en USD):

* 📁 **Dataset Completo:** [`data/estudio_mercado_50_ofertas_2026.csv`](data/estudio_mercado_50_ofertas_2026.csv)

### Frecuencias Verificadas en el Mercado:
* **SQL:** **100%** (50/50 avisos) — El filtro absoluto y no negociable de la industria.
* **Python:** **96%** (48/50 avisos) — El lenguaje de programación predeterminado para ingesta y orquestación.
* **Apache Airflow:** **90%** (45/50 avisos) — El estándar corporativo de orquestación de flujos de trabajo.
* **Docker / Linux:** **88%** (44/50 avisos) — Base operativa y reproducibilidad en contenedores.
* **Apache Spark / PySpark:** **84%** (42/50 avisos) — Motor de cómputo para grandes volúmenes de datos.
* **Databricks o Snowflake:** **60%** (30/50 avisos) — Plataformas cloud gestionadas dominantes.
* **dbt (Data Build Tool):** **46%** (23/50 avisos) — Crecimiento exponencial en modelado y analítica moderna.
* **Inglés Conversacional B2+:** **44%** (22/50 avisos) — Obligatorio para multiplicar salarios y acceder a vacantes en USD ($2.500 - $6.500 USD/mes).

### ¿Qué Nube Aprender Primero?
1. **AWS (Amazon Web Services) — 52% de ofertas:** Elección predeterminada si su objetivo son startups de alto crecimiento y posiciones remotas globales en USD (S3, Glue, EMR, Athena).
2. **Microsoft Azure — 20% de ofertas:** Recomendada si busca insertarse en el sector financiero corporativo, banca tradicional o grandes empresas de Colombia y México (Azure Data Factory, Fabric, Synapse).
3. **Google Cloud (GCP) — 16% de ofertas:** Fuerte adopción en scale-ups tecnológicas de comercio electrónico y analítica masiva (BigQuery, Cloud Composer).

---

## 🧪 Código que se Ejecuta Solo (CI/CD)

Este repositorio cuenta con un pipeline de integración continua en **GitHub Actions** (`.github/workflows/ci.yml`) que se ejecuta en cada *commit* o *pull request* con versiones de paquetes fijadas:
* Valida estilo y calidad de código con `ruff`.
* Ejecuta el script determinista de generación de datos sintéticos `scripts/generar_datos.py`.
* Corre la suite de pruebas unitarias con `pytest` validando la integridad del CSV de mercado, consultas DuckDB en memoria, contratos de datos con Pydantic y lógica de idempotencia.

---

## 📂 Estructura del Repositorio

```
como-ser-un-data-engineer/
├── .github/
│   └── workflows/
│       └── ci.yml                        # GitHub Actions: tests, linting y datos automáticos
├── build/                                # Scripts de compilación del libro (Markdown -> HTML -> PDF)
│   ├── book.config.js                    # Metadatos editoriales
│   ├── build.js                          # Compilador de la Edición Completa (545 págs)
│   └── build_nucleo.js                   # Compilador de la Edición Núcleo (266 págs)
├── data/
│   └── estudio_mercado_50_ofertas_2026.csv # CSV oficial con 50 avisos reales de 2026
├── libro/
│   ├── capitulos/                        # Los 50 capítulos en Markdown con código completo
│   ├── libro-digital.pdf                 # PDF de la Edición Completa
│   └── libro-edicion-nucleo.pdf          # PDF de la Edición Núcleo
├── scripts/
│   └── generar_datos.py                  # Generador reproducible de los 8 datasets del libro
├── tests/
│   └── test_pipeline_reproducible.py     # Suite automatizada con pytest
├── .env.example                          # Plantilla segura de variables de entorno
├── docker-compose.yml                    # Entorno multicontenedor (Postgres, MinIO, Spark)
├── pyproject.toml                        # Configuración de dependencias modernas con uv
└── README.md                             # Documentación principal
```

---

## ⚖️ Aviso Legal y Disclaimer Tributario

Los datos salariales, cálculos de parafiscales, aportes a seguridad social (PILA, IBC) y regímenes contractuales citados en el Capítulo 36 tienen fines exclusivamente pedagógicos e informativos con base en la normativa colombiana vigente (Ley 100 de 1993, Ley 1122 de 2007, Ley 1955 de 2019 art. 244, Decreto 1273 de 2018). **No constituyen asesoría tributaria, laboral ni financiera.** Se recomienda validar cada situación particular con un contador público certificado.

---

## 📄 Licencia

El código fuente, scripts de laboratorio y archivos de configuración están licenciados bajo la [Licencia MIT](LICENSE).  
El texto, diagramas y contenido pedagógico de los capítulos están protegidos bajo derechos de autor © 2026 Luis Galvis.
