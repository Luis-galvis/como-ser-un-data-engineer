"""
tests/test_pipeline_reproducible.py
Suite de pruebas automatizadas y reproducibles para el código y datos
del libro 'Cómo ser un Data Engineer'.
Ejecutado automáticamente por GitHub Actions CI.
"""

import csv
import os

from pydantic import BaseModel, Field, field_validator

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_PATH = os.path.join(DATA_DIR, "estudio_mercado_50_ofertas_2026.csv")


# =============================================================================
# 1. VERIFICACIÓN DEL ESTUDIO DE MERCADO REAL (50 OFERTAS 2026)
# =============================================================================

def test_mercado_csv_existencia_y_volumen():
    """Valida que el archivo CSV del estudio de mercado exista y contenga exactamente 50 avisos."""
    assert os.path.exists(CSV_PATH), f"No se encontró el archivo: {CSV_PATH}"

    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        filas = list(reader)

    assert len(filas) == 50, f"Se esperaban exactamente 50 ofertas, pero se encontraron {len(filas)}."


def test_mercado_csv_integridad_columnas_y_fechas():
    """Valida que no existan IDs duplicados y que todas las ofertas tengan fuentes y fechas válidas en 2026."""
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        filas = list(reader)

    ids = set()
    for fila in filas:
        aviso_id = int(fila["id"])
        assert aviso_id not in ids, f"ID de aviso duplicado: {aviso_id}"
        ids.add(aviso_id)

        assert fila["empresa"].strip() != "", f"Empresa vacía en id {aviso_id}"
        assert fila["pais_modalidad"].strip() != "", f"País/modalidad vacío en id {aviso_id}"
        assert fila["fecha_relevamiento"].startswith("2026-"), f"Fecha no corresponde a 2026 en id {aviso_id}"
        assert fila["plataforma_fuente"].strip() != "", f"Fuente vacía en id {aviso_id}"


def test_mercado_porcentajes_frecuencias_verificadas():
    """Valida que las frecuencias calculadas coincidan matemáticamente con las publicadas en el libro."""
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        filas = list(reader)

    len(filas)
    sql_count = sum(1 for r in filas if r["sql_requerido"].strip().upper() == "TRUE")
    python_count = sum(1 for r in filas if r["python_requerido"].strip().upper() == "TRUE")
    airflow_count = sum(1 for r in filas if r["airflow_requerido"].strip().upper() == "TRUE")
    docker_count = sum(1 for r in filas if r["docker_linux"].strip().upper() == "TRUE")
    spark_count = sum(1 for r in filas if r["spark_requerido"].strip().upper() == "TRUE")
    dbt_count = sum(1 for r in filas if r["dbt_requerido"].strip().upper() == "TRUE")

    # Verificaciones exactas
    assert sql_count == 50, f"SQL debe ser 100% (50/50), encontrado: {sql_count}"
    assert python_count == 48, f"Python debe ser 96% (48/50), encontrado: {python_count}"
    assert airflow_count == 45, f"Airflow debe ser 90% (45/50), encontrado: {airflow_count}"
    assert docker_count == 44, f"Docker/Linux debe ser 88% (44/50), encontrado: {docker_count}"
    assert spark_count == 42, f"Spark debe ser 84% (42/50), encontrado: {spark_count}"
    assert dbt_count == 23, f"dbt debe ser 46% (23/50), encontrado: {dbt_count}"


# =============================================================================
# 2. PRUEBAS DE SQL ANALÍTICO Y DUCKDB (CAPÍTULOS 01, 02, 02A)
# =============================================================================

def test_duckdb_funciones_ventana_y_deduplicacion():
    """Valida la ejecución en DuckDB de funciones de ventana analíticas (ROW_NUMBER para deduplicación)."""
    import duckdb

    con = duckdb.connect(database=":memory:")

    # Crear datos con duplicados simulados
    con.execute("""
        CREATE TABLE eventos (
            id VARCHAR,
            cliente_id INT,
            version INT,
            monto DOUBLE
        );
        INSERT INTO eventos VALUES
            ('EVT-1', 101, 1, 100.0),
            ('EVT-1', 101, 2, 150.0),
            ('EVT-2', 102, 1, 300.0),
            ('EVT-3', 103, 1, 45.0),
            ('EVT-3', 103, 2, 90.0);
    """)

    # Deduplicar quedándonos con la última versión mediante ROW_NUMBER()
    resultado = con.execute("""
        WITH deduplicados AS (
            SELECT
                id,
                cliente_id,
                monto,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY version DESC) as rn
            FROM eventos
        )
        SELECT id, monto FROM deduplicados WHERE rn = 1 ORDER BY id;
    """).fetchall()

    assert len(resultado) == 3
    assert resultado[0] == ('EVT-1', 150.0)
    assert resultado[1] == ('EVT-2', 300.0)
    assert resultado[2] == ('EVT-3', 90.0)


# =============================================================================
# 3. PRUEBAS DE CONTRATOS DE DATOS Y CUARENTENA (CAPÍTULO 28)
# =============================================================================

class ContratoTransaccion(BaseModel):
    transaccion_id: str
    cliente_id: int
    monto_usd: float = Field(gt=0.0)
    pais_iso: str

    @field_validator("pais_iso")
    @classmethod
    def validar_pais(cls, v: str) -> str:
        paises_permitidos = {"CO", "MX", "CL", "PE", "US", "ES", "BR"}
        if v.upper() not in paises_permitidos:
            raise ValueError(f"Código de país {v} no admitido en el contrato.")
        return v.upper()


def test_contrato_datos_y_aislamiento_dead_letter_queue():
    """Valida que los registros válidos se procesen y los inválidos vayan a Dead-Letter Queue."""
    lote_entrante = [
        {"transaccion_id": "TX-001", "cliente_id": 100, "monto_usd": 150.50, "pais_iso": "CO"},
        {"transaccion_id": "TX-002", "cliente_id": 101, "monto_usd": -50.00, "pais_iso": "MX"},  # Inválido: monto negativo
        {"transaccion_id": "TX-003", "cliente_id": 102, "monto_usd": 200.00, "pais_iso": "XX"},  # Inválido: país desconocido
        {"transaccion_id": "TX-004", "cliente_id": 103, "monto_usd": 89.90, "pais_iso": "CL"},
    ]

    procesados = []
    dead_letter_queue = []

    for registro in lote_entrante:
        try:
            validado = ContratoTransaccion(**registro)
            procesados.append(validado.model_dump())
        except Exception as err:
            dead_letter_queue.append({
                "registro_crudo": registro,
                "motivo_rechazo": str(err)
            })

    assert len(procesados) == 2, "Deben aprobarse exactamente 2 transacciones válidas."
    assert len(dead_letter_queue) == 2, "Deben aislarse exactamente 2 registros corruptos."
    assert procesados[0]["transaccion_id"] == "TX-001"
    assert procesados[1]["transaccion_id"] == "TX-004"


# =============================================================================
# 4. PRUEBA DE IDEMPOTENCIA EN INGESTA
# =============================================================================

def test_idempotencia_upsert_en_memoria():
    """Valida que ejecutar la misma operación N veces no genere duplicados ni altere el estado final."""
    almacen_destino = {}

    def upsert_registros(registros):
        for reg in registros:
            # Clave primaria transaccion_id
            almacen_destino[reg["transaccion_id"]] = reg

    lote = [
        {"transaccion_id": "TX-100", "estado": "PENDIENTE", "monto": 50.0},
        {"transaccion_id": "TX-101", "estado": "COMPLETADO", "monto": 120.0},
    ]

    # Primera ejecución
    upsert_registros(lote)
    assert len(almacen_destino) == 2
    assert almacen_destino["TX-100"]["estado"] == "PENDIENTE"

    # Segunda ejecución idéntica (idempotencia pura)
    upsert_registros(lote)
    assert len(almacen_destino) == 2

    # Tercera ejecución con actualización de estado
    lote_actualizado = [
        {"transaccion_id": "TX-100", "estado": "COMPLETADO", "monto": 50.0},
    ]
    upsert_registros(lote_actualizado)
    assert len(almacen_destino) == 2
    assert almacen_destino["TX-100"]["estado"] == "COMPLETADO"
