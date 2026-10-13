#!/usr/bin/env python3
"""
scripts/generar_datos.py
Generador integral y determinista de datasets reproducibles para el libro 'Cómo ser un Data Engineer'.
Genera la totalidad de los datos requeridos por los capítulos:
  - data/ventas.parquet: Transacciones comerciales analíticas (Cap. 1-17, 27-28)
  - data/clientes.parquet: Catálogo dimensional de clientes (Cap. 1, 17, 34)
  - data/eventos_telemetria.json: Telemetría semiestructurada JSON (Cap. 7, 21, 32)
  - data/interacciones.parquet: Clickstream y sesiones para joins masivos (Cap. 12, 13)
  - data/usuarios.parquet: Información de usuarios con PII para CDC y GDPR (Cap. 22, 31)
  - data/pedidos.parquet: Transacciones relacionales para indexación B-Tree (Cap. 0B)
  - data/eventos_anuales.parquet: Datos multianuales para particionamiento temporal (Cap. 16, 29)
  - data/transacciones_sesgadas.parquet: Dataset con distribución sesgada (Zipf/Pareto) para provocar Skew y Shuffle Spill en Spark UI (Cap. 16, 27)
"""

import datetime
import json
import os
import random
import sys


def generar_datasets():
    print("=================================================================")
    print(" Generando suite completa de datasets para el libro 'Cómo ser un Data Engineer'")
    print("=================================================================")
    random.seed(42)

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("ERROR: PyArrow es requerido para generar los archivos Parquet oficiales.")
        print("Por favor instale PyArrow con: uv pip install pyarrow (o pip install pyarrow)")
        sys.exit(1)

    paises = ["CO", "MX", "CL", "PE", "AR", "BR", "ES", "US"]
    categorias = ["ELECTRONICA", "HOGAR", "MODA", "SERVICIOS", "ALIMENTOS", "DEPORTES"]
    estados = ["COMPLETADO", "PENDIENTE", "CANCELADO", "RECHAZADO"]
    fecha_base = datetime.date(2025, 1, 1)

    # 1. data/ventas.parquet (10,000 registros)
    print("1/8. Generando data/ventas.parquet (10,000 registros)...")
    transacciones = []
    for i in range(1, 10001):
        fecha = fecha_base + datetime.timedelta(days=random.randint(0, 400))
        cliente_id = random.randint(100, 1500)
        cantidad = random.randint(1, 10)
        precio = round(random.uniform(5.0, 500.0), 2)
        transacciones.append({
            "transaccion_id": f"TX-{i:07d}",
            "cliente_id": cliente_id,
            "pais": random.choice(paises),
            "categoria_producto": random.choice(categorias),
            "estado_pago": random.choices(estados, weights=[0.80, 0.10, 0.05, 0.05])[0],
            "fecha_transaccion": fecha,
            "cantidad": cantidad,
            "precio_unitario": precio,
            "monto_usd": round(cantidad * precio, 2)
        })
    pq.write_table(pa.Table.from_pylist(transacciones), os.path.join(data_dir, "ventas.parquet"), compression="snappy")
    print("     [OK] data/ventas.parquet")

    # 2. data/clientes.parquet (1,500 clientes)
    print("2/8. Generando data/clientes.parquet (1,500 clientes)...")
    nombres = ["Ana", "Carlos", "Elena", "David", "Sofia", "Jorge", "Lucia", "Mateo", "Valentina", "Andres"]
    apellidos = ["Gomez", "Rodriguez", "Lopez", "Martinez", "Perez", "Garcia", "Silva", "Sanchez", "Ramirez", "Torres"]
    segmentos = ["ESTANDAR", "PRO", "ENTERPRISE", "CORPORATIVO"]

    clientes = []
    for cid in range(100, 1600):
        nom = f"{random.choice(nombres)} {random.choice(apellidos)}"
        clientes.append({
            "cliente_id": cid,
            "nombre": nom,
            "correo": f"cliente_{cid}@{random.choice(['empresa.com', 'correo.co', 'mail.org'])}",
            "pais_iso": random.choice(paises),
            "segmento": random.choice(segmentos),
            "fecha_registro": fecha_base + datetime.timedelta(days=random.randint(0, 300))
        })
    pq.write_table(pa.Table.from_pylist(clientes), os.path.join(data_dir, "clientes.parquet"), compression="snappy")
    print("     [OK] data/clientes.parquet")

    # 3. data/eventos_telemetria.json (1,000 eventos anidados)
    print("3/8. Generando data/eventos_telemetria.json (1,000 eventos)...")
    eventos = []
    for i in range(1, 1001):
        eventos.append({
            "evento_id": f"EVT-{i:06d}",
            "usuario_id": random.randint(100, 1500),
            "tipo": random.choice(["CLICK", "VIEW", "PURCHASE", "LOGOUT", "SEARCH"]),
            "timestamp": (datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(minutes=i * 3)).isoformat(),
            "payload": {
                "ip": f"192.168.{random.randint(1, 10)}.{random.randint(1, 254)}",
                "dispositivo": random.choice(["Android", "iOS", "Windows", "macOS"]),
                "duracion_segundos": random.randint(5, 600)
            }
        })
    with open(os.path.join(data_dir, "eventos_telemetria.json"), "w", encoding="utf-8") as f:
        json.dump(eventos, f, indent=2)
    print("     [OK] data/eventos_telemetria.json")

    # 4. data/interacciones.parquet (25,000 eventos de clickstream)
    print("4/8. Generando data/interacciones.parquet (25,000 eventos)...")
    interacciones = []
    paginas = ["/home", "/productos", "/carrito", "/checkout", "/soporte", "/blog"]
    for i in range(1, 25001):
        interacciones.append({
            "interaccion_id": f"INT-{i:08d}",
            "usuario_id": random.randint(100, 1500),
            "url_pagina": random.choice(paginas),
            "tiempo_estadia_ms": random.randint(200, 45000),
            "fecha_evento": fecha_base + datetime.timedelta(days=random.randint(0, 365))
        })
    pq.write_table(pa.Table.from_pylist(interacciones), os.path.join(data_dir, "interacciones.parquet"), compression="snappy")
    print("     [OK] data/interacciones.parquet")

    # 5. data/usuarios.parquet (2,000 usuarios con campos PII para CDC y Ley 1581)
    print("5/8. Generando data/usuarios.parquet (2,000 usuarios con PII)...")
    usuarios = []
    for uid in range(100, 2100):
        usuarios.append({
            "usuario_id": uid,
            "nombre_completo": f"{random.choice(nombres)} {random.choice(apellidos)}",
            "documento_identidad": f"CC-{random.randint(10000000, 99999999)}",
            "telefono": f"+57 3{random.randint(10, 25)} {random.randint(1000000, 9999999)}",
            "correo": f"usuario_{uid}@proveedor.com",
            "consentimiento_ley1581": random.choice([True, True, True, False]),
            "fecha_actualizacion": fecha_base + datetime.timedelta(days=random.randint(0, 400))
        })
    pq.write_table(pa.Table.from_pylist(usuarios), os.path.join(data_dir, "usuarios.parquet"), compression="snappy")
    print("     [OK] data/usuarios.parquet")

    # 6. data/pedidos.parquet (20,000 registros para indexación relacional en Cap. 0B)
    print("6/8. Generando data/pedidos.parquet (20,000 pedidos)...")
    pedidos = []
    for pid in range(1, 20001):
        fecha_pedido = fecha_base + datetime.timedelta(days=random.randint(0, 365))
        pedidos.append({
            "pedido_id": pid,
            "cliente_id": random.randint(100, 5000),
            "fecha_pedido": fecha_pedido,
            "estado": random.choice(["PENDIENTE", "PROCESADO", "ENVIADO", "ENTREGADO"]),
            "monto_total": round(random.uniform(10.0, 2500.0), 2)
        })
    pq.write_table(pa.Table.from_pylist(pedidos), os.path.join(data_dir, "pedidos.parquet"), compression="snappy")
    print("     [OK] data/pedidos.parquet")

    # 7. data/eventos_anuales.parquet (50,000 registros multianuales para particionamiento)
    print("7/8. Generando data/eventos_anuales.parquet (50,000 registros)...")
    eventos_anuales = []
    for eid in range(1, 50001):
        anio = random.choice([2024, 2025, 2026])
        mes = random.randint(1, 12)
        dia = random.randint(1, 28)
        eventos_anuales.append({
            "evento_id": f"EA-{eid:08d}",
            "anio": anio,
            "mes": mes,
            "fecha_evento": datetime.date(anio, mes, dia),
            "tipo_evento": random.choice(["LOGIN", "TRANSACCION", "RECLAMO", "RENOVACION"]),
            "valor": round(random.uniform(1.0, 1000.0), 2)
        })
    pq.write_table(pa.Table.from_pylist(eventos_anuales), os.path.join(data_dir, "eventos_anuales.parquet"), compression="snappy")
    print("     [OK] data/eventos_anuales.parquet")

    # 8. data/transacciones_sesgadas.parquet (100,000 registros con Data Skew agudo)
    # 85% de los registros asignados a cliente_id = 9999 o pais = 'CO' para forzar spill en Spark
    print("8/8. Generando data/transacciones_sesgadas.parquet (100,000 registros con Data Skew)...")
    sesgadas = []
    for sid in range(1, 100001):
        # Distribución con sesgo agudo (85% concentrado en dos valores clave)
        if random.random() < 0.85:
            cid = 9999
            pais = "CO"
        else:
            cid = random.randint(100, 2000)
            pais = random.choice(["US", "ES", "MX", "CL", "BR", "PE"])

        sesgadas.append({
            "transaccion_id": f"SKEW-{sid:08d}",
            "cliente_id": cid,
            "pais": pais,
            "monto": round(random.uniform(5.0, 1000.0), 2),
            "fecha": fecha_base + datetime.timedelta(days=random.randint(0, 365)),
            "datos_payload_pesados": "x" * 256  # Aumenta huella de memoria para forzar Spill en RAM limitada
        })
    pq.write_table(pa.Table.from_pylist(sesgadas), os.path.join(data_dir, "transacciones_sesgadas.parquet"), compression="snappy")
    print("     [OK] data/transacciones_sesgadas.parquet")

    print("\n=================================================================")
    print(" [LISTO] Todos los datasets de laboratorio han sido generados exitosamente en:")
    print("        ", data_dir)
    print("=================================================================")

if __name__ == "__main__":
    generar_datasets()
