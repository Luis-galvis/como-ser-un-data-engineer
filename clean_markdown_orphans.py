import os
import re

CAP_DIR = r'c:\Users\lgalv\Desktop\obsidian\libro\capitulos'
MIRROR_DIR = r'D:\estudio\libro\capitulos'

files = [f for f in sorted(os.listdir(CAP_DIR)) if f.endswith('.md')]

print(f"Limpiando lineas huerfanas de blockquote en {len(files)} archivos...")

for file in files:
    path = os.path.join(CAP_DIR, file)
    with open(path, 'r', encoding='utf-8') as fh:
        text = fh.read()

    # Reemplazar "> \n" o ">\n" por salto de linea dentro del bloque o limpiar
    # Si hay una linea que solo tiene ">", la unificamos para que el bloque sea continuo
    # Ejemplo:
    # > Texto 1
    # >
    # > Texto 2
    # Convertir a:
    # > Texto 1\n> Texto 2
    text = re.sub(r'\n>\s*\n>\s*', '\n> \n> ', text)

    # Eliminar cualquier ">" solitario que no tenga texto a continuacion
    text = re.sub(r'\n>\s*\n(?!\s*>)', '\n\n', text)

    # Quitar marcas [FALTA] si quedan en los archivos
    text = text.replace('[FALTA]', '').strip() + '\n'

    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(text)

    if os.path.exists(MIRROR_DIR):
        with open(os.path.join(MIRROR_DIR, file), 'w', encoding='utf-8') as fh:
            fh.write(text)

print("[OK] Limpieza de bloques completada en todos los capitulos.")
