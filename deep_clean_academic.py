import os
import re

CAP_DIR = r'c:\Users\lgalv\Desktop\obsidian\libro\capitulos'
MIRROR_DIR = r'D:\estudio\libro\capitulos'

files = [f for f in sorted(os.listdir(CAP_DIR)) if f.endswith('.md') and f != '00-portada.md']

print(f"Iniciando limpieza profunda y academizacion de {len(files)} capitulos...")

for file in files:
    filepath = os.path.join(CAP_DIR, file)
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Quitar caracteres invisibles
    content = content.replace('\uFE0F', '').replace('\u200B', '')

    # 2. Numero de capitulo
    m = re.match(r'^(\d+)-', file)
    cap_num = int(m.group(1)) if m else 1

    # 3. Eliminar HTML <details> y <summary> y <pre style="...">
    content = re.sub(r'<details>\s*<summary>\s*\**Ver Solución[^\n<]*\**\s*<\/summary>', r'**Solución:**', content, flags=re.IGNORECASE)
    content = content.replace('</details>', '')
    content = re.sub(r'<pre[^>]*><code>([\s\S]*?)<\/code><\/pre>', r'```sql\n\1\n```', content)

    # 4. Sustituir encabezados de callout rotos
    # PROFUNDIZACION TECNICA
    content = re.sub(
        r'\*\*\s*PROFUNDIZACIÓN TÉCNICA:\s*([^\*]+)\*\*',
        rf'### {cap_num}.2 Profundización Técnica: \1',
        content,
        flags=re.IGNORECASE
    )

    # PATRON DE IMPLEMENTACION
    content = re.sub(
        r'\*\*\s*PATRÓN DE IMPLEMENTACIÓN EN PRODUCCIÓN:\s*([^\*]+)\*\*',
        rf'> **Consideraciones de Producción {cap_num}.1: \1**',
        content,
        flags=re.IGNORECASE
    )

    # CONSIDERACIONES CRITICAS Y ANTI-PATRONES
    content = re.sub(
        r'\*\*\s*CONSIDERACIONES CRÍTICAS Y ANTI-PATRONES\s*\*\*',
        rf'### {cap_num}.3 Criterios de Fiabilidad y Anti-Patrones en Producción',
        content,
        flags=re.IGNORECASE
    )

    # EJERCICIOS DE CONSOLIDACION
    content = re.sub(
        r'\*\*\s*\[?AÑADIDO\]?\s*EJERCICIOS DE CONSOLIDACIÓN\s*\*\*',
        r'## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos',
        content,
        flags=re.IGNORECASE
    )

    # 5. Limpiar cualquier tag HTML residual
    content = re.sub(r'<div[^>]*>', '', content)
    content = re.sub(r'<\/div>', '', content)
    content = re.sub(r'<strong[^>]*>(.*?)<\/strong>', r'**\1**', content)
    content = re.sub(r'<em[^>]*>(.*?)<\/em>', r'*\1*', content)
    content = re.sub(r'<code[^>]*>(.*?)<\/code>', r'`\1`', content)
    content = re.sub(r'<br\s*\/?>', '\n', content)

    # 6. Limpieza de lineas en blanco triples o cuadruples
    content = re.sub(r'\n{4,}', '\n\n', content)

    # Guardar archivo limpio
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

    # Sincronizar espejo si existe
    if os.path.exists(MIRROR_DIR):
        with open(os.path.join(MIRROR_DIR, file), 'w', encoding='utf-8') as f:
            f.write(content)

print(f"Limpieza academica completada en todos los capitulos!")
