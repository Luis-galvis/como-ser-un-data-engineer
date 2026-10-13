import os
import re
import zipfile
import html

CAP_DIR = r'c:\Users\lgalv\Desktop\obsidian\libro\capitulos'
OUT_EPUB = r'c:\Users\lgalv\Desktop\obsidian\libro\libro-digital.epub'
MIRROR_EPUB = r'D:\estudio\libro\libro-digital.epub'
CONDOR_IMG = r'c:\Users\lgalv\Desktop\obsidian\condor_profesional.jpg'

print("Iniciando compilacion de EPUB 3.0 formal...")

files = [f for f in sorted(os.listdir(CAP_DIR)) if f.endswith('.md') and f != '00-portada.md']

# Preparar archivo zip
epub = zipfile.ZipFile(OUT_EPUB, 'w')

# 1. mimetype (debe ser el primer archivo, sin comprimir)
epub.writestr('mimetype', 'application/epub+zip', compress_type=zipfile.ZIP_STORED)

# 2. META-INF/container.xml
container_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
    <rootfiles>
        <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
    </rootfiles>
</container>'''
epub.writestr('META-INF/container.xml', container_xml, compress_type=zipfile.ZIP_DEFLATED)

# 3. Estilos CSS
css_content = '''
body {
    font-family: Georgia, serif;
    font-size: 1em;
    line-height: 1.45;
    color: #1b2030;
    margin: 5%;
}
h1 { font-size: 1.8em; color: #1b2030; border-bottom: 1px solid #1b2030; padding-bottom: 0.3em; margin-top: 1em; }
h2 { font-size: 1.3em; color: #1b2030; margin-top: 1.2em; }
h3 { font-size: 1.1em; color: #4a5160; margin-top: 1em; }
p { text-align: justify; margin-bottom: 0.6em; text-indent: 1.2em; }
p:first-of-type, h2 + p, h3 + p { text-indent: 0; }
blockquote {
    border-left: 3px solid #4a5160;
    background: #fbf9f4;
    padding: 0.5em 1em;
    margin: 1em 0;
    font-style: italic;
    color: #4a5160;
}
pre {
    background: #f2ece0;
    border: 1px solid #d0c8b6;
    padding: 0.6em;
    font-family: monospace;
    font-size: 0.85em;
    overflow-x: auto;
}
code { font-family: monospace; font-size: 0.9em; background: #ede6d8; padding: 2px 4px; }
ul { margin-left: 1.5em; margin-bottom: 0.8em; }
.cover-img { max-width: 90%; height: auto; display: block; margin: 2em auto; }
'''
epub.writestr('OEBPS/styles/academic.css', css_content, compress_type=zipfile.ZIP_DEFLATED)

# 4. Imagen del cóndor
if os.path.exists(CONDOR_IMG):
    with open(CONDOR_IMG, 'rb') as img_f:
        epub.writestr('OEBPS/images/condor.jpg', img_f.read(), compress_type=zipfile.ZIP_DEFLATED)

# 5. Portada XHTML
cover_xhtml = '''<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<head>
    <title>Portada</title>
    <link rel="stylesheet" type="text/css" href="styles/academic.css"/>
</head>
<body style="text-align: center; margin: 0; padding: 2em;">
    <p style="font-style: italic; font-size: 1.3em; color: #4a5160; margin-bottom: 0;">Cómo ser un</p>
    <h1 style="border: none; font-size: 2.2em; margin-top: 0.2em; margin-bottom: 0.5em;">DATA ENGINEER</h1>
    <p style="font-style: italic; font-size: 1em; color: #4a5160;">Una mirada a fondo de los fundamentos y los procesos prácticos para llegar de cero a Data Engineer</p>
    <img src="images/condor.jpg" alt="Cóndor Andino" class="cover-img"/>
    <h3 style="margin-top: 2em; color: #1b2030;">Luis Galvis</h3>
    <p style="font-size: 0.9em; color: #4a5160;">2026</p>
</body>
</html>'''
epub.writestr('OEBPS/cover.xhtml', cover_xhtml, compress_type=zipfile.ZIP_DEFLATED)

# 6. Páginas preliminares
prelims_xhtml = '''<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <title>Preliminares</title>
    <link rel="stylesheet" type="text/css" href="styles/academic.css"/>
</head>
<body>
    <h2>Cómo ser un Data Engineer</h2>
    <p><strong>Autor:</strong> Luis Galvis</p>
    <p><strong>Edición:</strong> Primera edición (2026)</p>
    <p>© 2026 Luis Galvis. Todos los derechos reservados. Código bajo licencia MIT.</p>
    <p>Escrito y compilado en Ibagué, Tolima, Colombia.</p>
    <hr/>
    <blockquote style="text-align: center; margin: 3em 1em;">
        "A mis padres, por ser el cimiento inquebrantable, el sacrificio silencioso y la luz que impulsó cada uno de mis pasos. A mi pareja, por su amor incondicional, su infinita paciencia y su aliento constante en cada noche de estudio y creación. Y a todos los ingenieros y científicos que buscan comprender la esencia profunda de los datos más allá de las herramientas efímeras."<br/><br/>
        <strong>— Luis Galvis</strong>
    </blockquote>
</body>
</html>'''
epub.writestr('OEBPS/prelims.xhtml', prelims_xhtml, compress_type=zipfile.ZIP_DEFLATED)

# 7. Convertir capítulos
manifest_items = [
    '<item id="css" href="styles/academic.css" media-type="text/css"/>',
    '<item id="cover-image" href="images/condor.jpg" media-type="image/jpeg" properties="cover-image"/>',
    '<item id="cover" href="cover.xhtml" media-type="application/xhtml+xml"/>',
    '<item id="prelims" href="prelims.xhtml" media-type="application/xhtml+xml"/>',
    '<item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>'
]
spine_items = [
    '<itemref idref="cover"/>',
    '<itemref idref="prelims"/>',
    '<itemref idref="nav"/>'
]
nav_links = []

for idx, file in enumerate(files):
    filepath = os.path.join(CAP_DIR, file)
    with open(filepath, 'r', encoding='utf-8') as f:
        md = f.read()

    # Título
    m = re.search(r'^#\s+(.+)$', md, flags=re.MULTILINE)
    title = m.group(1).replace('[AÑADIDO]', '').replace('[FALTA]', '').strip() if m else f"Capítulo {idx}"

    # Parsear a XHTML básico
    # Código
    def code_sub(match):
        c = html.escape(match.group(2))
        return f'<pre><code>{c}</code></pre>'
    md = re.sub(r'```([a-zA-Z0-9_-]*)\r?\n([\s\S]*?)```', code_sub, md)

    # Encabezados
    md = re.sub(r'^### (.*$)', r'<h3>\1</h3>', md, flags=re.MULTILINE)
    md = re.sub(r'^## (.*$)', r'<h2>\1</h2>', md, flags=re.MULTILINE)
    md = re.sub(r'^# (.*$)', r'<h1>\1</h1>', md, flags=re.MULTILINE)

    # Citas
    md = re.sub(r'^\> (.*$)', r'<blockquote><p>\1</p></blockquote>', md, flags=re.MULTILINE)

    # Formatos
    md = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', md)
    md = re.sub(r'\*(.*?)\*', r'<em>\1</em>', md)
    md = re.sub(r'`([^`]+)`', r'<code>\1</code>', md)
    md = re.sub(r'^\* (.*$)', r'<li>\1</li>', md, flags=re.MULTILINE)
    md = re.sub(r'---', r'<hr/>', md)

    # Párrafos
    paras = md.split('\n\n')
    clean_paras = []
    for p in paras:
        p = p.strip()
        if not p:
            continue
        if p.startswith('<h') or p.startswith('<blockquote') or p.startswith('<pre') or p.startswith('<hr') or p.startswith('<li'):
            clean_paras.append(p)
        else:
            clean_paras.append(f'<p>{p}</p>')
    body_html = "\n".join(clean_paras)

    chap_id = f"chap_{idx:02d}"
    chap_file = f"capitulo_{idx:02d}.xhtml"

    chap_xhtml = f'''<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <title>{html.escape(title)}</title>
    <link rel="stylesheet" type="text/css" href="styles/academic.css"/>
</head>
<body>
{body_html}
</body>
</html>'''

    epub.writestr(f'OEBPS/{chap_file}', chap_xhtml, compress_type=zipfile.ZIP_DEFLATED)
    manifest_items.append(f'<item id="{chap_id}" href="{chap_file}" media-type="application/xhtml+xml"/>')
    spine_items.append(f'<itemref idref="{chap_id}"/>')
    nav_links.append(f'<li><a href="{chap_file}">{html.escape(title)}</a></li>')

# 8. Nav XHTML (TOC de EPUB 3)
nav_xhtml = f'''<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<head>
    <title>Tabla de Contenido</title>
    <link rel="stylesheet" type="text/css" href="styles/academic.css"/>
</head>
<body>
    <nav epub:type="toc" id="toc">
        <h1>Tabla de Contenido</h1>
        <ol>
            {"".join(nav_links)}
        </ol>
    </nav>
</body>
</html>'''
epub.writestr('OEBPS/nav.xhtml', nav_xhtml, compress_type=zipfile.ZIP_DEFLATED)

# 9. content.opf
opf_content = f'''<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="pub-id">
    <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
        <dc:identifier id="pub-id">urn:uuid:optimal-data-eng-2026</dc:identifier>
        <dc:title>Cómo ser un Data Engineer</dc:title>
        <dc:creator>Luis Galvis</dc:creator>
        <dc:language>es-CO</dc:language>
        <dc:date>2026-10-11</dc:date>
        <meta property="dcterms:modified">2026-10-11T20:00:00Z</meta>
    </metadata>
    <manifest>
        {"".join(manifest_items)}
    </manifest>
    <spine>
        {"".join(spine_items)}
    </spine>
</package>'''
epub.writestr('OEBPS/content.opf', opf_content, compress_type=zipfile.ZIP_DEFLATED)

epub.close()
print(f"[OK] Archivo EPUB generado: {OUT_EPUB}")

if os.path.exists(r'D:\estudio\libro'):
    import shutil
    shutil.copyfile(OUT_EPUB, MIRROR_EPUB)
    print(f"[OK] Archivo EPUB sincronizado en: {MIRROR_EPUB}")
