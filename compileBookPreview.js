const fs = require('fs');
const path = require('path');

const CAP_DIR = 'c:\\Users\\lgalv\\Desktop\\obsidian\\libro\\capitulos';
const OUT_HTML = 'c:\\Users\\lgalv\\Desktop\\obsidian\\libro\\LIBRO_PREVIEW.html';
const PUBLIC_HTML = 'c:\\Users\\lgalv\\Desktop\\obsidian\\public\\libro_preview.html';
const MIRROR_HTML = 'D:\\estudio\\libro\\LIBRO_PREVIEW.html';

// Simple lightweight markdown parser for preview
function mdToHtml(md) {
  let html = md;

  // Protect code blocks
  const codeBlocks = [];
  html = html.replace(/```([a-zA-Z0-9_-]*)\r?\n([\s\S]*?)```/g, (match, lang, code) => {
    const idx = codeBlocks.length;
    if (lang === 'mermaid') {
      codeBlocks.push(`<div class="mermaid">${code}</div>`);
    } else {
      const escaped = code.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      codeBlocks.push(`<pre><code class="language-${lang}">${escaped}</code></pre>`);
    }
    return `___CODE_BLOCK_${idx}___`;
  });

  // Headers
  html = html.replace(/^### (.*$)/gim, '<h3>$1</h3>')
             .replace(/^## (.*$)/gim, '<h2>$1</h2>')
             .replace(/^# (.*$)/gim, '<h1 id="$1">$1</h1>');

  // Blockquotes
  html = html.replace(/^\> (.*$)/gim, '<blockquote>$1</blockquote>');

  // Bold & Italic
  html = html.replace(/\*\*\*(.*?)\*\*\*/g, '<strong><em>$1</em></strong>')
             .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
             .replace(/\*(.*?)\*/g, '<em>$1</em>');

  // Inline code
  html = html.replace(/`([^`]+)`/g, '<code>$1</code>');

  // Unordered lists
  html = html.replace(/^\* (.*$)/gim, '<ul><li>$1</li></ul>')
             .replace(/<\/ul>\s*<ul>/g, '');

  // Horizontal rules
  html = html.replace(/^---$/gim, '<hr>');

  // Paragraphs
  html = html.split(/\n\n+/).map(p => {
    p = p.trim();
    if (!p) return '';
    if (p.startsWith('<h') || p.startsWith('<div') || p.startsWith('<ul') || p.startsWith('<table') || p.startsWith('<blockquote') || p.startsWith('___CODE_BLOCK_') || p.startsWith('<hr')) {
      return p;
    }
    return `<p>${p.replace(/\n/g, '<br>')}</p>`;
  }).join('\n\n');

  // Restore code blocks
  codeBlocks.forEach((block, idx) => {
    html = html.replace(`___CODE_BLOCK_${idx}___`, block);
  });

  return html;
}

// 1. Get and sort chapter files
const files = fs.readdirSync(CAP_DIR)
  .filter(f => f.endsWith('.md'))
  .sort();

console.log(`Encontrados ${files.length} capítulos para compilar.`);

const tocItems = [];
const chaptersHtml = [];

for (const file of files) {
  const content = fs.readFileSync(path.join(CAP_DIR, file), 'utf8');
  
  // Extract chapter title
  const h1Match = content.match(/^#\s+(.+)$/m);
  const title = h1Match ? h1Match[1].replace(/\[.*?\]/g, '').trim() : file;
  const slug = file.replace('.md', '');

  tocItems.push({ file, title, slug });
  
  // Convert markdown
  const body = mdToHtml(content);
  chaptersHtml.push(`
    <article id="${slug}" class="chapter-container">
      <div class="chapter-meta">Archivo: <code>${file}</code></div>
      ${body}
    </article>
    <div class="page-break"></div>
  `);
}

// 2. Assemble Master Preview HTML
const fullHtml = `<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Cómo ser un Data Engineer — Previsualización Editorial</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
  <script>mermaid.initialize({ startOnLoad: true, theme: 'neutral' });</script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,500;0,600;0,700;1,400;1,600&family=EB+Garamond:ital,wght@0,400;0,500;0,600;1,400;1,500&family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600;700&display=swap');

    :root {
      --ink: #1b2030;
      --slate: #4a5160;
      --paper: #fbf9f4;
      --sidebar-bg: #f4efe6;
      --border: #e0d8cb;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: 'EB Garamond', Georgia, serif;
      font-size: 11.5pt;
      line-height: 1.45;
      color: var(--ink);
      background-color: #e5dec9;
      display: flex;
    }

    /* Barra lateral interactiva */
    #sidebar {
      width: 320px;
      height: 100vh;
      background: var(--sidebar-bg);
      border-right: 1px solid var(--border);
      position: sticky;
      top: 0;
      overflow-y: auto;
      padding: 24px 20px;
      flex-shrink: 0;
      font-family: 'Inter', sans-serif;
    }

    #sidebar h2 {
      font-size: 1.05rem;
      font-weight: 700;
      color: var(--ink);
      margin-bottom: 6px;
      letter-spacing: -0.01em;
    }

    #sidebar p.subtitle {
      font-size: 0.8rem;
      color: var(--slate);
      margin-bottom: 18px;
      line-height: 1.3;
    }

    .toc-list {
      list-style: none;
    }

    .toc-list li {
      margin-bottom: 6px;
    }

    .toc-list a {
      display: block;
      padding: 6px 10px;
      font-size: 0.82rem;
      color: var(--slate);
      text-decoration: none;
      border-radius: 4px;
      line-height: 1.35;
      transition: all 0.15s ease;
    }

    .toc-list a:hover {
      background: #e6dfd1;
      color: var(--ink);
      font-weight: 600;
    }

    .status-badge {
      display: inline-block;
      font-size: 0.7rem;
      background: #2f6b52;
      color: #fff;
      padding: 2px 6px;
      border-radius: 3px;
      margin-top: 10px;
      margin-bottom: 15px;
    }

    /* Contenedor principal de lectura */
    #content {
      flex: 1;
      padding: 40px 60px;
      max-width: 950px;
      margin: 0 auto;
      background: var(--paper);
      box-shadow: 0 4px 25px rgba(0,0,0,0.08);
      min-height: 100vh;
    }

    .chapter-container {
      margin-bottom: 60px;
    }

    .chapter-meta {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.75rem;
      color: var(--slate);
      text-align: right;
      margin-bottom: 10px;
      padding-bottom: 5px;
      border-bottom: 1px dashed var(--border);
    }

    h1 {
      font-family: 'Cormorant Garamond', Georgia, serif;
      font-size: 2.2rem;
      font-weight: 700;
      color: var(--ink);
      line-height: 1.15;
      margin-top: 20px;
      margin-bottom: 12px;
      border-bottom: 2px solid var(--ink);
      padding-bottom: 8px;
    }

    h2 {
      font-family: 'Cormorant Garamond', Georgia, serif;
      font-size: 1.6rem;
      font-weight: 600;
      color: var(--ink);
      margin-top: 28px;
      margin-bottom: 10px;
    }

    h3 {
      font-family: 'Cormorant Garamond', Georgia, serif;
      font-size: 1.25rem;
      font-weight: 600;
      color: var(--slate);
      margin-top: 20px;
      margin-bottom: 8px;
    }

    p {
      margin-bottom: 12px;
      text-align: justify;
    }

    blockquote {
      border-left: 3px solid var(--slate);
      padding-left: 14px;
      font-style: italic;
      color: var(--slate);
      margin: 16px 0;
      font-size: 1.05rem;
    }

    pre {
      background: #f0ebe1;
      padding: 14px;
      border-radius: 4px;
      overflow-x: auto;
      font-family: 'JetBrains Mono', monospace;
      font-size: 8.8pt;
      line-height: 1.4;
      margin: 16px 0;
      border: 1px solid #e2dbce;
    }

    code {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.9em;
      background: #f0ebe1;
      padding: 1px 4px;
      border-radius: 3px;
    }

    pre code {
      background: none;
      padding: 0;
    }

    hr {
      border: none;
      border-top: 1px solid var(--border);
      margin: 24px 0;
    }

    ul, ol {
      margin-left: 24px;
      margin-bottom: 14px;
    }

    li {
      margin-bottom: 4px;
    }

    .page-break {
      page-break-after: always;
      border-top: 2px dashed #d5ccba;
      margin: 40px 0;
      position: relative;
    }

    .page-break::after {
      content: "FIN DE CAPÍTULO";
      position: absolute;
      top: -10px;
      left: 50%;
      transform: translateX(-50%);
      background: var(--paper);
      padding: 0 12px;
      font-family: 'Inter', sans-serif;
      font-size: 0.65rem;
      letter-spacing: 0.1em;
      color: var(--slate);
    }

    @media print {
      body { background: none; }
      #sidebar { display: none; }
      #content { max-width: 100%; padding: 0; margin: 0; box-shadow: none; }
      .page-break::after { display: none; }
    }
  </style>
</head>
<body>

  <aside id="sidebar">
    <h2>📖 Cómo ser un Data Engineer</h2>
    <p class="subtitle">Luis Galvis · Edición de Trabajo 2026</p>
    <div class="status-badge">14 Capítulos Disponibles</div>
    <ul class="toc-list">
      ${tocItems.map(item => `<li><a href="#${item.slug}">${item.title}</a></li>`).join('\n      ')}
    </ul>
    <div style="margin-top: 24px; padding-top: 16px; border-top: 1px solid var(--border); font-size: 0.75rem; color: var(--slate);">
      💡 <em>Tip: Presiona <code>Ctrl + P</code> para ver la maqueta en 7x10 in lista para PDF.</em>
    </div>
  </aside>

  <main id="content">
    ${chaptersHtml.join('\n')}
  </main>

</body>
</html>`;

fs.writeFileSync(OUT_HTML, fullHtml, 'utf8');
fs.writeFileSync(PUBLIC_HTML, fullHtml, 'utf8');
if (fs.existsSync('D:\\estudio\\libro')) {
  fs.writeFileSync(MIRROR_HTML, fullHtml, 'utf8');
}

console.log('✅ Compilado visor maestro:', OUT_HTML);
console.log('✅ Copiado a public para servidor web:', PUBLIC_HTML);
