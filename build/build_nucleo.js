// Compilador del extracto corto: 'Cómo ser un Data Engineer · Edición Núcleo' (~150 páginas)
// Compila exclusivamente los 23 capítulos clasificados como [NÚCLEO EMPLEABILIDAD]
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');
const puppeteer = require('puppeteer-core');
const katexDir = path.dirname(require.resolve('katex/package.json'));
const cfg = require('./book.config');
const { render } = require('./render');
const { normalizeChapter } = require('./normalize');

const ROOT = path.join(__dirname, '..');
const CAP_DIR = path.join(ROOT, 'libro', 'capitulos');
const OUT_DIR = path.join(ROOT, 'libro');
const CHROME = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const MERMAID_JS = require.resolve('mermaid/dist/mermaid.min.js');

const roman = n => ['', 'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII', 'XIX', 'XX'][n];

function katexCss() {
  let css = fs.readFileSync(path.join(katexDir, 'dist', 'katex.min.css'), 'utf8');
  return css.replace(/url\(fonts\/([^)]+?\.woff2)\)\s*format\("woff2"\)(,\s*url\([^)]*\)\s*format\("[^"]*"\))*/g, (m, f) => {
    const p = path.join(katexDir, 'dist', 'fonts', f);
    return fs.existsSync(p) ? `url(data:font/woff2;base64,${fs.readFileSync(p).toString('base64')}) format("woff2")` : m;
  });
}

function loadNucleoChapters() {
  const files = fs.readdirSync(CAP_DIR).filter(f => f.endsWith('.md') && f !== '00-portada.md').sort();
  const selected = [];
  files.forEach(file => {
    const raw = normalizeChapter(fs.readFileSync(path.join(CAP_DIR, file), 'utf8'));
    if (raw.includes('[NÚCLEO EMPLEABILIDAD]')) {
      const h1 = raw.match(/^#\s+(?:Cap[ií]tulo\s+([\w.]+):\s*)?(.+)$/m);
      const num = h1 && h1[1] ? h1[1] : '';
      const title = h1 ? h1[2].trim() : file;
      const body = raw.replace(/^#\s+.+$/m, '');
      selected.push({ file, num, title, html: render(body) });
    }
  });
  return selected;
}

function formatTitle(title) {
  return title
    .replace(/\[NÚCLEO EMPLEABILIDAD\]/g, '<span class="badge-nucleo">NÚCLEO</span>')
    .replace(/\[RECOMENDADO\]/g, '<span class="badge-rec">RECOMENDADO</span>')
    .replace(/\[AVANZADO\]/g, '<span class="badge-adv">AVANZADO</span>');
}

function assembleNucleo(chapters, pageMap) {
  const pg = k => (pageMap && pageMap[k]) || '·';
  const toc = chapters.map(c => `
    <tr class="toc-ch">
      <td class="num">${c.num !== '' ? 'Cap. ' + c.num : ''}</td>
      <td class="ttl"><span>${formatTitle(c.title)}</span></td>
      <td class="pn">${pg('CAP_' + String(c.num).toUpperCase())}</td>
    </tr>
  `).join('');

  const body = chapters.map(c => `
    <article class="chapter" id="cap-${c.num}">
      <div class="chapter-label">CAPÍTULO ${c.num}</div>
      <h1>${formatTitle(c.title)}</h1>
      ${c.html}
    </article>
  `).join('\n');

  const condor = 'data:image/jpeg;base64,' + fs.readFileSync(path.join(ROOT, 'condor_profesional.jpg')).toString('base64');
  const css = fs.readFileSync(path.join(__dirname, 'book.css'), 'utf8');

  return `<!doctype html>
<html lang="es"><head><meta charset="utf-8"><title>${cfg.title} · Edición Núcleo</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,600;0,700;1,400;1,600&family=EB+Garamond:ital,wght@0,400;0,500;0,600;1,400;1,500&family=JetBrains+Mono:wght@400;500;600&display=swap');
${katexCss()}
${css}
</style></head><body>
<div class="cover-container"><div class="cover-border"><div class="cover-inner-border">
  <div>
    <div class="cover-pre">EDICIÓN NÚCLEO · VÍA RÁPIDA A EMPLEABILIDAD</div>
    <div class="cover-title">${cfg.titleMain}</div>
    <div class="cover-sub">Los 23 capítulos imprescindibles para conseguir empleo como Data Engineer Junior</div>
  </div>
  <div class="cover-art"><img src="${condor}" alt="Cóndor andino"></div>
  <div class="cover-foot"><div class="cover-author">${cfg.author}</div><div class="cover-year">${cfg.year}</div></div>
</div></div></div>

<section class="sheet bare"><div class="title-page">
  <div class="pre">EDICIÓN NÚCLEO ESENCIAL</div>
  <div class="main">${cfg.titleMain}</div>
  <div class="sub">Guía condensada de alta densidad técnica para inserción laboral</div>
  <div class="auth">${cfg.author}</div><div class="yr">${cfg.year}</div></div></section>

<section class="sheet bare">
  <div class="dedication">“${cfg.dedication}”<div class="sig">— ${cfg.author}</div></div>
  <div class="legal">
    <div class="t">${cfg.title} — Edición Núcleo</div>
    <div>Extracto de 23 Capítulos Esenciales para Empleo</div>
    <div style="margin:8px 0">${cfg.rights}</div>
    <p>${cfg.rightsText}</p>
    <p>${cfg.codeLicense}</p>
    <div class="origin">Escrito y compilado en ${cfg.place} · ${cfg.year}</div>
  </div></section>

<section class="sheet"><h2 class="front-title">Índice de Capítulos Núcleo</h2><table class="toc-table">${toc}</table></section>

<section class="sheet guide"><h2 class="front-title">Propósito de la Edición Núcleo</h2>
<p>Este extracto contiene únicamente los 23 capítulos catalogados como <strong>[NÚCLEO EMPLEABILIDAD]</strong> del tratado completo de 50 capítulos. Representa la ruta crítica indispensable que exige el mercado laboral para superar pruebas de código en vivo y entrevistas de arquitectura de nivel Junior a Mid-Level.</p>
<p>Si dispone de tiempo acotado o busca validar rápidamente sus competencias mínimas viables antes de postularse, comience por esta selección.</p>
</section>

${body}
</body></html>`;
}

async function makePdf(html, htmlPath, pdfPath) {
  fs.writeFileSync(htmlPath, html, 'utf8');
  const browser = await puppeteer.launch({ executablePath: CHROME, headless: true, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.goto('file:///' + htmlPath.replace(/\\/g, '/'), { waitUntil: 'networkidle0', timeout: 120000 });
  await page.addScriptTag({ path: MERMAID_JS });
  await page.evaluate(async () => {
    mermaid.initialize({
      startOnLoad: false, securityLevel: 'loose', theme: 'base',
      themeVariables: { fontFamily: 'EB Garamond, Georgia, serif', fontSize: '22px', primaryColor: '#ffffff',
        primaryBorderColor: '#4a5160', primaryTextColor: '#1b2030', lineColor: '#4a5160', secondaryColor: '#f4f4f1', tertiaryColor: '#ffffff' },
      flowchart: { htmlLabels: true, curve: 'basis', useMaxWidth: true, wrappingWidth: 150, nodeSpacing: 30, rankSpacing: 40 }, sequence: { useMaxWidth: true },
    });
    const nodes = [...document.querySelectorAll('.mermaid')];
    for (let i = 0; i < nodes.length; i++) {
      const n = nodes[i];
      try {
        const { svg } = await mermaid.render('mmd_nuc_' + i, n.dataset.src);
        n.outerHTML = `<div class="diagram">${svg}</div>`;
      } catch (e) {
        n.outerHTML = `<div class="code-container"><div class="code-header">Diagrama</div><pre><code>${n.dataset.src.replace(/</g, '&lt;')}</code></pre></div>`;
      }
    }
    await document.fonts.ready;
  });
  fs.writeFileSync(htmlPath, '<!doctype html>' + await page.evaluate(() => document.documentElement.outerHTML), 'utf8');
  await page.pdf({ path: pdfPath, preferCSSPageSize: true, printBackground: true });
  await browser.close();
}

function locatePages(pdfPath) {
  const out = execFileSync('python', [path.join(__dirname, 'pdf_pages.py'), pdfPath], { encoding: 'utf8' });
  const line = out.split(/\r?\n/).reverse().find(l => l.startsWith('{'));
  return JSON.parse(line);
}

(async () => {
  const chapters = loadNucleoChapters();
  console.log(`Compilando Edición Núcleo (${chapters.length} capítulos indispensables)...`);
  const htmlPath = path.join(OUT_DIR, 'libro-edicion-nucleo.html');
  const pdfPath = path.join(OUT_DIR, 'libro-edicion-nucleo.pdf');

  await makePdf(assembleNucleo(chapters, null), htmlPath, pdfPath);
  let info = locatePages(pdfPath);
  console.log('Pasada 1 Edición Núcleo:', info.pages, 'páginas');
  await makePdf(assembleNucleo(chapters, info.labels), htmlPath, pdfPath);
  const info2 = locatePages(pdfPath);
  console.log('Pasada 2 Edición Núcleo:', info2.pages, 'páginas completadas.');
  console.log('[OK] Edición Núcleo generada exitosamente en:', pdfPath);
})().catch(e => { console.error(e); process.exit(1); });
