// Compilador del libro: Markdown -> HTML -> PDF (7x10 in) con índice de páginas reales.
// Uso: node build/build.js        (ejecuta normalización, render y PDF en dos pasadas)
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
const PUBLIC_DIR = path.join(ROOT, 'public');
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

function loadChapters() {
  const files = fs.readdirSync(CAP_DIR).filter(f => f.endsWith('.md') && f !== '00-portada.md').sort();
  return files.map(file => {
    const raw = normalizeChapter(fs.readFileSync(path.join(CAP_DIR, file), 'utf8'));
    const h1 = raw.match(/^#\s+(?:Cap[ií]tulo\s+([\w.]+):\s*)?(.+)$/m);
    const num = h1 && h1[1] ? h1[1] : '';
    const title = h1 ? h1[2].trim() : file;
    const body = raw.replace(/^#\s+.+$/m, '');
    return { file, num, title, html: render(body) };
  });
}

function formatTitle(title) {
  return title
    .replace(/\[NÚCLEO EMPLEABILIDAD\]/g, '<span class="badge-nucleo">NÚCLEO</span>')
    .replace(/\[RECOMENDADO\]/g, '<span class="badge-rec">RECOMENDADO</span>')
    .replace(/\[AVANZADO\]/g, '<span class="badge-adv">AVANZADO</span>');
}

function assemble(chapters, pageMap) {
  const parts = [];
  let current = null, partNo = 0;
  chapters.forEach(ch => {
    const p = cfg.parts.find(x => x.start === ch.file);
    if (p) { partNo += 1; current = { no: partNo, title: p.title, chapters: [] }; parts.push(current); }
    current.chapters.push(ch);
  });

  const pg = k => (pageMap && pageMap[k]) || '·';
  const toc = parts.map(p => `
    <tr class="toc-part"><td colspan="2">Parte ${roman(p.no)} · ${p.title}</td><td class="pn">${pg('PARTE_' + roman(p.no))}</td></tr>
    ${p.chapters.map(c => `<tr class="toc-ch"><td class="num">${c.num !== '' ? 'Cap. ' + c.num : ''}</td><td class="ttl"><span>${formatTitle(c.title)}</span></td><td class="pn">${pg('CAP_' + String(c.num).toUpperCase())}</td></tr>`).join('')}
  `).join('');

  const body = parts.map(p => `
    <section class="part-page">
      <div class="kicker">PARTE ${roman(p.no)}</div>
      <div class="pt">${p.title}</div>
      <div class="rule"></div>
      <ul>${p.chapters.map(c => `<li>${c.num !== '' ? 'Capítulo ' + c.num + ' · ' : ''}${formatTitle(c.title)}</li>`).join('')}</ul>
    </section>
    ${p.chapters.map(c => `
    <article class="chapter" id="cap-${c.num}">
      <div class="chapter-label">CAPÍTULO ${c.num}</div>
      <h1>${formatTitle(c.title)}</h1>
      ${c.html}
    </article>`).join('\n')}
  `).join('\n');

  const condor = 'data:image/jpeg;base64,' + fs.readFileSync(path.join(ROOT, 'condor_profesional.jpg')).toString('base64');
  const css = fs.readFileSync(path.join(__dirname, 'book.css'), 'utf8');

  return `<!doctype html>
<html lang="es"><head><meta charset="utf-8"><title>${cfg.title}</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,600;0,700;1,400;1,600&family=EB+Garamond:ital,wght@0,400;0,500;0,600;1,400;1,500&family=JetBrains+Mono:wght@400;500;600&display=swap');
${katexCss()}
${css}
</style></head><body>
<div class="cover-container"><div class="cover-border"><div class="cover-inner-border">
  <div><div class="cover-pre">${cfg.titlePre}</div><div class="cover-title">${cfg.titleMain}</div><div class="cover-sub">${cfg.subtitle}</div></div>
  <div class="cover-art"><img src="${condor}" alt="Cóndor andino"></div>
  <div class="cover-foot"><div class="cover-author">${cfg.author}</div><div class="cover-year">${cfg.year}</div></div>
</div></div></div>

<section class="sheet bare"><div class="title-page">
  <div class="pre">${cfg.titlePre}</div><div class="main">${cfg.titleMain}</div><div class="sub">${cfg.subtitle}</div>
  <div class="auth">${cfg.author}</div><div class="yr">${cfg.year}</div></div></section>

<section class="sheet bare">
  <div class="dedication">“${cfg.dedication}”<div class="sig">— ${cfg.author}</div></div>
  <div class="legal">
    <div class="t">${cfg.title}</div>
    <div>${cfg.edition}</div>
    <div style="margin:8px 0">${cfg.rights}</div>
    <p>${cfg.rightsText}</p>
    <p>${cfg.codeLicense}</p>
    <div class="origin">Escrito y compilado en ${cfg.place} · ${cfg.year}</div>
  </div></section>

<section class="sheet"><h2 class="front-title">Índice general</h2><table class="toc-table">${toc}</table></section>

<section class="sheet guide"><h2 class="front-title">Guía del lector</h2>${cfg.guide.map(t => `<p>${t}</p>`).join('')}</section>

${body}
</body></html>`;
}

async function makePdf(html, htmlPath, pdfPath) {
  fs.writeFileSync(htmlPath, html, 'utf8');
  const browser = await puppeteer.launch({ executablePath: CHROME, headless: true, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  const errors = [];
  await page.goto('file:///' + htmlPath.replace(/\\/g, '/'), { waitUntil: 'networkidle0', timeout: 120000 });
  await page.addScriptTag({ path: MERMAID_JS });
  const merrs = await page.evaluate(async () => {
    mermaid.initialize({
      startOnLoad: false, securityLevel: 'loose', theme: 'base',
      themeVariables: { fontFamily: 'EB Garamond, Georgia, serif', fontSize: '22px', primaryColor: '#ffffff',
        primaryBorderColor: '#4a5160', primaryTextColor: '#1b2030', lineColor: '#4a5160', secondaryColor: '#f4f4f1', tertiaryColor: '#ffffff' },
      flowchart: { htmlLabels: true, curve: 'basis', useMaxWidth: true, wrappingWidth: 150, nodeSpacing: 30, rankSpacing: 40 }, sequence: { useMaxWidth: true },
    });
    const errs = [];
    const nodes = [...document.querySelectorAll('.mermaid')];
    for (let i = 0; i < nodes.length; i++) {
      const n = nodes[i];
      try {
        const { svg } = await mermaid.render('mmd' + i, n.dataset.src);
        n.outerHTML = `<div class="diagram">${svg}</div>`;
      } catch (e) {
        errs.push({ i, msg: String(e.message || e).slice(0, 200), src: n.dataset.src.slice(0, 80) });
        n.outerHTML = `<div class="code-container"><div class="code-header">Diagrama</div><pre><code>${n.dataset.src.replace(/</g, '&lt;')}</code></pre></div>`;
      }
    }
    await document.fonts.ready;
    return errs;
  });
  if (merrs.length) console.warn('Diagramas con error:', JSON.stringify(merrs, null, 1));
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
  const chapters = loadChapters();
  console.log(`Compilando ${chapters.length} capítulos...`);
  const htmlPath = path.join(OUT_DIR, 'libro_academico_completo.html');
  const pdfPath = path.join(OUT_DIR, 'libro-digital.pdf');

  await makePdf(assemble(chapters, null), htmlPath, pdfPath);
  let info = locatePages(pdfPath);
  console.log('Pasada 1:', info.pages, 'páginas');
  await makePdf(assemble(chapters, info.labels), htmlPath, pdfPath);
  const info2 = locatePages(pdfPath);
  const same = JSON.stringify(info.labels) === JSON.stringify(info2.labels);
  console.log('Pasada 2:', info2.pages, 'páginas;', same ? 'índice consistente' : 'AVISO: el índice cambió, repetir');
  if (!same) { await makePdf(assemble(chapters, info2.labels), htmlPath, pdfPath); }

  fs.mkdirSync(PUBLIC_DIR, { recursive: true });
  fs.copyFileSync(htmlPath, path.join(PUBLIC_DIR, 'libro_academico_completo.html'));
  if (fs.existsSync('D:\\estudio\\libro')) {
    fs.copyFileSync(pdfPath, 'D:\\estudio\\libro\\libro-digital.pdf');
    console.log('PDF sincronizado en D:\\estudio\\libro');
  }
  console.log('[OK]', pdfPath);
})().catch(e => { console.error(e); process.exit(1); });
