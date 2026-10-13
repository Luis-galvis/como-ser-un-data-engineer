// Normalización idempotente de los manuscritos Markdown (libro/capitulos/*.md).
// Corrige artefactos de generación que NO son contenido: marcas de edición,
// escapes LaTeX rotos, residuos de scripts, encabezados con comentarios y
// numeración de secciones duplicada. Ejecutar: node build/normalize.js
const fs = require('fs');
const path = require('path');

const CAP_DIR = path.join(__dirname, '..', 'libro', 'capitulos');

function fixControlChars(t) {
  // Las barras invertidas de LaTeX se interpretaron como escapes de JS.
  return t
    .replace(/\x09ext\{/g, '\\text{')
    .replace(/\x09imes/g, '\\times')
    .replace(/\x0Crac\{/g, '\\frac{')
    .replace(/\x07pprox/g, '\\approx')
    .replace(/\x08eta/g, '\\beta');
}

function normalizeChapter(raw) {
  let t = raw.replace(/\r\n/g, '\n');
  t = fixControlChars(t);

  // Marcas de edición
  t = t.replace(/\[AÑADIDO\]\s*/g, '').replace(/\[FALTA\]\s*/g, '');

  // Residuos de generación (Cap. 9)
  t = t.replace(/\n`;\s*\n+\s*save\([^\n]*\);\s*/g, '\n');
  t = t.replace(/\n\s*save\('[^']*',\s*cap\d+\);\s*/g, '\n');

  // "# <!-- NOTAS ... -->" convertido a un comentario puro (no es un H1 real)
  t = t.replace(/^#\s*(<!--[\s\S]*?-->)\s*$/m, '$1');

  // HTML heredado -> Markdown
  t = t.replace(/<\/?ul>\s*\n?/g, '');
  t = t.replace(/^\s*<li>([\s\S]*?)<\/li>\s*$/gm, '* $1');
  t = t.replace(/<p>([\s\S]*?)<\/p>/g, '$1');
  t = t.replace(/<\/?p>/g, '');

  // Secciones numeradas: todas H2 y renumeradas de forma consecutiva
  const m = t.match(/^# Capítulo (\d+):/m);
  if (m) {
    const chap = parseInt(m[1], 10);
    let n = 0;
    t = t.replace(/^#{2,4}\s+(\d+)\.(\d+)\s+(.+)$/gm, (_, a, b, title) => {
      n += 1;
      return `## ${chap}.${n} ${title.trim()}`;
    });
  }

  // Encabezados estándar con nivel uniforme (H2)
  t = t.replace(/^#{2,4}\s+(Objetivos de Aprendizaje)\s*$/gm, '## $1');
  t = t.replace(/^#{2,4}\s+(Preguntas de Revisión y Ejercicios Prácticos)\s*$/gm, '## $1');
  t = t.replace(/^#{2,4}\s+(Lecturas Recomendadas)\s*$/gm, '## $1');

  // Líneas en blanco repetidas
  t = t.replace(/\n{4,}/g, '\n\n\n');
  return t.replace(/\s+$/, '') + '\n';
}

if (require.main === module) {
  let changed = 0;
  for (const f of fs.readdirSync(CAP_DIR).filter(f => f.endsWith('.md') && f !== '00-portada.md')) {
    const p = path.join(CAP_DIR, f);
    const before = fs.readFileSync(p, 'utf8');
    const after = normalizeChapter(before);
    if (after !== before.replace(/\r\n/g, '\n')) {
      fs.writeFileSync(p, after, 'utf8');
      changed++;
      console.log('normalizado:', f);
    }
  }
  console.log(`Listo. ${changed} archivos modificados.`);
}

module.exports = { normalizeChapter };
