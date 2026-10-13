const fs = require('fs');
const path = require('path');

const CAP_DIR = 'c:\\Users\\lgalv\\Desktop\\obsidian\\libro\\capitulos';
const files = fs.readdirSync(CAP_DIR).filter(f => f.endsWith('.md'));

console.log(`Normalizando ${files.length} capítulos al formato académico estricto sin emojis...`);

files.forEach(file => {
  const p = path.join(CAP_DIR, file);
  let content = fs.readFileSync(p, 'utf8');

  // Remove all emojis
  content = content.replace(/[\u{1F300}-\u{1F9FF}\u{2600}-\u{26FF}\u{2700}-\u{27BF}]/gu, '');

  // Normalize box titles from chat-style to academic handbook style
  content = content.replace(/ALEGORÍA CONCEPTUAL:\s*/gi, 'RECUADRO CONCEPTUAL: ')
                   .replace(/BAJO EL CAPÓ:\s*/gi, 'PROFUNDIZACIÓN TÉCNICA: ')
                   .replace(/CÓDIGO Y PATRÓN DE PRODUCCIÓN:\s*/gi, 'PATRÓN DE IMPLEMENTACIÓN EN PRODUCCIÓN: ')
                   .replace(/ERRORES COMUNES Y ANTI-PATRONES/gi, 'CONSIDERACIONES CRÍTICAS Y ANTI-PATRONES')
                   .replace(/EJERCICIOS PRÁCTICOS DE CONSOLIDACIÓN/gi, 'EJERCICIOS DE CONSOLIDACIÓN');

  // Strip residual leading spaces in tags
  content = content.replace(/<strong style="[^"]*">\s+/g, '<strong style="color: var(--ink); font-size: 0.95em; letter-spacing: 0.04em; text-transform: uppercase;">');

  fs.writeFileSync(p, content, 'utf8');
});

console.log('Capítulos normalizados exitosamente.');
