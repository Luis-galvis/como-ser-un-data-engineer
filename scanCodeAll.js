const fs = require('fs');
const path = require('path');
const vault = 'D:\\estudio\\estudio';

const files = fs.readdirSync(vault).filter(f => f.endsWith('.md'));
const allCodeIssues = [];

for (const f of files) {
  const p = path.join(vault, f);
  const content = fs.readFileSync(p, 'utf8');
  const lines = content.split(/\r?\n/);

  lines.forEach((line, idx) => {
    const l = line.trim();
    // Check casing mistakes in pyspark methods
    if (/withcolumn\b/i.test(l) && !l.includes('withColumn') && !l.startsWith('#')) {
      allCodeIssues.push({ file: f, line: idx + 1, issue: 'Casing: withcolumn -> withColumn', code: l });
    }
    if (/withcolumnrenamed\b/i.test(l) && !l.includes('withColumnRenamed') && !l.startsWith('#')) {
      allCodeIssues.push({ file: f, line: idx + 1, issue: 'Casing: withcolumnrenamed -> withColumnRenamed', code: l });
    }
    if (/groupby\b/i.test(l) && !l.includes('groupBy') && !l.includes('group by') && !l.startsWith('#') && (l.includes('.groupby') || l.includes('df.groupby'))) {
      allCodeIssues.push({ file: f, line: idx + 1, issue: 'Casing: df.groupby -> df.groupBy', code: l });
    }
    // Check quotes mismatch in expressions like ("dummy_col,
    if (/\(\"[a-zA-Z0-9_-]+,[a-zA-Z0-9_]/.test(l)) {
      allCodeIssues.push({ file: f, line: idx + 1, issue: 'Comillas sin cerrar antes de la coma', code: l });
    }
    // Check missing parens on .show
    if (/\.show\s*$/.test(l) && !l.includes('.show()')) {
      allCodeIssues.push({ file: f, line: idx + 1, issue: 'Falta invocación con paréntesis: .show -> .show()', code: l });
    }
  });
}

console.log('=== LISTA DE CORRECCIONES DE CÓDIGO DETECTADAS ===');
console.log('Total:', allCodeIssues.length);
allCodeIssues.forEach(c => {
  console.log(`[${c.file}:${c.line}] ${c.issue}\n   Snippet original: ${c.code}`);
});
