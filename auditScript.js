const fs = require('fs');
const path = require('path');
const vault = 'D:\\estudio\\estudio';

const confidentialKeywords = [
  'scotia', 'scotiabank', 'scotiatech', 'salario', 'sueldo', '5.8', '5,8', 'cop', 'usd',
  'password', 'secret', 'token', 'api_key', 'apikey', 'credential', 'client_secret',
  '192.168.', '10.0.', '172.16.', 'internal.net', '@gmail', '@scotia', 'luis.galvis'
];

const results = [];
const codeIssues = [];

const files = fs.readdirSync(vault).filter(f => f.endsWith('.md'));

for (const f of files) {
  const p = path.join(vault, f);
  const content = fs.readFileSync(p, 'utf8');
  const lines = content.split(/\r?\n/);
  
  lines.forEach((line, idx) => {
    const l = line.toLowerCase();
    for (const kw of confidentialKeywords) {
      if (l.includes(kw)) {
        // filter out generic keywords like 'Secret' in Delta Lake / Spark docs or 'USD' in generic retail examples unless sensitive
        results.push({ file: f, line: idx + 1, keyword: kw, text: line.trim() });
      }
    }
  });

  // Code blocks check
  const codeBlockRegex = /```([a-zA-Z0-9_-]*)\r?\n([\s\S]*?)```/g;
  let cm;
  while ((cm = codeBlockRegex.exec(content)) !== null) {
    const lang = (cm[1] || '').toLowerCase();
    const code = cm[2];
    
    if (lang === 'python' || lang === 'pyspark' || lang === 'py') {
      const codeLines = code.split(/\r?\n/);
      codeLines.forEach((cline, clidx) => {
        // check withcolumn vs withColumn
        if (cline.includes('.withcolumn(') || cline.includes('withcolumn(')) {
          codeIssues.push({ file: f, lineNum: clidx + 1, issue: 'Casing incorrecto: withcolumn() -> withColumn()', snippet: cline.trim() });
        }
        if (cline.includes('.withcolumnrenamed(') || cline.includes('withcolumnrenamed(')) {
          codeIssues.push({ file: f, lineNum: clidx + 1, issue: 'Casing incorrecto: withcolumnrenamed() -> withColumnRenamed()', snippet: cline.trim() });
        }
        // check unclosed quotes or syntax typos
        if (cline.includes('user_dataframe.withcolumn("dummy_col,lit(1))')) {
          codeIssues.push({ file: f, lineNum: clidx + 1, issue: 'Falta comilla de cierre y paréntesis: ("dummy_col,lit(1)) -> ("dummy_col", lit(1))', snippet: cline.trim() });
        }
        if (cline.includes('.show') && (cline.match(/\(/g) || []).length > (cline.match(/\)/g) || []).length) {
          codeIssues.push({ file: f, lineNum: clidx + 1, issue: 'Paréntesis sin cerrar en .show', snippet: cline.trim() });
        }
      });
    }
  }
}

console.log('=== RESULTADOS DE CONFIDENCIALIDAD ===');
console.log('Total hallazgos brutos:', results.length);
results.forEach(r => {
  console.log(`[${r.file}:${r.line}] (${r.keyword}): ${r.text}`);
});

console.log('\n=== PROBLEMAS DE CÓDIGO REAL ENCONTRADOS ===');
console.log('Total errores de código:', codeIssues.length);
codeIssues.forEach(c => {
  console.log(`[${c.file}:${c.lineNum}] ${c.issue} -> ${c.snippet}`);
});
