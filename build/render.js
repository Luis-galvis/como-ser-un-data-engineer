// Renderizador Markdown -> HTML editorial (marked + KaTeX + highlight.js).
const { Marked, Renderer } = require('marked');
const katex = require('katex');
const hljs = require('highlight.js');

const LANG_LABEL = {
  python: 'Python', py: 'Python', sql: 'SQL', bash: 'Bash', sh: 'Bash', shell: 'Bash',
  yaml: 'YAML', yml: 'YAML', json: 'JSON', hcl: 'HCL (Terraform)', terraform: 'HCL (Terraform)',
  scala: 'Scala', java: 'Java', text: '', txt: '', xml: 'XML', ini: 'INI', toml: 'TOML',
  dockerfile: 'Dockerfile', diff: 'Diff', javascript: 'JavaScript', js: 'JavaScript', avro: 'Avro',
};
const LANG_ALIAS = { hcl: 'ini', terraform: 'ini', py: 'python', sh: 'bash', shell: 'bash', yml: 'yaml', avro: 'json' };

const esc = s => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const slug = s => s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^\w\s-]/g, '').trim().replace(/\s+/g, '-');

function tex(src, display) {
  return katex.renderToString(src, { displayMode: display, throwOnError: false, strict: 'ignore' });
}

const mathBlock = {
  name: 'mathBlock', level: 'block',
  start(src) { const i = src.indexOf('$$'); return i < 0 ? undefined : i; },
  tokenizer(src) {
    const m = /^\$\$([\s\S]+?)\$\$[ \t]*(?:\n|$)/.exec(src);
    if (m) return { type: 'mathBlock', raw: m[0], text: m[1].trim() };
  },
  renderer(t) { return `<div class="math-block">${tex(t.text, true)}</div>\n`; },
};
const mathInline = {
  name: 'mathInline', level: 'inline',
  start(src) { const i = src.indexOf('$'); return i < 0 ? undefined : i; },
  tokenizer(src) {
    let m = /^\$\$([\s\S]+?)\$\$/.exec(src);
    if (m) return { type: 'mathInline', raw: m[0], text: m[1].trim(), display: true };
    m = /^\$(?!\s)((?:\\\$|[^$\n])+?)(?<![\s\\])\$(?!\d)/.exec(src);
    if (m) {
      const inner = m[1];
      // Si empieza con un número, validar que sea fórmula matemática (contiene comandos LaTeX u operadores)
      if (/^\d/.test(inner) && !/[\\^_=+<>]/.test(inner)) {
        return undefined;
      }
      return { type: 'mathInline', raw: m[0], text: inner, display: false };
    }
  },
  renderer(t) { return tex(t.text, t.display); },
};

function highlight(code, lang) {
  const l = LANG_ALIAS[lang] || lang;
  try {
    if (l && hljs.getLanguage(l)) return hljs.highlight(code, { language: l, ignoreIllegals: true }).value;
  } catch (e) { /* cae a texto plano */ }
  return esc(code);
}

function fixMermaid(src) {
  // Etiquetas de arista con símbolos especiales -> entre comillas
  src = src.replace(/(-->|---|-\.->|==>)\|([^|"\n]+)\|/g, '$1|"$2"|');
  // Saltos de línea dentro de etiquetas entre comillas -> <br/>
  return src.replace(/"([^"]*)"/g, (m, inner) => '"' + inner.replace(/\r?\n/g, '<br/>') + '"');
}

const marked = new Marked({ gfm: true, breaks: false });
marked.use({
  extensions: [mathBlock, mathInline],
  renderer: {
    code({ text, lang }) {
      const l = (lang || '').trim().split(/\s+/)[0].toLowerCase();
      if (l === 'mermaid') {
        return `<div class="mermaid" data-src="${esc(fixMermaid(text)).replace(/"/g, '&quot;')}"></div>\n`;
      }
      const lines = text.replace(/\n$/, '').split('\n').length;
      const label = l in LANG_LABEL ? LANG_LABEL[l] : (l ? l.toUpperCase() : '');
      const plain = !l || l === 'text' || l === 'txt';
      const cls = ['code-container', lines > 28 ? 'long' : '', plain ? 'plain' : ''].filter(Boolean).join(' ');
      const head = label ? `<div class="code-header">${label}</div>` : '';
      return `<div class="${cls}">${head}<pre><code>${highlight(text.replace(/\n$/, ''), l)}</code></pre></div>\n`;
    },
    codespan({ text }) {
      return `<code class="inline-code">${text}</code>`;
    },
    heading({ tokens, depth }) {
      const inner = this.parser.parseInline(tokens);
      const plain = inner.replace(/<[^>]+>/g, '');
      return `<h${depth} id="${slug(plain)}">${inner}</h${depth}>\n`;
    },
    blockquote({ tokens }) {
      const body = this.parser.parse(tokens);
      let cls = 'callout';
      if (/^<p><strong>Definici[oó]n/.test(body)) cls += ' definition';
      else if (/^<p><strong>Consideraciones de Producci[oó]n/.test(body)) cls += ' production';
      else if (/^<p><strong>(Teorema|Propiedad|Lema)/.test(body)) cls += ' theorem';
      else if (/^<p><em>["“«]/.test(body)) cls = 'epigraph';
      return `<blockquote class="${cls}">${body}</blockquote>\n`;
    },
    table(token) {
      const html = Renderer.prototype.table.call(this, token);
      return `<div class="table-wrap">${html.replace('<table>', '<table class="academic-table">')}</div>\n`;
    },
    hr() { return '<div class="divider-line"></div>\n'; },
  },
});

// <details> -> caja "Solución" siempre visible (en PDF no hay desplegables).
function expandDetails(md) {
  return md.replace(
    /<details>\s*<summary>([\s\S]*?)<\/summary>([\s\S]*?)(?:<\/details>|(?=\n---\s*\n)|(?=\n#{1,6}\s)|$)/g,
    (_, title, body) => {
      let t = title.replace(/\*\*/g, '').replace(/<[^>]+>/g, '').trim();
      t = t.replace(/^Ver\s+/i, '');
      t = t.charAt(0).toUpperCase() + t.slice(1);
      return `\n\n<div class="solution"><div class="solution-title">${t}</div>\n\n${body.trim()}\n\n</div>\n\n`;
    }
  ).replace(/<\/details>/g, '');
}

function render(md) {
  let src = md.replace(/\r\n/g, '\n').replace(/<!--[\s\S]*?-->/g, '');
  src = expandDetails(src);
  let html = marked.parse(src);
  // Objetivos de aprendizaje en caja propia
  html = html.replace(/(<h2 id="objetivos-de-aprendizaje">[\s\S]*?<\/ul>)/, '<div class="objectives">$1</div>');
  return html;
}

module.exports = { render, esc, slug };
