const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
require('dotenv').config();
const { compileMasterBook } = require('./bookCompiler.js');

const app = express();
const PORT = process.env.PORT || 3333;
let GROQ_API_KEY = process.env.GROQ_API_KEY || '';
let VAULT_PATH = process.env.VAULT_PATH || 'D:\\estudio\\estudio';
let CURRENT_MODEL = process.env.DEFAULT_MODEL || 'qwen/qwen3.8-27b';

// Auto-detect vault directory if needed
function resolveVaultPath(targetPath) {
  if (fs.existsSync(targetPath)) return targetPath;
  const parent = path.dirname(targetPath);
  if (fs.existsSync(parent)) return parent;
  if (fs.existsSync('D:\\estudio')) {
    if (fs.existsSync('D:\\estudio\\estudio')) return 'D:\\estudio\\estudio';
    return 'D:\\estudio';
  }
  return targetPath;
}

VAULT_PATH = resolveVaultPath(VAULT_PATH);

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// Helper: Call Groq API
async function callGroq(messages, temperature = 0.5, model = CURRENT_MODEL, jsonMode = false) {
  if (!GROQ_API_KEY) {
    throw new Error('GROQ_API_KEY no está configurada.');
  }

  const payload = {
    model: model || CURRENT_MODEL,
    messages,
    temperature
  };

  if (jsonMode) {
    payload.response_format = { type: 'json_object' };
  }

  const res = await fetch('https://api.groq.com/openai/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${GROQ_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });

  const data = await res.json();
  if (data.error) {
    throw new Error(data.error.message || 'Error en Groq API');
  }

  return data.choices && data.choices[0] ? data.choices[0].message.content : '';
}

// Helper: Scan Vault recursively
function getVaultFiles(dir, baseDir = dir) {
  let results = [];
  if (!fs.existsSync(dir)) return results;

  const list = fs.readdirSync(dir, { withFileTypes: true });
  for (const item of list) {
    const fullPath = path.join(dir, item.name);
    if (item.isDirectory()) {
      if (item.name === '.obsidian' || item.name === 'node_modules' || item.name === '.git') {
        continue;
      }
      results = results.concat(getVaultFiles(fullPath, baseDir));
    } else if (item.isFile() && item.name.endsWith('.md')) {
      const relPath = path.relative(baseDir, fullPath).replace(/\\/g, '/');
      results.push({
        fullPath,
        relPath,
        name: item.name.replace(/\.md$/, '')
      });
    }
  }
  return results;
}

// Parse markdown metadata, links and tags
function parseNote(fullPath, relPath, name) {
  try {
    const content = fs.readFileSync(fullPath, 'utf8');
    const stats = fs.statSync(fullPath);

    // Extract [[links]]
    const wikilinkRegex = /\[\[([^\]|#]+)(?:#[^\]|]+)?(?:\|[^\]]+)?\]\]/g;
    const links = [];
    let match;
    while ((match = wikilinkRegex.exec(content)) !== null) {
      const target = match[1].trim();
      if (target && !links.includes(target)) {
        links.push(target);
      }
    }

    // Extract #tags (avoid markdown headings like # Heading)
    const tagRegex = /(?:^|\s)#([a-zA-Z0-9_\-\u00C0-\u017F]+)/g;
    const tags = [];
    let tMatch;
    while ((tMatch = tagRegex.exec(content)) !== null) {
      const tag = tMatch[1].trim();
      if (tag && !tags.includes(tag) && !tag.match(/^\d+$/)) {
        tags.push(tag);
      }
    }

    // Excerpt preview (strip frontmatter and headings)
    const cleanContent = content
      .replace(/^---[\s\S]*?---/m, '')
      .replace(/^#+\s+/gm, '')
      .trim();
    const excerpt = cleanContent.slice(0, 160) + (cleanContent.length > 160 ? '...' : '');

    return {
      name,
      relPath,
      fullPath,
      links,
      tags,
      size: stats.size,
      updatedAt: stats.mtime,
      excerpt,
      wordCount: cleanContent.split(/\s+/).filter(Boolean).length
    };
  } catch (err) {
    return {
      name,
      relPath,
      fullPath,
      links: [],
      tags: [],
      size: 0,
      updatedAt: new Date(),
      excerpt: '',
      wordCount: 0
    };
  }
}

// GET /api/config
app.get('/api/config', async (req, res) => {
  let groqOk = false;
  let models = [
    'qwen/qwen3.8-27b',
    'openai/gpt-oss-120b',
    'qwen/qwen3.6-27b',
    'openai/gpt-oss-20b'
  ];

  try {
    const checkRes = await fetch('https://api.groq.com/openai/v1/models', {
      headers: { 'Authorization': `Bearer ${GROQ_API_KEY}` }
    });
    const data = await checkRes.json();
    if (data.data) {
      groqOk = true;
      const apiModels = data.data
        .map(m => m.id)
        .filter(id => !id.includes('whisper') && !id.includes('guard'));
      if (apiModels.length > 0) models = apiModels;
    }
  } catch (e) {
    groqOk = false;
  }

  const vaultExists = fs.existsSync(VAULT_PATH);
  res.json({
    vaultPath: VAULT_PATH,
    vaultExists,
    currentModel: CURRENT_MODEL,
    groqConfigured: !!GROQ_API_KEY,
    groqOk,
    models
  });
});

// POST /api/config
app.post('/api/config', (req, res) => {
  const { vaultPath, model, groqKey } = req.body;
  if (vaultPath) {
    VAULT_PATH = vaultPath;
    if (!fs.existsSync(VAULT_PATH)) {
      try {
        fs.mkdirSync(VAULT_PATH, { recursive: true });
      } catch (e) {}
    }
  }
  if (model) CURRENT_MODEL = model;
  if (groqKey) GROQ_API_KEY = groqKey;

  res.json({
    success: true,
    vaultPath: VAULT_PATH,
    currentModel: CURRENT_MODEL,
    vaultExists: fs.existsSync(VAULT_PATH)
  });
});

// GET /api/notes
app.get('/api/notes', (req, res) => {
  try {
    const files = getVaultFiles(VAULT_PATH);
    const parsedNotes = files.map(f => parseNote(f.fullPath, f.relPath, f.name));

    // Calculate backlinks
    const backlinkMap = {};
    parsedNotes.forEach(note => {
      note.links.forEach(target => {
        const key = target.toLowerCase();
        if (!backlinkMap[key]) backlinkMap[key] = [];
        backlinkMap[key].push(note.name);
      });
    });

    parsedNotes.forEach(note => {
      note.backlinks = backlinkMap[note.name.toLowerCase()] || [];
      note.connectionCount = note.links.length + note.backlinks.length;
    });

    // Sort by recent
    parsedNotes.sort((a, b) => new Date(b.updatedAt) - new Date(a.updatedAt));

    res.json({ notes: parsedNotes, total: parsedNotes.length, vaultPath: VAULT_PATH });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/notes/:filename
app.get('/api/notes/:filename', (req, res) => {
  try {
    const filename = req.params.filename;
    let filePath = path.join(VAULT_PATH, filename.endsWith('.md') ? filename : `${filename}.md`);
    
    // Also try searching in subfolders if not found directly
    if (!fs.existsSync(filePath)) {
      const allFiles = getVaultFiles(VAULT_PATH);
      const found = allFiles.find(f => f.name.toLowerCase() === filename.toLowerCase() || f.relPath.toLowerCase() === filename.toLowerCase());
      if (found) filePath = found.fullPath;
    }

    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: 'Nota no encontrada' });
    }

    const content = fs.readFileSync(filePath, 'utf8');
    const name = path.basename(filePath, '.md');
    const noteMeta = parseNote(filePath, path.relative(VAULT_PATH, filePath), name);

    res.json({
      name,
      relPath: noteMeta.relPath,
      fullPath: filePath,
      content,
      links: noteMeta.links,
      tags: noteMeta.tags,
      updatedAt: noteMeta.updatedAt
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/notes
app.post('/api/notes', (req, res) => {
  try {
    let { title, content, subfolder = '' } = req.body;
    
    // Auto-detect title from content if empty
    if (!title || !title.trim()) {
      if (content && content.trim()) {
        const headingMatch = content.match(/^#+\s*(.+)$/m);
        if (headingMatch && headingMatch[1].trim()) {
          title = headingMatch[1].trim();
        } else {
          const firstLine = content.split('\n').find(l => l.trim().length > 0);
          if (firstLine) title = firstLine.replace(/^[#*\s-]+/, '').trim();
        }
      }
    }

    if (!title || !title.trim()) {
      return res.status(400).json({ error: 'El título es requerido' });
    }

    // Sanitize title for valid filename (strip leading '#' and invalid chars)
    const cleanTitle = title.trim()
      .replace(/^#+\s*/, '')
      .replace(/[\\/:*?"<>|]/g, '-')
      .trim();

    const targetDir = subfolder ? path.join(VAULT_PATH, subfolder) : VAULT_PATH;
    if (!fs.existsSync(targetDir)) {
      fs.mkdirSync(targetDir, { recursive: true });
    }

    const filePath = path.join(targetDir, `${cleanTitle}.md`);
    fs.writeFileSync(filePath, content || '', 'utf8');

    const noteMeta = parseNote(filePath, path.relative(VAULT_PATH, filePath), cleanTitle);
    res.json({ success: true, message: 'Nota guardada exitosamente', note: noteMeta });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/notes/:filename
app.delete('/api/notes/:filename', (req, res) => {
  try {
    const filename = req.params.filename;
    let filePath = path.join(VAULT_PATH, filename.endsWith('.md') ? filename : `${filename}.md`);
    if (!fs.existsSync(filePath)) {
      const allFiles = getVaultFiles(VAULT_PATH);
      const found = allFiles.find(f => f.name.toLowerCase() === filename.toLowerCase());
      if (found) filePath = found.fullPath;
    }

    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      return res.json({ success: true, message: 'Nota eliminada' });
    }
    res.status(404).json({ error: 'Nota no encontrada' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/graph
app.get('/api/graph', (req, res) => {
  try {
    const files = getVaultFiles(VAULT_PATH);
    const parsedNotes = files.map(f => parseNote(f.fullPath, f.relPath, f.name));

    const nodesMap = new Map();
    const edges = [];

    // Add existing notes as nodes
    parsedNotes.forEach(note => {
      nodesMap.set(note.name.toLowerCase(), {
        id: note.name,
        label: note.name,
        type: 'note',
        size: Math.max(16, Math.min(36, 16 + (note.links.length * 3))),
        color: '#8b5cf6', // purple obsidian
        tags: note.tags,
        excerpt: note.excerpt,
        exists: true
      });
    });

    // Create edges and placeholder nodes for linked but uncreated notes
    parsedNotes.forEach(sourceNote => {
      sourceNote.links.forEach(target => {
        const targetKey = target.toLowerCase();
        if (!nodesMap.has(targetKey)) {
          // Placeholder concept node
          nodesMap.set(targetKey, {
            id: target,
            label: target,
            type: 'concept',
            size: 12,
            color: '#06b6d4', // cyan conceptual
            tags: [],
            excerpt: 'Nota conceptual pendiente de crear',
            exists: false
          });
        }

        edges.push({
          id: `${sourceNote.name}->${target}`,
          from: sourceNote.name,
          to: nodesMap.get(targetKey).id,
          arrows: 'to',
          color: { color: 'rgba(139, 92, 246, 0.4)', highlight: '#ec4899' }
        });
      });
    });

    res.json({
      nodes: Array.from(nodesMap.values()),
      edges
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/ai/autolink
// Automatically analyzes user's raw note and inserts [[wikilinks]], finds conceptual relationships
app.post('/api/ai/autolink', async (req, res) => {
  try {
    const { title, content } = req.body;
    if (!content || !content.trim()) {
      return res.status(400).json({ error: 'El contenido es necesario para conectar notas' });
    }

    // Get all existing notes in the vault
    const files = getVaultFiles(VAULT_PATH);
    const existingNotes = files.map(f => f.name);

    const prompt = `
Eres un asistente experto en Obsidian y gestión del conocimiento en red (Zettelkasten / Second Brain).
Tu misión es tomar las notas que el usuario escribe de forma rápida y natural, y enriquecerlas conectándolas inteligentemente a su red neuronal de notas.

NOTAS EXISTENTES EN LA BÓVEDA DEL USUARIO:
${existingNotes.length > 0 ? existingNotes.map(n => `- "${n}"`).join('\n') : '(Aún no hay notas previas; esta es de las primeras notas)'}

TÍTULO DE LA NOTA ACTUAL: "${title || 'Sin Título'}"

TEXTO ESCRITO POR EL USUARIO:
"""
${content}
"""

INSTRUCCIONES:
1. CORRECCIÓN ORTOGRÁFICA Y GRAMATICAL RESPETUOSA (Muy importante):
   - Revisa la ortografía: coloca tildes faltantes (ej: "existia" -> "existía", "mas" -> "más", "volumenes" -> "volúmenes"), corrige errores tipográficos/de dedo y ajusta mayúsculas y puntuación básica.
   - REGLA FUNDAMENTAL: NO modifiques el estilo, el tono ni la estructura de las oraciones del usuario. No uses lenguaje rebuscado, acartonado ni formal. Debe sonar exactamente como el usuario lo escribió, pero con ortografía impecable.
2. ENLACES INTELIGENTES DE OBSIDIAN ([[...]]):
   - Identifica conceptos clave y términos técnicos y enlázalos usando la sintaxis de Obsidian [[NombreDeNota]].
   - Prioriza enlazar con notas existentes si guardan relación temática.
   - Si hay conceptos clave, paradigmas o tecnologías importantes que merecen ser sus propias notas, enlázalos también con [[Nombre De Nota]] (en mayúscula inicial y nombres limpios).
   - No satures cada palabra; enlaza únicamente los conceptos relevantes para mantener una lectura fluida.
3. CONEXIONES Y SUGERENCIAS:
   - Sugiere 2 a 4 títulos de nuevas notas que complementarían perfectamente esta nota en su red.
   - Sugiere 2 a 5 etiquetas #tag útiles para Obsidian.
   - Proporciona una breve frase explicando qué conexiones conceptuales aporta esta nota a la red.

RESPONDE EXCLUSIVAMENTE EN FORMATO JSON VÁLIDO con la siguiente estructura:
{
  "enhancedContent": "El texto del usuario con ortografía corregida y los [[enlaces]] incorporados de forma limpia",
  "newLinksCreated": ["Concepto1", "Concepto2"],
  "suggestedNewNotes": ["Título sugerido 1", "Título sugerido 2"],
  "suggestedTags": ["#estudio", "#concepto"],
  "neuralInsight": "Breve explicación de las conexiones y por qué son valiosas"
}
`;

    const rawResponse = await callGroq([
      { role: 'system', content: 'Eres un sistema de indexación y red neuronal para Obsidian. Siempre devuelves JSON estricto.' },
      { role: 'user', content: prompt }
    ], 0.3, CURRENT_MODEL, true);

    let parsed;
    try {
      parsed = JSON.parse(rawResponse);
    } catch (e) {
      // Fallback if not strict JSON
      const jsonMatch = rawResponse.match(/\{[\s\S]*\}/);
      if (jsonMatch) parsed = JSON.parse(jsonMatch[0]);
      else throw new Error('No se pudo decodificar la respuesta JSON de la IA');
    }

    res.json(parsed);
  } catch (err) {
    console.error('Error in autolink:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/ai/expand
// Takes an idea or brief text and expands it into a fully structured Obsidian note
app.post('/api/ai/expand', async (req, res) => {
  try {
    const { title, idea } = req.body;
    const files = getVaultFiles(VAULT_PATH);
    const existingNotes = files.map(f => f.name).slice(0, 30);

    const prompt = `
Eres un asistente creador de notas para Obsidian.
El usuario tiene esta idea o título rápido: "${title}":
${idea ? `Contexto o boceto:\n${idea}` : ''}

NOTAS EXISTENTES EN SU BÓVEDA:
${existingNotes.map(n => `- [[${n}]]`).join(', ') || 'Ninguna aún'}

Genera una nota en Markdown de alta calidad para Obsidian que incluya:
- Un encabezado claro # ${title}
- Resumen conceptual clave
- Puntos principales bien desglosados con viñetas y explicaciones concisas
- Enlaces de Obsidian [[NombreDeNota]] para conectar con notas existentes o crear nuevos nodos conceptuales
- Sección "## Notas Relacionadas" al final con enlaces [[...]]
- Sección de etiquetas (#tags)

Devuelve SOLO el contenido en Markdown, sin rodeos ni explicaciones antes o después.
`;

    const generatedMarkdown = await callGroq([
      { role: 'system', content: 'Eres un experto en Obsidian y síntesis de conocimiento.' },
      { role: 'user', content: prompt }
    ], 0.6, CURRENT_MODEL, false);

    res.json({ content: generatedMarkdown.trim() });
  } catch (err) {
    console.error('Error in expand:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/ai/generate-linked-note
// Generates a complete, high-quality note filled with contextual knowledge from the current note
app.post('/api/ai/generate-linked-note', async (req, res) => {
  try {
    const { title, sourceNoteTitle, sourceNoteContent } = req.body;
    if (!title || !title.trim()) {
      return res.status(400).json({ error: 'El título de la nota es requerido' });
    }

    const files = getVaultFiles(VAULT_PATH);
    const existingNotes = files.map(f => f.name).slice(0, 35);

    const prompt = `
Eres un asistente experto en creación de notas de estudio para Obsidian (sistema Zettelkasten).
El usuario quiere crear una nota completa y bien documentada para: "${title}".

CONTEXTO DE ORIGEN:
${sourceNoteTitle ? `Esta nota nace o se relaciona directamente con la nota: [[${sourceNoteTitle}]]` : ''}
${sourceNoteContent ? `Fragmento de la nota origen:\n"""${sourceNoteContent.slice(0, 1000)}"""` : ''}

NOTAS EXISTENTES EN LA BÓVEDA DEL USUARIO:
${existingNotes.map(n => `- [[${n}]]`).join(', ') || 'Ninguna aún'}

INSTRUCCIONES PARA LA NOTA:
1. Genera una nota completa, clara, concisa y pedagógica en Markdown. NO crees un cascarón vacío; explica el concepto a fondo pero directo al grano.
2. Comienza con: # ${title}
3. Breve definición de 2-3 líneas explicando qué es y por qué existe en este ecosistema.
4. Sección "## Conceptos Clave y Funcionamiento": puntos con viñetas explicando sus partes o proceso.
5. Usa enlaces de Obsidian [[...]] para conectar de vuelta con ${sourceNoteTitle ? `[[${sourceNoteTitle}]]` : 'las notas relevantes'} y otros conceptos técnicos que tengan sentido.
6. Agrega una sección "## Notas Relacionadas" con enlaces en viñetas.
7. Agrega etiquetas al final (ej: #tecnologia #concepto).
8. NO uses introducciones como "Aquí tienes tu nota". Devuelve SOLAMENTE el texto en Markdown listo para guardar en Obsidian.
`;

    const content = await callGroq([
      { role: 'system', content: 'Eres un generador de notas atómicas de alta calidad para Obsidian. Solo devuelves Markdown puro.' },
      { role: 'user', content: prompt }
    ], 0.5, CURRENT_MODEL, false);

    // Save directly to the vault!
    const cleanTitle = title.trim().replace(/^#+\s*/, '').replace(/[\\/:*?"<>|]/g, '-').trim();
    const filePath = path.join(VAULT_PATH, `${cleanTitle}.md`);
    fs.writeFileSync(filePath, content.trim(), 'utf8');

    const noteMeta = parseNote(filePath, path.relative(VAULT_PATH, filePath), cleanTitle);

    res.json({
      success: true,
      title: cleanTitle,
      content: content.trim(),
      note: noteMeta
    });
  } catch (err) {
    console.error('Error in generate-linked-note:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/vault/gaps
// Analyzes the vault for uncreated referenced links, empty notes, and isolated concepts
app.get('/api/vault/gaps', (req, res) => {
  try {
    const files = getVaultFiles(VAULT_PATH);
    const existing = new Set(files.map(f => f.name.toLowerCase()));
    const emptyNotes = [];
    const missingMap = {};

    for (const f of files) {
      const content = fs.readFileSync(f.fullPath, 'utf8');
      if (content.trim().length < 50) {
        emptyNotes.push({
          name: f.name,
          relPath: f.relPath,
          size: content.trim().length
        });
      }
      const rx = /\[\[([^\]|#]+)\]\]/g;
      let m;
      while ((m = rx.exec(content)) !== null) {
        const target = m[1].trim();
        if (!existing.has(target.toLowerCase())) {
          if (!missingMap[target]) missingMap[target] = [];
          if (!missingMap[target].includes(f.name)) missingMap[target].push(f.name);
        }
      }
    }

    const missingNotes = Object.keys(missingMap).map(title => ({
      title,
      referencedBy: missingMap[title],
      referenceCount: missingMap[title].length
    })).sort((a, b) => b.referenceCount - a.referenceCount);

    res.json({
      missingNotes,
      emptyNotes,
      totalGaps: missingNotes.length + emptyNotes.length
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/ai/batch-fill-gaps
// Automatically generates and saves multiple missing/empty notes using AI
app.post('/api/ai/batch-fill-gaps', async (req, res) => {
  try {
    const { titles } = req.body;
    if (!titles || !Array.isArray(titles) || titles.length === 0) {
      return res.status(400).json({ error: 'Lista de notas requerida' });
    }

    const files = getVaultFiles(VAULT_PATH);
    const existingNotes = files.map(f => f.name).slice(0, 30);
    const results = [];
    const targetTitles = titles.slice(0, 15);

    // Helper to generate a single note
    async function generateOne(itemTitle) {
      try {
        const prompt = `
Eres un generador de notas atómicas de alta calidad para Obsidian (estudio de Big Data, Spark y Computación Distribuida).
El usuario necesita la nota: "${itemTitle}".

NOTAS EN SU BÓVEDA ACTUAL:
${existingNotes.map(n => `- [[${n}]]`).join(', ')}

INSTRUCCIONES:
1. Redacta una nota completa, pedagógica y clara en Markdown sobre "${itemTitle}".
2. Título # ${itemTitle}
3. Resumen y definición conceptual de qué es y por qué es fundamental en este ecosistema.
4. Sección "## Conceptos Clave y Funcionamiento": puntos con viñetas claros y concisos.
5. Sección "## Notas Relacionadas": enlaces usando [[...]] que conecten con los conceptos de su estudio (como Spark, PySpark, etc).
6. Etiquetas #tag al final.
7. Devuelve SOLO Markdown puro, sin introducciones ni despedidas.
`;

        const content = await callGroq([
          { role: 'system', content: 'Eres un experto en Obsidian y síntesis de conocimiento.' },
          { role: 'user', content: prompt }
        ], 0.5, CURRENT_MODEL, false);

        const cleanTitle = itemTitle.trim().replace(/^#+\s*/, '').replace(/[\\/:*?"<>|]/g, '-').trim();
        const filePath = path.join(VAULT_PATH, `${cleanTitle}.md`);
        fs.writeFileSync(filePath, content.trim(), 'utf8');

        return { title: cleanTitle, success: true };
      } catch (err) {
        console.error(`Error generando ${itemTitle}:`, err);
        return { title: itemTitle, success: false, error: err.message };
      }
    }

    // Process in batches of 3 concurrently
    for (let i = 0; i < targetTitles.length; i += 3) {
      const chunk = targetTitles.slice(i, i + 3);
      const chunkResults = await Promise.all(chunk.map(t => generateOne(t)));
      results.push(...chunkResults);
    }

    res.json({
      success: true,
      generatedCount: results.filter(r => r.success).length,
      notes: results
    });
  } catch (err) {
    console.error('Error in batch-fill-gaps:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/ai/editorial-transform
// Transforms raw technical notes into a commercial, book-quality chapter ready for publishing/selling
app.post('/api/ai/editorial-transform', async (req, res) => {
  try {
    const { title, content, author = 'Luis' } = req.body;
    if (!content || !content.trim()) {
      return res.status(400).json({ error: 'El contenido de la nota es requerido' });
    }

    const prompt = `
Eres un editor técnico de élite y autor de libros de informática de alto impacto (estilo O'Reilly, Manning y Leanpub).
Tu objetivo es transformar la siguiente nota técnica en un **Capítulo de Libro / Manual Técnico Vendible** de máxima calidad pedagógica y editorial en Markdown.

TÍTULO DEL CAPÍTULO: "${title || 'Manual de Ingeniería de Datos'}"
AUTOR: "${author}"

CONTENIDO TÉCNICO BASE:
"""
${content}
"""

REGLAS EDITORIALES DE PUBLICACIÓN:
1. Formato de Portada de Capítulo:
   # Capítulo: ${title}
   > **Autor:** ${author} | **Nivel:** De 0 a Senior | **Especialidad:** Data Engineering & Cloud Data Platforms
2. Introducción y Por qué Importa (visión ejecutiva: qué problema de negocio resuelve y por qué esta habilidad se paga tan bien en el mercado).
3. Analogía Visual del Mundo Real (para que el estudiante entienda el concepto intuitivamente antes del código).
4. Arquitectura y Mecanismo Interno (explicación paso a paso de qué pasa por debajo).
5. Laboratorio Práctico con Código Comentado (código real en Python, PySpark, SQL o Cloud según corresponda, listo para copiar y correr).
6. Caja de Buenas Prácticas: "> [!TIP] Consejo Senior de Producción" (trucos de optimización y experiencia real).
7. Caja de Trampas: "> [!WARNING] Errores Comunes de Novatos" (lo que causa Out of Memory, caídas o sobrecostos en cloud).
8. Caja de Entrevista Técnica: "> [!IMPORTANT] Pregunta Típica de Entrevista ($10M+ COP / $4k+ USD)" y cómo responderla con autoridad.
9. Checklist de Dominio (3-4 puntos clave para que el estudiante verifique si ya domina el tema).
10. Mantén los enlaces de Obsidian [[...]] para mantener la navegabilidad del libro.

Devuelve SOLAMENTE el contenido en Markdown, listo para exportar a PDF editorial.
`;

    const transformedContent = await callGroq([
      { role: 'system', content: 'Eres un editor y autor de manuales técnicos profesionales. Solo devuelves Markdown editorial impecable.' },
      { role: 'user', content: prompt }
    ], 0.5, CURRENT_MODEL, false);

    res.json({
      success: true,
      title: title || 'Capítulo Editorial',
      content: transformedContent.trim()
    });
  } catch (err) {
    console.error('Error in editorial-transform:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/ai/chat
// Conversational AI that knows all vault notes, acts as a neural brain assistant
app.post('/api/ai/chat', async (req, res) => {
  try {
    const { messages, currentNoteName, currentNoteContent } = req.body;
    if (!messages || !Array.isArray(messages)) {
      return res.status(400).json({ error: 'Mensajes inválidos' });
    }

    // Build Vault Knowledge Base summary
    const files = getVaultFiles(VAULT_PATH);
    const notesSummary = files.map(f => {
      const p = parseNote(f.fullPath, f.relPath, f.name);
      return {
        title: p.name,
        links: p.links,
        tags: p.tags,
        excerpt: p.excerpt
      };
    });

    const vaultContext = `
CONTEXTO DE LA RED NEURONAL DE NOTAS (Bóveda del usuario: "${VAULT_PATH}"):
Total de notas en la bóveda: ${notesSummary.length}

ÍNDICE DE NOTAS Y CONEXIONES:
${notesSummary.length > 0 ? notesSummary.map(n => `- [[${n.title}]]: ${n.excerpt} | Enlaza a: ${n.links.map(l => `[[${l}]]`).join(', ') || 'sin enlaces'} | Tags: ${n.tags.join(' ')}`).join('\n') : '(La bóveda aún está vacía o empezando)'}

${currentNoteName ? `NOTA QUE EL USUARIO ESTÁ EDITANDO ACTUALMENTE: [[${currentNoteName}]]\nContenido actual:\n${currentNoteContent || ''}` : ''}
`;

    const systemPrompt = `
Eres "Neural Mind", el copiloto de inteligencia artificial del cerebro digital del usuario en Obsidian.
Tienes acceso a la red neuronal completa de notas de su bóveda.

TUS CAPACIDADES:
1. Responde preguntas basándote en lo que el usuario ha escrito en sus notas, explicando cómo se conectan los conceptos.
2. Cada vez que menciones una nota existente o sugieras un concepto clave para su bóveda, usa la sintaxis de Obsidian [[NombreDeNota]].
3. Identifica lagunas de conocimiento ("gaps"), ideas huérfanas sin conectar y sugiere activamente nuevas conexiones entre notas que el usuario quizás no ha notado.
4. Si el usuario te pide crear una nota o desarrollar un tema, dale una estructura completa en Markdown lista para guardar.
5. Sé conciso, inspirador, inteligente y claro.
`;

    const conversationMessages = [
      { role: 'system', content: systemPrompt + '\n' + vaultContext },
      ...messages
    ];

    const reply = await callGroq(conversationMessages, 0.6, CURRENT_MODEL, false);

    // Extract suggested notes from reply
    const linkMatches = [];
    const rx = /\[\[([^\]|#]+)\]\]/g;
    let m;
    while ((m = rx.exec(reply)) !== null) {
      if (!linkMatches.includes(m[1].trim())) linkMatches.push(m[1].trim());
    }

    res.json({
      reply,
      detectedLinks: linkMatches
    });
  } catch (err) {
    console.error('Error in chat:', err);
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// 📚 API: LIBRO MAESTRO (COMPILACIÓN Y STATS)
// ==========================================

app.get('/api/book/stats', (req, res) => {
  try {
    const bookFileName = '📚 Libro Maestro - Data Engineering de Cero a Senior.md';
    const filePath = path.join(VAULT_PATH, bookFileName);
    if (!fs.existsSync(filePath)) {
      return res.json({
        exists: false,
        fileName: bookFileName
      });
    }
    const stat = fs.statSync(filePath);
    const content = fs.readFileSync(filePath, 'utf8');
    const wordCount = content.split(/\s+/).filter(Boolean).length;
    const metaMatch = content.match(/Total de Capítulos y Secciones:\*\*\s*(\d+)/);
    const chapterCount = metaMatch ? parseInt(metaMatch[1], 10) : (content.match(/## 📖 Capítulo:/g) || []).length + (content.match(/### 📑 Anexo:/g) || []).length;
    res.json({
      exists: true,
      fileName: bookFileName,
      filePath,
      updatedAt: stat.mtime,
      totalChapters: chapterCount,
      totalWordCount: wordCount,
      estimatedPages: Math.round(wordCount / 280),
      fileSizeKb: Math.round(stat.size / 1024),
      markdown: content
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/book/compile', (req, res) => {
  try {
    const author = req.body?.author || 'Luis Galvis';
    const result = compileMasterBook(VAULT_PATH, author);
    res.json({
      success: true,
      message: 'Libro maestro compilado exitosamente con todas las notas de la bóveda.',
      ...result
    });
  } catch (err) {
    console.error('Error compiling book:', err);
    res.status(500).json({ error: err.message });
  }
});

// Download book file directly as Markdown (.md)
app.get('/api/book/download', (req, res) => {
  try {
    const bookFileName = '📚 Libro Maestro - Data Engineering de Cero a Senior.md';
    const filePath = path.join(VAULT_PATH, bookFileName);
    if (!fs.existsSync(filePath)) {
      compileMasterBook(VAULT_PATH, 'Luis Galvis');
    }
    res.download(filePath, 'La_Enciclopedia_Definitiva_Data_Engineering_Luis_Galvis.md');
  } catch (err) {
    res.status(500).send('Error descargando el libro: ' + err.message);
  }
});

// Dedicated editorial printable HTML view for 1-click PDF export
app.get('/book/print', (req, res) => {
  try {
    const bookFileName = '📚 Libro Maestro - Data Engineering de Cero a Senior.md';
    const filePath = path.join(VAULT_PATH, bookFileName);
    if (!fs.existsSync(filePath)) {
      compileMasterBook(VAULT_PATH, 'Luis Galvis');
    }
    const markdownContent = fs.readFileSync(filePath, 'utf8');

    const html = `<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>La Enciclopedia Definitiva de Data Engineering - Luis Galvis</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
  <style>
    @page {
      size: letter;
      margin: 18mm 16mm 20mm 16mm;
    }
    * { box-sizing: border-box; }
    body {
      font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      color: #0f172a;
      background: #f8fafc;
      margin: 0;
      padding: 0;
      line-height: 1.7;
      font-size: 11pt;
    }
    .print-control-bar {
      position: sticky;
      top: 0;
      background: #0f172a;
      color: #f8fafc;
      padding: 12px 24px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      z-index: 9999;
      box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    }
    .print-bar-title {
      font-weight: 700;
      font-size: 14px;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .print-bar-actions {
      display: flex;
      gap: 12px;
    }
    .btn-print {
      background: #6366f1;
      color: #fff;
      border: none;
      padding: 8px 18px;
      border-radius: 6px;
      font-weight: 600;
      cursor: pointer;
      font-size: 13px;
      transition: all 0.2s;
    }
    .btn-print:hover { background: #4f46e5; }
    .btn-close {
      background: #334155;
      color: #fff;
      border: none;
      padding: 8px 14px;
      border-radius: 6px;
      cursor: pointer;
      font-size: 13px;
    }
    .book-wrapper {
      max-width: 900px;
      margin: 24px auto;
      background: #fff;
      padding: 48px 60px;
      border-radius: 12px;
      box-shadow: 0 4px 25px rgba(0,0,0,0.06);
    }
    h1 {
      font-size: 24pt;
      font-weight: 800;
      color: #0f172a;
      border-bottom: 2pt solid #6366f1;
      padding-bottom: 8pt;
      margin-top: 36pt;
      margin-bottom: 16pt;
      line-height: 1.25;
      page-break-before: always;
    }
    h1:first-of-type {
      page-break-before: avoid;
      margin-top: 0;
    }
    h2 {
      font-size: 16pt;
      font-weight: 700;
      color: #1e293b;
      border-bottom: 1px solid #cbd5e1;
      padding-bottom: 4pt;
      margin-top: 24pt;
      margin-bottom: 12pt;
      page-break-after: avoid;
    }
    h3 {
      font-size: 13pt;
      font-weight: 600;
      color: #334155;
      margin-top: 18pt;
      margin-bottom: 8pt;
      page-break-after: avoid;
    }
    h4 {
      font-size: 11pt;
      font-weight: 600;
      color: #475569;
      margin-top: 14pt;
      margin-bottom: 6pt;
    }
    p { margin-bottom: 10pt; }
    ul, ol { margin-bottom: 12pt; padding-left: 20pt; }
    li { margin-bottom: 4pt; }
    pre {
      background: #f1f5f9;
      border: 1px solid #e2e8f0;
      padding: 12px 16px;
      border-radius: 6px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 9.5pt;
      line-height: 1.5;
      page-break-inside: avoid;
      overflow-x: auto;
      margin-bottom: 14pt;
    }
    code {
      font-family: 'JetBrains Mono', monospace;
      background: #f1f5f9;
      border: 1px solid #e2e8f0;
      padding: 2px 5px;
      border-radius: 4px;
      font-size: 9pt;
      color: #0f172a;
    }
    blockquote {
      border-left: 4px solid #6366f1;
      padding-left: 16px;
      margin-left: 0;
      margin-right: 0;
      margin-bottom: 12pt;
      color: #475569;
      font-style: italic;
      background: #f8fafc;
      padding-top: 8px;
      padding-bottom: 8px;
      border-radius: 0 6px 6px 0;
    }
    hr {
      border: none;
      border-top: 1px solid #e2e8f0;
      margin: 24pt 0;
    }
    a {
      color: #4f46e5;
      text-decoration: none;
    }
    .book-part-break, .book-chapter-break {
      page-break-before: always;
    }
    .obsidian-link {
      background: #eef2ff;
      color: #4338ca;
      padding: 1px 6px;
      border-radius: 4px;
      border: 1px solid #c7d2fe;
      font-size: 10pt;
    }
    @media print {
      body { background: #fff !important; }
      .print-control-bar { display: none !important; }
      .book-wrapper {
        max-width: 100% !important;
        margin: 0 !important;
        padding: 0 !important;
        box-shadow: none !important;
        border-radius: 0 !important;
      }
    }
  </style>
</head>
<body>
  <div class="print-control-bar">
    <div class="print-bar-title">
      <span>📚</span>
      <span>La Enciclopedia Definitiva de Data Engineering — <strong>Autor: Luis Galvis</strong></span>
    </div>
    <div class="print-bar-actions">
      <button class="btn-print" onclick="window.print()">🖨️ Imprimir / Guardar en PDF</button>
      <button class="btn-close" onclick="window.close()">Cerrar Ventana</button>
    </div>
  </div>

  <div class="book-wrapper" id="bookTarget">
    Cargando libro maestro editorial...
  </div>

  <script>
    const rawMarkdown = ${JSON.stringify(markdownContent)};
    let html = marked.parse(rawMarkdown);
    html = html.replace(/\\[\\[([^\\]|#]+)(?:\\|([^\\]]+))?\\]\\]/g, (match, note, alias) => {
      return '<span class="obsidian-link">🔗 ' + (alias || note) + '</span>';
    });
    document.getElementById('bookTarget').innerHTML = html;

    // Trigger print automatically if ?auto=1 in URL
    if (window.location.search.includes('auto=1')) {
      setTimeout(() => { window.print(); }, 600);
    }
  </script>
</body>
</html>`;

    res.send(html);
  } catch (err) {
    res.status(500).send('Error generando vista de impresión: ' + err.message);
  }
});

// Start Server
app.listen(PORT, () => {
  console.log(`===============================================`);
  console.log(`🧠 Obsidian Neural Mind corriendo en:`);
  console.log(`👉 http://localhost:${PORT}`);
  console.log(`📁 Bóveda vinculada: ${VAULT_PATH}`);
  console.log(`🤖 Modelo Groq activo: ${CURRENT_MODEL}`);
  console.log(`===============================================`);
});
