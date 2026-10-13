const fs = require('fs');
const path = require('path');

const CATEGORIES = {
  'Parte I: Fundamentos y SQL Analítico': [
    'SQL.md',
    'AGRUPACIONES DE DATOS Y FUNCIONES DE AGREGACION.md',
    'Tipos de Datos y Casting en Spark.md',
    'Filter y Condicionales en DataFrames.md',
    'Filter.md',
    'with column y withcolum rename.md',
    'como abrir archivos y cargar en pyspark.md',
    'Funciones de Transformación en PySpark.md'
  ],
  'Parte II: El Motor de Cómputo: Apache Spark y PySpark': [
    'Hadoop.md',
    'MapReduce.md',
    'Spark.md',
    'Apache Spark.md',
    'PySpark.md',
    'spark session.md',
    'conf spark.md',
    'SparkConf.md',
    'dataframes.md',
    'Cluster Computing.md',
    'Cluster Manager.md',
    'Driver Program.md',
    'Worker Node.md',
    'Ejecutores.md'
  ],
  'Parte III: Arquitectura Interna y Optimización de Rendimiento': [
    'RDD.md',
    'DAG.md',
    'lazy evaluation.md',
    'Acción.md',
    'Spark SQL.md',
    'Spark SQL y Catalyst Optimizer.md',
    'Cache.md',
    'Memoria RAM.md',
    'Spark Memory Management.md',
    'Partitioning y Shuffling en Spark.md',
    'Cloud Cost Optimization in Spark.md'
  ],
  'Parte IV: Arquitectura Lakehouse, Delta Lake y Modelado': [
    'Cloud Computing.md',
    'Databricks.md',
    'Delta Lake.md',
    'Medallion Architecture.md',
    'Dimensional Modeling.md',
    'Modelado Dimensional en Lakehouse.md',
    'Data Modeling Patterns.md'
  ],
  'Parte V: Gobernanza, Seguridad y Linaje de Datos': [
    'Unity Catalog.md',
    'Data Cataloging.md',
    'Data Lineage and Governance.md'
  ],
  'Parte VI: Ingesta, Streaming y Tiempo Real': [
    'Streaming.md',
    'Spark Structured Streaming.md',
    'Apache Kafka.md',
    'Change Data Capture (CDC).md',
    'Ingesta Batch vs Streaming y CDC.md',
    'Streaming Data Quality Patterns.md'
  ],
  'Parte VII: Orquestación Moderna de Pipelines': [
    'SSIS.md',
    'ETL Pipelines.md',
    'Azure Data Factory.md',
    'dbt.md',
    'Apache Airflow vs Azure Data Factory.md',
    'Orquestación de Pipelines Hybrid.md'
  ],
  'Parte VIII: Data Quality, Observabilidad y DataOps': [
    'Data Quality.md',
    'Data Quality Framework.md',
    'Great Expectations.md',
    'Integración de Data Quality en ETL.md',
    'Data Observability.md',
    'CI-CD for Data Pipelines.md',
    'DataOps y CI-CD para Data Engineering.md',
    'Terraform for Data Engineering.md'
  ],
  'Parte IX: System Design, Roadmap y Preparación Senior': [
    'Machine Learning.md',
    'Graph Processing.md',
    'Data Engineering Roadmap.md',
    'Data Engineer Interview Guide y System Design.md'
  ]
};

// Clean sensitive/personal info and remove AI/metadata residues
function sanitizeContent(rawText) {
  let text = rawText;
  // Remove salaries or specific personal numbers
  text = text.replace(/\$5\.8M\s*COP/gi, '[Rango Salarial Junior]');
  text = text.replace(/\$12-13M\s*COP/gi, '[Rango Salarial Senior $12M+]');
  text = text.replace(/\$10-14M/gi, '[Rango Salarial Senior]');
  
  // Remove any reference to AI, bots or automated note generation
  text = text.replace(/Concepto generado a partir de la red de notas\.?/gi, '');
  text = text.replace(/generado por ia/gi, '');
  text = text.replace(/red neuronal de notas/gi, 'compendio de ingeniería');
  text = text.replace(/red neuronal/gi, 'arquitectura');
  
  // Remove raw hashtag lines at the bottom of notes (e.g. #bigdata #spark #sql)
  text = text.replace(/(?:^|\n)\s*(?:#[a-zA-Z0-9_\-áéíóúÁÉÍÓÚ]+\s*)+$/gm, '');
  
  // Clean frontmatter if any
  text = text.replace(/^---[\s\S]*?---/m, '');
  return text.trim();
}

// Slug generator for Table of Contents anchors
function slugify(text) {
  return text.toLowerCase()
    .replace(/[^\w\s-]/g, '')
    .trim()
    .replace(/[\s_]+/g, '-');
}

function compileMasterBook(vaultPath, author = 'Luis Galvis') {
  if (!fs.existsSync(vaultPath)) {
    throw new Error(`La ruta de la bóveda "${vaultPath}" no existe.`);
  }

  const allFiles = fs.readdirSync(vaultPath).filter(f => f.endsWith('.md'));
  const processedFiles = new Set();
  
  // Excluded notes
  const excludedNotes = [
    'PATH CARRER.md',
    'Master Course - Data Engineering de Cero a Senior 100.md',
    '📚 Libro Maestro - Data Engineering de Cero a Senior.md'
  ];

  let bookSections = [];
  let tocItems = [];
  let totalWordCount = 0;
  let totalChapters = 0;

  // Process categorized parts
  for (const [partName, fileList] of Object.entries(CATEGORIES)) {
    let partContent = [];
    const partSlug = slugify(partName);
    
    tocItems.push({
      type: 'part',
      title: partName,
      slug: partSlug,
      chapters: []
    });

    for (const fileName of fileList) {
      const fullPath = path.join(vaultPath, fileName);
      if (fs.existsSync(fullPath) && !excludedNotes.includes(fileName)) {
        processedFiles.add(fileName);
        totalChapters++;

        const title = fileName.replace(/\.md$/, '');
        const rawContent = fs.readFileSync(fullPath, 'utf8');
        const cleanContent = sanitizeContent(rawContent);
        const words = cleanContent.split(/\s+/).filter(Boolean).length;
        totalWordCount += words;

        const chapterSlug = slugify(`capitulo-${title}`);
        tocItems[tocItems.length - 1].chapters.push({
          title,
          slug: chapterSlug
        });

        // Demote headings inside note so they fit nicely under chapter H2
        let adjustedContent = cleanContent
          .replace(/^#\s+(.+)$/gm, '### $1')
          .replace(/^##\s+(.+)$/gm, '#### $1');

        partContent.push(`
<a id="${chapterSlug}"></a>
## 📖 Capítulo: ${title}

${adjustedContent}

---
`);
      }
    }

    if (partContent.length > 0) {
      bookSections.push(`
<a id="${partSlug}"></a>
# ${partName}

> *Este bloque profundiza en los principios de arquitectura, patrones de diseño y aplicación práctica en producción.*

${partContent.join('\n')}
`);
    }
  }

  // Any remaining notes in vault not yet categorized (Appendices)
  let appendixContent = [];
  for (const fileName of allFiles) {
    if (!processedFiles.has(fileName) && !excludedNotes.includes(fileName)) {
      const fullPath = path.join(vaultPath, fileName);
      const title = fileName.replace(/\.md$/, '');
      const rawContent = fs.readFileSync(fullPath, 'utf8');
      if (rawContent.trim().length > 30) {
        totalChapters++;
        const cleanContent = sanitizeContent(rawContent);
        const words = cleanContent.split(/\s+/).filter(Boolean).length;
        totalWordCount += words;

        const chapterSlug = slugify(`apendice-${title}`);
        appendixContent.push(`
<a id="${chapterSlug}"></a>
### 📑 Anexo: ${title}

${cleanContent.replace(/^#\s+(.+)$/gm, '#### $1')}

---
`);
      }
    }
  }

  if (appendixContent.length > 0) {
    bookSections.push(`
# 📎 Apéndice: Notas y Casos Complementarios

${appendixContent.join('\n')}
`);
  }

  // Build Table of Contents Markdown
  let tocMarkdown = `## 📑 Tabla de Contenidos General\n\n`;
  tocItems.forEach(part => {
    tocMarkdown += `- **[${part.title}](#${part.slug})**\n`;
    part.chapters.forEach(ch => {
      tocMarkdown += `  - [${ch.title}](#${ch.slug})\n`;
    });
  });

  const now = new Date();
  const dateStr = now.toLocaleDateString('es-CO', { year: 'numeric', month: 'long', day: 'numeric' });

  // Book Header / Frontmatter
  const bookHeader = `# 📚 La Enciclopedia Definitiva de Data Engineering: De Cero a Senior
> **Guía Maestra de Arquitectura de Datos, Cómputo Distribuido, Lakehouse, Streaming y Cloud Platforms**  
> **Escrito por:** ${author} | **Edición Oficial de Autor** | **Año:** ${now.getFullYear()}

---

## ✍️ Prefacio y Dedicatoria
*Por ${author}*

Bienvenido a esta obra de ingeniería y arquitectura de datos. Este libro no es una colección de notas desordenadas ni un tutorial superficial; es el compendio definitivo nacido de cientos de horas de diseño de sistemas distribuidos, resolución de problemas críticos en producción a escala empresarial y la convicción de que la ingeniería de datos se debe enseñar con lógica, rigor y claridad.

### La Filosofía del Libro: De Cero a Senior con Lógica y Alegorías
Aprender Big Data y Data Engineering suele ser frustrante cuando los manuales presentan teoría matemática densa o código descontextualizado. Mi propósito al escribir este volumen es llevarte de la mano **desde los cimientos absolutos (0) hasta el diseño de nivel Senior**:

1. **Alegorías e Intuición Cotidiana:** Cada tema complejo se introduce con analogías intuitivas. Entenderás por qué *Lazy Evaluation* y el grafo *DAG* operan igual que la comanda de un chef en un restaurante de alta cocina, por qué el *Shuffling* distribuido funciona como una central logística de clasificación postal, o por qué *Delta Lake* actúa como un libro notarial inmutable con sellos de tiempo.
2. **Fundamentos Rigurosos Bajo el Capó:** Qué ocurre físicamente en la memoria RAM, los ejecutores y la red al lanzar un job distribuido.
3. **Código Industrial y Patrones de Producción:** Sintaxis probada en **PySpark**, **SQL Analítico**, **Lakehouse**, **Kafka**, **Azure Data Factory** y **Great Expectations**.
4. **Criterio Senior y Optimización de Costos:** Cómo erradicar cuellos de botella (*data skew*), prevenir fallos silenciosos por nulos y reducir la factura cloud.

### Métricas del Volumen:
- **Total de Capítulos y Secciones:** ${totalChapters}
- **Extensión aproximada:** ${totalWordCount.toLocaleString()} palabras (~${Math.round(totalWordCount / 280)} páginas estimadas)
- **Estructura Didáctica:** 9 Partes Temáticas + Apéndices de Estudio

---

${tocMarkdown}

---
`;

  const finalBookMarkdown = bookHeader + '\n\n' + bookSections.join('\n\n');

  // Save to vault as a master file
  const bookFileName = '📚 Libro Maestro - Data Engineering de Cero a Senior.md';
  const outPath = path.join(vaultPath, bookFileName);
  fs.writeFileSync(outPath, finalBookMarkdown, 'utf8');

  return {
    filePath: outPath,
    fileName: bookFileName,
    totalChapters,
    totalWordCount,
    estimatedPages: Math.round(totalWordCount / 280),
    updatedAt: now,
    markdown: finalBookMarkdown
  };
}

module.exports = {
  compileMasterBook
};
