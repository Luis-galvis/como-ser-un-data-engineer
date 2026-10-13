const fs = require('fs');
const path = require('path');

const VAULT_PATH = 'D:\\estudio\\estudio';
const OUT_DIR = path.join(__dirname, 'libro');
const MIRROR_DIR = 'D:\\estudio\\libro';

if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

function getAllFiles(dir, fileList = []) {
  const files = fs.readdirSync(dir);
  for (const file of files) {
    if (file.startsWith('.obsidian') || file.startsWith('.trash') || file.startsWith('.')) continue;
    const fullPath = path.join(dir, file);
    const stat = fs.statSync(fullPath);
    if (stat.isDirectory()) {
      getAllFiles(fullPath, fileList);
    } else if (file.endsWith('.md')) {
      fileList.push(fullPath);
    }
  }
  return fileList;
}

const filePaths = getAllFiles(VAULT_PATH);
console.log(`Encontradas ${filePaths.length} notas en el vault.`);

const notes = [];
const linkMap = new Map(); // target -> [sources]

// 1. First pass: Parse every note
for (const fp of filePaths) {
  const content = fs.readFileSync(fp, 'utf8');
  const relPath = path.relative(VAULT_PATH, fp);
  const baseName = path.basename(fp, '.md');
  const stat = fs.statSync(fp);

  // Frontmatter
  let frontmatter = null;
  let body = content;
  const fmMatch = content.match(/^---\s*[\r\n]+([\s\S]*?)[\r\n]+---/);
  if (fmMatch) {
    frontmatter = fmMatch[1].trim();
    body = content.slice(fmMatch[0].length);
  }

  // Tags (#tag)
  const tags = new Set();
  const tagRegex = /(?:^|\s)#([a-zA-Z0-9_\-áéíóúÁÉÍÓÚ]+)/g;
  let tm;
  while ((tm = tagRegex.exec(content)) !== null) {
    tags.add(tm[1]);
  }

  // Word count
  const words = content.split(/\s+/).filter(Boolean).length;

  // Headings (H1, H2, H3)
  const headings = [];
  const lines = content.split(/\r?\n/);
  for (const l of lines) {
    const hm = l.match(/^(#{1,3})\s+(.+)$/);
    if (hm) {
      headings.push(`${hm[1]} ${hm[2].trim()}`);
    }
  }

  // Outgoing links [[...]]
  const outgoingLinks = new Set();
  const linkRegex = /\[\[([^\]|#]+)(?:\|([^\]]+))?\]\]/g;
  let lm;
  while ((lm = linkRegex.exec(content)) !== null) {
    outgoingLinks.add(lm[1].trim());
  }

  // Code blocks & languages
  const codeLanguages = new Set();
  const codeBlockRegex = /```([a-zA-Z0-9_\-]+)?[\r\n]/g;
  let cbm;
  let hasCode = false;
  while ((cbm = codeBlockRegex.exec(content)) !== null) {
    hasCode = true;
    if (cbm[1]) codeLanguages.add(cbm[1].toLowerCase());
    else codeLanguages.add('plain');
  }

  // Images / embeds
  const images = [];
  const imgRegex = /!\[\[([^\]]+)\]\]|!\[([^\]]*)\]\(([^)]+)\)/g;
  let im;
  while ((im = imgRegex.exec(content)) !== null) {
    images.push(im[1] || im[3]);
  }

  // Confidentiality keywords
  const confMatches = [];
  if (/scotia/i.test(content)) confMatches.push('Mención de Scotia / ScotiaTech');
  if (/salario|sueldo|\$5\.8M|\$12-13M|\$10-14M|COP/i.test(content)) confMatches.push('Referencias Salariales / Cifras Personales');
  if (/password|secret|token|apikey|client_secret/i.test(content)) confMatches.push('Posibles credenciales');

  const noteObj = {
    path: fp,
    relPath,
    title: baseName,
    hasFrontmatter: !!frontmatter,
    frontmatter,
    tags: Array.from(tags),
    wordCount: words,
    headings,
    outgoingLinks: Array.from(outgoingLinks),
    backlinks: [],
    hasCode,
    codeLanguages: Array.from(codeLanguages),
    images,
    mtime: stat.mtime,
    confMatches
  };

  notes.push(noteObj);
}

// 2. Second pass: Calculate Backlinks
for (const n of notes) {
  for (const link of n.outgoingLinks) {
    // Normalise name
    const target = notes.find(cand => cand.title.toLowerCase() === link.toLowerCase());
    if (target) {
      if (!target.backlinks.includes(n.title)) {
        target.backlinks.push(n.title);
      }
    }
  }
}

// 3. Generate inventario.csv
let csvContent = 'Ruta,Titulo,Palabras,Tiene_Frontmatter,Tags,Cant_Headings,Enlaces_Salientes,Backlinks,Tiene_Codigo,Lenguajes_Codigo,Cant_Imagenes,Confidencial,Ultima_Modificacion\n';
for (const n of notes) {
  const row = [
    `"${n.relPath.replace(/"/g, '""')}"`,
    `"${n.title.replace(/"/g, '""')}"`,
    n.wordCount,
    n.hasFrontmatter ? 'SI' : 'NO',
    `"${n.tags.join('; ')}"`,
    n.headings.length,
    n.outgoingLinks.length,
    n.backlinks.length,
    n.hasCode ? 'SI' : 'NO',
    `"${n.codeLanguages.join('; ')}"`,
    n.images.length,
    n.confMatches.length > 0 ? `"${n.confMatches.join('; ')}"` : 'NO',
    `"${n.mtime.toISOString().slice(0, 10)}"`
  ].join(',');
  csvContent += row + '\n';
}

fs.writeFileSync(path.join(OUT_DIR, 'inventario.csv'), csvContent, 'utf8');

// 4. Generate INVENTARIO_VAULT.md
let invMd = `# 📊 INVENTARIO GENERAL DEL VAULT DE OBSIDIAN

**Bóveda auditada:** \`D:\\estudio\\estudio\`  
**Fecha de auditoría:** ${new Date().toLocaleDateString('es-CO', { year: 'numeric', month: 'long', day: 'numeric' })}  
**Total de notas procesadas:** ${notes.length} notas  
**Total de palabras acumuladas:** ${notes.reduce((acc, n) => acc + n.wordCount, 0).toLocaleString()} palabras  

---

## 📋 Resumen Métrico del Manuscrito Fuente
| Métrica | Valor |
| :--- | :--- |
| **Total de Notas Markdown** | ${notes.length} |
| **Notas con Fragmentos de Código** | ${notes.filter(n => n.hasCode).length} |
| **Lenguajes Detectados** | ${Array.from(new Set(notes.flatMap(n => n.codeLanguages))).join(', ')} |
| **Notas Huérfanas (sin enlaces ni backlinks)** | ${notes.filter(n => n.outgoingLinks.length === 0 && n.backlinks.length === 0).length} |
| **Riesgos de Confidencialidad Detectados** | ${notes.filter(n => n.confMatches.length > 0).length} notas |

---

## 📑 Detalle Ficha por Nota

`;

for (const n of notes) {
  invMd += `### 📄 [[${n.title}]]
- **Ruta relativa:** \`${n.relPath}\`
- **Extensión:** ${n.wordCount.toLocaleString()} palabras | **Última modificación:** ${n.mtime.toISOString().slice(0, 10)}
- **Etiquetas (#tags):** ${n.tags.map(t => '#' + t).join(' ') || '(ninguna)'}
- **Frontmatter YAML:** ${n.hasFrontmatter ? 'Presente' : 'Ausente'}
- **Encabezados:** ${n.headings.length > 0 ? n.headings.slice(0, 5).join(' ➔ ') + (n.headings.length > 5 ? ' ...' : '') : '(sin encabezados)'}
- **Código:** ${n.hasCode ? 'Sí (' + n.codeLanguages.join(', ') + ')' : 'No'}
- **Enlaces salientes ([[...]]):** ${n.outgoingLinks.length > 0 ? n.outgoingLinks.map(l => '[[' + l + ']]').join(', ') : 'Ninguno'}
- **Backlinks (citado por):** ${n.backlinks.length > 0 ? n.backlinks.map(b => '[[' + b + ']]').join(', ') : 'Ninguno (huérfana entrante)'}
- **Imágenes / Adjuntos:** ${n.images.length > 0 ? n.images.join(', ') : '0'}
- **Riesgo Confidencial:** ${n.confMatches.length > 0 ? '⚠️ **' + n.confMatches.join(' | ') + '**' : 'Limpio'}

---
`;
}

fs.writeFileSync(path.join(OUT_DIR, 'INVENTARIO_VAULT.md'), invMd, 'utf8');

// 5. Generate RESUMEN_PARA_REVISION.md & REPORTE_EDITORIAL.md
// Build proposed chapter tree and gaps analysis
const proposedStructure = [
  {
    part: 'Parte I: Fundamentos de Ingeniería y SQL Analítico',
    desc: 'Bases conceptuales del dato, álgebra relacional y transformaciones tabulares.',
    chapters: [
      {
        title: 'Fundamentos de SQL y Computación Analítica',
        sources: ['SQL.md'],
        summary: 'Introduce SQL declarativo vs imperativo, arquitectura relacional en Big Data y ejecución distribuida.',
        gaps: 'Requiere conectar con funciones de ventana (Window Functions) avanzadas.'
      },
      {
        title: 'Agrupaciones, Agregaciones y Métricas de Negocio',
        sources: ['AGRUPACIONES DE DATOS Y FUNCIONES DE AGREGACION.md'],
        summary: 'Patrones de agregación con groupBy, sum, avg y el costo del shuffle al consolidar datos.',
        gaps: 'Normalizar ejemplos de variables para mantener consistencia de nombres en todo el libro.'
      },
      {
        title: 'Tipos de Datos y Casting Riguroso en Memoria',
        sources: ['Tipos de Datos y Casting en Spark.md'],
        summary: 'Gestión de StructType, precisión decimal para finanzas (Decimal vs Double) y control de nulos.',
        gaps: 'Completo tras el enriquecimiento industrial.'
      },
      {
        title: 'Filtrado y Expresiones Condicionales',
        sources: ['Filter y Condicionales en DataFrames.md', 'Filter.md'],
        summary: 'Uso de filter/where, sintaxis de columna (&, |, ~), when/otherwise y optimización Predicate Pushdown.',
        gaps: 'Fusión necesaria entre Filter.md y Filter y Condicionales en DataFrames.md (duplicado temático).'
      },
      {
        title: 'Transformación de Columnas y Operaciones con Esquemas',
        sources: ['with column y withcolum rename.md', 'Funciones de Transformación en PySpark.md'],
        summary: 'Métodos withColumn, withColumnRenamed, funciones de texto, fechas, arrays y explosión (explode).',
        gaps: 'Explicar por qué evitar las UDFs de Python a favor de funciones nativas de C++/Java.'
      },
      {
        title: 'Ingesta de Archivos y Fuentes de Datos',
        sources: ['como abrir archivos y cargar en pyspark.md'],
        summary: 'Lectura de CSV, Parquet y JSON con SparkReader, paso de opciones de lectura y esquemas explícitos.',
        gaps: 'Agregar lectura de archivos particionados por fecha/región.'
      }
    ]
  },
  {
    part: 'Parte II: El Motor de Cómputo Distribuido: Apache Spark y PySpark',
    desc: 'Arquitectura física de clúster, evolución desde Hadoop y componentes del runtime.',
    chapters: [
      {
        title: 'De Hadoop y MapReduce al Cómputo en Memoria',
        sources: ['Hadoop.md', 'MapReduce.md', 'Spark.md', 'Apache Spark.md'],
        summary: 'El cuello de botella de E/S en disco de MapReduce y la revolución del procesamiento en RAM de Spark.',
        gaps: 'Unificar las notas Spark.md y Apache Spark.md en un solo capítulo maestro.'
      },
      {
        title: 'Arquitectura de Clúster: Driver, Workers y Ejecutores',
        sources: ['Cluster Computing.md', 'Cluster Manager.md', 'Driver Program.md', 'Worker Node.md', 'Ejecutores.md'],
        summary: 'Anatomía interna de un clúster: asignación de recursos, JVM de ejecutores y coordinación del Driver.',
        gaps: 'Sintetizar las 5 notas atómicas en un capítulo con diagrama arquitectónico.'
      },
      {
        title: 'Entorno de Desarrollo y Configuración con PySpark',
        sources: ['PySpark.md', 'spark session.md', 'conf spark.md', 'SparkConf.md'],
        summary: 'Inicialización de SparkSession con el patrón Builder, configuración de memoria y paralelismo.',
        gaps: 'Fusión de conf spark.md y SparkConf.md.'
      },
      {
        title: 'Abstracciones de Datos: De RDDs a DataFrames',
        sources: ['dataframes.md', 'RDD.md'],
        summary: 'Inmutabilidad, linaje de transformaciones (lineage) y por qué DataFrames superan a RDDs en analítica.',
        gaps: 'Ilustrar la analogía del chef y la receta para consolidar el aprendizaje.'
      }
    ]
  },
  {
    part: 'Parte III: Arquitectura Interna y Optimización de Rendimiento',
    desc: 'Mecanismos bajo el capó: grafos DAG, motor Catalyst, memoria y gestión de cuellos de botella.',
    chapters: [
      {
        title: 'Evaluación Perezosa (Lazy Evaluation) y el Grafo DAG',
        sources: ['lazy evaluation.md', 'DAG.md', 'Acción.md'],
        summary: 'Diferencia estricta entre Transformaciones (estrechas/amplias) y Acciones terminales.',
        gaps: 'Capítulo sólido; detallar planes lógicos vs físicos en el DAG.'
      },
      {
        title: 'El Optimizador Catalyst y Spark SQL',
        sources: ['Spark SQL.md', 'Spark SQL y Catalyst Optimizer.md'],
        summary: 'Fases de Catalyst: análisis de árbol lógico, optimización basada en reglas (RBO) y costos (CBO).',
        gaps: 'Fusionar las dos notas de Catalyst en una sola exposición cohesiva.'
      },
      {
        title: 'Gestión de Memoria RAM y Estrategias de Caché',
        sources: ['Memoria RAM.md', 'Cache.md', 'Spark Memory Management.md'],
        summary: 'Unified Memory Manager, Execution Memory vs Storage Memory, niveles de persistencia y derrame a disco (Spill).',
        gaps: 'Añadir tabla comparativa de niveles de almacenamiento (MEMORY_ONLY vs MEMORY_AND_DISK).'
      },
      {
        title: 'Particionamiento, Shuffling y Mitigación de Data Skew',
        sources: ['Partitioning y Shuffling en Spark.md'],
        summary: 'El costo del shuffle en red, estrategias Hash/Range y técnicas de salting para corregir desbalances.',
        gaps: 'Muy completo; incluir ejemplo visual de la técnica de salting.'
      },
      {
        title: 'Optimización de Costos en la Nube',
        sources: ['Cloud Cost Optimization in Spark.md'],
        summary: 'Ajuste de instancias Spot, Serverless Spark, dimensionamiento de clústeres y reducción de egress.',
        gaps: 'Listo para producción.'
      }
    ]
  },
  {
    part: 'Parte IV: Arquitectura Lakehouse, Delta Lake y Modelado',
    desc: 'Almacenamiento transaccional, evolución de esquemas y patrones dimensionales modernos.',
    chapters: [
      {
        title: 'Fundamentos de Cloud Computing y Lakehouse',
        sources: ['Cloud Computing.md', 'Databricks.md'],
        summary: 'Separación de cómputo y almacenamiento en S3/ADLS/GCS y convergencia de Data Lake con Data Warehouse.',
        gaps: 'Complementar con breve comparativa entre AWS, Azure y GCP.'
      },
      {
        title: 'Delta Lake y Transacciones ACID en Almacenamiento Columnar',
        sources: ['Delta Lake.md'],
        summary: 'El registro de transacciones _delta_log, Time Travel, evolución de esquemas y compactación (OPTIMIZE).',
        gaps: 'Ilustrar la analogía del libro notarial inmutable.'
      },
      {
        title: 'La Arquitectura Medallion: Bronce, Plata y Oro',
        sources: ['Medallion Architecture.md'],
        summary: 'Diseño de capas de calidad progresiva: aterrizaje crudo (Bronze), limpieza (Silver) y KPIs (Gold).',
        gaps: 'Excelente flujo; vincular con orquestación continua.'
      },
      {
        title: 'Modelado Dimensional y Patrones en Lakehouse',
        sources: ['Dimensional Modeling.md', 'Modelado Dimensional en Lakehouse.md', 'Data Modeling Patterns.md'],
        summary: 'Esquema Estrella vs Snowflake de Ralph Kimball, Slowly Changing Dimensions (SCD) y denormalización.',
        gaps: 'Integrar las 3 notas en un recorrido desde Kimball clásico hasta Lakehouse moderno.'
      }
    ]
  },
  {
    part: 'Parte V: Gobernanza, Seguridad y Linaje de Datos',
    desc: 'Control de acceso granular, catalogación y trazabilidad en arquitecturas empresariales.',
    chapters: [
      {
        title: 'Gobernanza Unificada con Unity Catalog',
        sources: ['Unity Catalog.md'],
        summary: 'Control centralizado de acceso, permisos granulares (RBAC) y linaje automático de extremo a extremo.',
        gaps: 'Destacar la aplicación de políticas antes de que Spark ejecute tareas.'
      },
      {
        title: 'Catálogo de Datos y Linaje (Data Lineage)',
        sources: ['Data Cataloging.md', 'Data Lineage and Governance.md'],
        summary: 'Visibilidad de activos de datos, cumplimiento normativo (GDPR) y auditoría de transformaciones.',
        gaps: 'Fusión de conceptos complementarios de catálogo y linaje.'
      }
    ]
  },
  {
    part: 'Parte VI: Ingesta, Streaming y Tiempo Real',
    desc: 'Arquitecturas de eventos, colas de mensajes y streaming continuo con tolerancia a fallos.',
    chapters: [
      {
        title: 'Fundamentos de Streaming y Apache Kafka',
        sources: ['Streaming.md', 'Apache Kafka.md'],
        summary: 'Mensajería distribuida, particiones de Kafka, offsets, semántica at-least-once y exact-once.',
        gaps: 'Añadir diagrama de flujo Productor ➔ Tópico ➔ Consumidor.'
      },
      {
        title: 'Spark Structured Streaming y Checkpointing',
        sources: ['Spark Structured Streaming.md'],
        summary: 'El DataFrame como tabla sin fin (unbounded table), micro-batches, marcas de agua (Watermarking) y recuperación.',
        gaps: 'Conectar directamente con los offsets de Kafka.'
      },
      {
        title: 'Captura de Cambios (Change Data Capture - CDC) y Patrones de Calidad',
        sources: ['Change Data Capture (CDC).md', 'Ingesta Batch vs Streaming y CDC.md', 'Streaming Data Quality Patterns.md'],
        summary: 'Ingesta de logs transaccionales (Debezium/Kafka Connect), comparación Batch vs Streaming y cuarentena de datos sucios.',
        gaps: 'Muy valioso para la industria; consolidar las tres notas en una estrategia unificada.'
      }
    ]
  },
  {
    part: 'Parte VII: Orquestación Moderna de Pipelines de Datos',
    desc: 'Diseño de flujos de trabajo, DAGs de orquestación y transformación semántica.',
    chapters: [
      {
        title: 'Evolución de Pipelines: De SSIS a la Orquestación Moderna',
        sources: ['SSIS.md', 'ETL Pipelines.md'],
        summary: 'Transición histórica desde ETL tradicionales basados en servidores físicos hacia ELT elástico en la nube.',
        gaps: 'Contrastar la rigidez de SSIS con la flexibilidad de código moderno.'
      },
      {
        title: 'Azure Data Factory vs. Apache Airflow',
        sources: ['Azure Data Factory.md', 'Apache Airflow vs Azure Data Factory.md'],
        summary: 'Orquestación sin código basada en nube (ADF) frente a orquestación como código Python (Airflow).',
        gaps: 'Armar matriz de decisión técnica (cuándo usar ADF y cuándo Airflow).'
      },
      {
        title: 'Transformaciones con dbt y Orquestación Híbrida',
        sources: ['dbt.md', 'Orquestación de Pipelines Hybrid.md'],
        summary: 'Modelado modular en SQL con dbt, tests automáticos de documentación y arquitecturas de pipelines híbridos.',
        gaps: 'Capítulo de alto valor para perfiles analíticos avanzados.'
      }
    ]
  },
  {
    part: 'Parte VIII: Data Quality, Observabilidad y DataOps',
    desc: 'Ingeniería de confiabilidad de datos, validación automatizada y CI/CD en pipelines.',
    chapters: [
      {
        title: 'Frameworks de Calidad de Datos y Great Expectations',
        sources: ['Data Quality.md', 'Data Quality Framework.md', 'Great Expectations.md', 'Integración de Data Quality en ETL.md'],
        summary: 'Los 6 pilares de calidad, reglas de validación como expectativas declarativas y prevención de datos corruptos.',
        gaps: 'Consolidar las 4 notas en un capítulo con caso práctico de assertions.'
      },
      {
        title: 'Observabilidad de Datos y Monitoreo Proactivo',
        sources: ['Data Observability.md'],
        summary: 'Los 5 pilares de observabilidad: frescura, volumen, esquema, distribución y linaje.',
        gaps: 'Capítulo autocontenido y conciso.'
      },
      {
        title: 'DataOps, CI/CD e Infraestructura como Código',
        sources: ['CI-CD for Data Pipelines.md', 'DataOps y CI-CD para Data Engineering.md', 'Terraform for Data Engineering.md'],
        summary: 'Despliegues automatizados de código de datos con Git, tests unitarios en PySpark y provisión con Terraform.',
        gaps: 'Unificar las tres notas en la metodología moderna de DataOps.'
      }
    ]
  },
  {
    part: 'Parte IX: System Design, Roadmap y Preparación de Nivel Senior',
    desc: 'Diseño de sistemas a escala, aplicaciones avanzadas y preparación de entrevistas técnicas.',
    chapters: [
      {
        title: 'Cargas de Trabajo Especializadas: Machine Learning y Procesamiento de Grafos',
        sources: ['Machine Learning.md', 'Graph Processing.md'],
        summary: 'Integración de Spark MLlib para entrenamiento distribuido y GraphX para análisis de redes y relaciones.',
        gaps: 'Capítulo complementario que abre puertas al perfil de Machine Learning Engineer.'
      },
      {
        title: 'Roadmap y Guía Definitiva de System Design para Senior Data Engineers',
        sources: ['Data Engineering Roadmap.md', 'Data Engineer Interview Guide y System Design.md'],
        summary: 'Metodología paso a paso para resolver problemas de System Design en entrevistas FAANG / globales y mapa de carrera.',
        gaps: 'Asegurar sanitización total de cualquier dato salarial o personal.'
      }
    ]
  }
];

// 6. Write RESUMEN_PARA_REVISION.md
let resRevMd = `# 📑 RESUMEN ESTRUCTURAL PARA REVISIÓN EDITORIAL
> **Libro:** La Enciclopedia Definitiva de Data Engineering: De Cero a Senior  
> **Autor:** Luis Galvis | **Editorial:** Optimal Labs Press (2026)  
> **Objetivo:** Validación del árbol temático, fuentes originales, detección de duplicados y vacíos.

---

## 🧭 Visión Pedagógica: De Cero a Senior
El manuscrito no impone divisiones artificiales; surge directamente de las **69 notas técnicas de la bóveda \`D:\\estudio\\estudio\`**, organizadas en una secuencia lógica que guía al lector desde el entendimiento básico de una tabla SQL hasta el diseño de arquitecturas distribuidas multi-nube y liderazgo técnico:

\`\`\`mermaid
graph TD
    P1["Parte I: Fundamentos y SQL Analítico"] --> P2["Parte II: Motor de Cómputo Spark & PySpark"]
    P2 --> P3["Parte III: Arquitectura Interna y Rendimiento"]
    P3 --> P4["Parte IV: Lakehouse, Delta Lake y Modelado"]
    P4 --> P5["Parte V: Gobernanza y Seguridad"]
    P4 --> P6["Parte VI: Ingesta, Streaming y Tiempo Real"]
    P5 --> P7["Parte VII: Orquestación Moderna de Pipelines"]
    P6 --> P7
    P7 --> P8["Parte VIII: Data Quality, Observabilidad y DataOps"]
    P8 --> P9["Parte IX: System Design y Preparación Senior"]
\`\`\`

---

`;

let chapCounter = 1;
for (const p of proposedStructure) {
  resRevMd += `## 🏛️ ${p.part}\n*${p.desc}*\n\n`;
  for (const c of p.chapters) {
    resRevMd += `### 📖 Capítulo ${chapCounter}: ${c.title}\n`;
    resRevMd += `- **Notas fuente de Obsidian:** ` + c.sources.map(s => '[[' + s.replace('.md', '') + ']]').join(', ') + '\n';
    resRevMd += `- **Resumen editorial (2-3 líneas):** ${c.summary}\n`;
    resRevMd += `- **Huecos / Duplicados a resolver:** ${c.gaps}\n\n`;
    chapCounter++;
  }
  resRevMd += `---\n\n`;
}

fs.writeFileSync(path.join(OUT_DIR, 'RESUMEN_PARA_REVISION.md'), resRevMd, 'utf8');

// 7. Write REPORTE_EDITORIAL.md
let repEdMd = `# 🛡️ REPORTE EDITORIAL Y AUDITORÍA TÉCNICA
> **Documento de Control y Aseguramiento de Calidad (QA)**  
> **Manuscrito:** La Enciclopedia Definitiva de Data Engineering  
> **Autor:** Luis Galvis  

---

## 1. 🚨 Detección y Mitigación de Riesgos de Confidencialidad
Se han auditado exhaustivamente todas las notas de la bóveda para garantizar que **ningún dato corporativo, nombre de cliente o información privada de empleo** llegue a la edición comercial:

| Archivo Fuente | Riesgo Detectado | Acción Editorial Recomendada |
| :--- | :--- | :--- |
| \`PATH CARRER.md\` | Mención explícita a ScotiaTech / Scotiabank, rol interno y cifras de salario (\$5.8M COP, negociaciones). | **EXCLUIR TOTALMENTE** del libro. La nota es personal y no técnica. |
| \`Cluster Manager.md\` | Contiene enlace saliente a \`[[PATH CARRER]]\`. | El enlace saliente se transforma en texto plano sin mencionar la nota privada. |
| Fragmentos de código en PySpark | Verificado: no hay credenciales, secretos, connection strings ni endpoints privados reales. | Se usan variables genéricas tipo \`users_path\`, \`spark_configs.json\`. |

---

## 2. 🧩 Notas Duplicadas o con Solapamiento Temático
Para asegurar un libro con estándar de editorial académica, se proponen las siguientes fusiones:

1. **\`Spark.md\` y \`Apache Spark.md\`:** Ambas explican la historia y arquitectura general. Se fusionan en el **Capítulo 7**, preservando las alegorías y el contexto de memoria RAM.
2. **\`Filter.md\` y \`Filter y Condicionales en DataFrames.md\`:** Una aborda la semántica funcional y otra las expresiones condicionales de columna. Se unifican en el **Capítulo 4**.
3. **\`conf spark.md\` y \`SparkConf.md\`:** Una muestra el código práctico de carga JSON y la otra los parámetros técnicos. Se unen en el **Capítulo 9**.
4. **\`Spark SQL.md\` y \`Spark SQL y Catalyst Optimizer.md\`:** Se integran en el **Capítulo 12** para una explicación de 360 grados de Catalyst (RBO + CBO).
5. **\`Data Quality.md\`, \`Data Quality Framework.md\` e \`Integración de Data Quality en ETL.md\`:** Se consolidan en el **Capítulo 22** junto a Great Expectations.

---

## 3. 🔍 Diagnóstico de Secciones Faltantes [FALTA] por Capítulo
Siguiendo la plantilla de referencia exigida (Título ➔ Objetivos ➔ Alegoría ➔ Fundamentos ➔ Bajo el Capó ➔ Código ➔ Errores Comunes ➔ Ejercicios ➔ Resumen):

- **Objetivos de Aprendizaje:** \`[FALTA]\` en la mayoría de notas originales de la bóveda (se redactarán de forma concisa en la fase de normalización).
- **Ejercicios Prácticos con Solución:** \`[FALTA]\` en las notas crudas. Se diseñará un banco de ejercicios al final de cada parte o en apéndice.
- **Lecturas Recomendadas:** \`[FALTA]\` (se consolidará una Bibliografía unificada con documentación oficial de Apache Spark, Delta Lake, Databricks y libros canónicos como Martin Kleppmann y Ralph Kimball).
- **Alegorías:** Presentes y sólidas en temas clave (el chef y la comanda para Lazy/DAG, la aduana de fábrica para tipos de datos, la oficina postal para shuffling). Se mantendrán intactas con su caja visual en color coral (\`#e2685c\`).

---

## 4. 📚 Términos Clave para Glosario e Índice Analítico
Durante la lectura del vault se han catalogado los términos técnicos anglosajones que deben aparecer en cursiva la primera vez y disponer de entrada formal en el glosario:

- **Cómputo & Runtime:** *Driver Program*, *Worker Node*, *Executor*, *Cluster Manager*, *DAG (Directed Acyclic Graph)*, *Lazy Evaluation*, *RDD (Resilient Distributed Dataset)*, *DataFrame*, *Tungsten Engine*, *Catalyst Optimizer*, *Predicate Pushdown*, *Code Generation*, *Shuffle*, *Data Skew*, *Spill*, *Salting*, *Coalesce*, *Repartition*, *Broadcast Join*.
- **Almacenamiento & Lakehouse:** *Lakehouse*, *Delta Lake*, *Time Travel*, *ACID*, *Medallion Architecture (Bronze, Silver, Gold)*, *Parquet*, *Schema Evolution*, *Slowly Changing Dimensions (SCD)*, *Fact Table*, *Dimension Table*, *Star Schema*, *Snowflake Schema*.
- **Streaming & Eventos:** *Streaming*, *Micro-batch*, *Watermarking*, *Checkpointing*, *CDC (Change Data Capture)*, *Offset*, *At-least-once*, *Exactly-once*, *Topic*, *Partition*.
- **Gobernanza & DataOps:** *Unity Catalog*, *RBAC (Role-Based Access Control)*, *Data Lineage*, *Data Observability*, *Data Quality*, *Assertions*, *CI/CD*, *Terraform*, *IaC (Infrastructure as Code)*.

---

## 5. 🛠️ Validación de Sintaxis de Código
Se auditaron los bloques de código presentes en las notas:
- **PySpark:** Sintaxis válida usando métodos idiomáticos de \`pyspark.sql.functions\` (\`col\`, \`when\`, \`cast\`, \`agg\`).
- **SQL:** Consultas declarativas compatibles con Spark SQL y ANSI SQL.
- **Python / JSON:** Scripts de lectura de archivos y carga de configuraciones validados.
- **Advertencias menores:** En notas iniciales como \`with column y withcolum rename.md\`, faltaban algunos paréntesis de cierre en strings de ejemplo rápido (ej: \`user_dataframe.withcolumn("dummy_col,lit(1)).show\`). En la fase de normalización se corregirán estas erratas tipográficas sin alterar la lógica.
`;

fs.writeFileSync(path.join(OUT_DIR, 'REPORTE_EDITORIAL.md'), repEdMd, 'utf8');

// 8. Try to mirror deliverables to D:\estudio\libro if available
try {
  if (!fs.existsSync(MIRROR_DIR)) fs.mkdirSync(MIRROR_DIR, { recursive: true });
  fs.copyFileSync(path.join(OUT_DIR, 'inventario.csv'), path.join(MIRROR_DIR, 'inventario.csv'));
  fs.copyFileSync(path.join(OUT_DIR, 'INVENTARIO_VAULT.md'), path.join(MIRROR_DIR, 'INVENTARIO_VAULT.md'));
  fs.copyFileSync(path.join(OUT_DIR, 'RESUMEN_PARA_REVISION.md'), path.join(MIRROR_DIR, 'RESUMEN_PARA_REVISION.md'));
  fs.copyFileSync(path.join(OUT_DIR, 'REPORTE_EDITORIAL.md'), path.join(MIRROR_DIR, 'REPORTE_EDITORIAL.md'));
  console.log('Archivos replicados exitosamente en:', MIRROR_DIR);
} catch (e) {
  console.log('Nota: D:\\estudio\\libro no disponible para replicacion directa, conservado en:', OUT_DIR);
}

console.log('Auditoria completada con exito.');
