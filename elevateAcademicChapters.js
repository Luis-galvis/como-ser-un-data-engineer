const fs = require('fs');
const path = require('path');

const CAP_DIR = 'c:\\Users\\lgalv\\Desktop\\obsidian\\libro\\capitulos';
const MIRROR_DIR = 'D:\\estudio\\libro\\capitulos';

const files = fs.readdirSync(CAP_DIR).filter(f => f.endsWith('.md') && f !== '00-portada.md');

console.log(`Procesando ${files.length} capítulos para elevación a estándar académico puro...`);

files.forEach(file => {
  const filePath = path.join(CAP_DIR, file);
  let content = fs.readFileSync(filePath, 'utf8');

  // 1. Eliminar caracteres invisibles y artefactos Unicode
  content = content.replace(/\uFE0F/g, '').replace(/\u200B/g, '');

  // 2. Extraer número de capítulo del nombre de archivo (ej. 01 -> 1)
  const numMatch = file.match(/^(\d+)-/);
  const capNum = numMatch ? parseInt(numMatch[1], 10) : 1;

  // 3. Sustituciones específicas para eliminar metáforas infantiles de IA
  if (file === '01-fundamentos-sql.md') {
    content = content.replace(
      /<div style="background-color: #fbf9f4; border-left: 5px solid #1b2030;[\s\S]*?<\/div>/,
      `> **Definición 1.1 (Cálculo Relacional y Paradigma Declarativo)**\n> En la teoría de bases de datos y sistemas de procesamiento distribuido, SQL opera bajo un modelo **declarativo**: el ingeniero especifica el predicado analítico y la proyección relacional deseada (*qué calcular*), desacoplándola por completo de la secuencia física de instrucciones mecánicas (*cómo calcularlo*).\n>\n> Frente a la programación imperativa en Java, C++ o Python puro —donde el desarrollador debe gestionar explícitamente bucles de iteración, buffers en memoria y asignación de hilos—, la abstracción declarativa traslada la responsabilidad de optimización al motor de cómputo. El optimizador de consultas (Catalyst en Spark) compila la sentencia en un árbol sintáctico abstracto (AST), aplica álgebra relacional para reordenar operaciones y genera el plan físico óptimo adaptado a los recursos disponibles en el clúster.`
    );
  }

  if (file === '13-evaluacion-perezosa-y-dag.md') {
    content = content.replace(
      /> \*"Spark es como un chef de alta cocina:[\s\S]*?\*"/,
      `> *"La evaluación perezosa en sistemas distribuidos no es una postergación pasiva de trabajo, sino una ventana de optimización matemática que permite al motor compilar, fusionar y simplificar grafos de ejecución antes de comprometer memoria y red."*`
    );
    content = content.replace(
      /<div style="background-color: #fbf9f4; border-left: 5px solid #1b2030;[\s\S]*?<\/div>/,
      `> **Definición 13.1 (Evaluación Perezosa y Diferimiento de Cómputo)**\n> La **evaluación perezosa** (*Lazy Evaluation*) es una estrategia computacional en la cual las operaciones de transformación sobre estructuras distribuidas (DataFrames y RDDs) no materializan resultados inmediatos en memoria ni generan tráfico en el clúster. En su lugar, cada operación se registra como un vértice o arista dentro de un grafo de dependencias dirigidas (*lineage graph*).\n>\n> Esta estrategia otorga al motor un panorama global del pipeline analítico antes de iniciar el procesamiento físico. De este modo, el optimizador puede fusionar transformaciones contiguas en un solo pase de datos (*pipeline execution*), anticipar filtros a la etapa de lectura en disco (*Predicate Pushdown*) y descartar columnas irrelevantes (*Column Pruning*), ejecutando el trabajo exclusivamente cuando una acción explícita lo demanda.`
    );
  }

  if (file === '00-roadmap-y-panorama.md') {
    content = content.replace(
      /<div style="background-color: #fbf9f4; border-left: 5px solid #1b2030;[\s\S]*?<\/div>/,
      `> **Definición 0.1 (Ingeniería de Datos como Disciplina de Sistemas Distribuidos)**\n> La ingeniería de datos contemporánea es la rama de la ingeniería de software responsable del diseño, implementación, escalabilidad y observabilidad de plataformas para la recolección, transformación, almacenamiento y gobierno de activos de información a gran escala.\n>\n> Su objetivo fundamental es garantizar que los datos fluyan desde sistemas operacionales heterogéneos hacia capas analíticas con alta disponibilidad, latencia predecible, consistencia transaccional (semántica ACID) e integridad estructural verificable.`
    );
  }

  if (file === '15-gestion-memoria-cache.md') {
    content = content.replace(
      /<div style="background-color: #fbf9f4; border-left: 5px solid #1b2030;[\s\S]*?<\/div>/,
      `> **Definición 15.1 (Modelo Unificado de Gestión de Memoria en Spark)**\n> A partir de Apache Spark 1.6+, el motor implementa un modelo de asignación unificada de memoria que supera la fragmentación estática tradicional. La memoria heap de cada ejecutor se subdivide dinámicamente entre dos reservas principales: **Execution Memory** (destinada a buffers de barajado, ordenamiento y agregaciones en memoria) y **Storage Memory** (destinada a la persistencia y cacheo de DataFrames y RDDs).\n>\n> La frontera entre ambas reservas es elástica: el cómputo de ejecución tiene prioridad absoluta y puede desalojar bloques almacenados en caché hacia almacenamiento secundario cuando las operaciones de shuffle requieren memoria de cálculo inmediata.`
    );
  }

  // 4. Transformar los restantes bloques <div style="..."> a Markdown puro académico
  // Bloque: RECUADRO CONCEPTUAL
  content = content.replace(
    /<div style="background-color: #fbf9f4; border-left: 5px solid #1b2030;[^"]*">\s*<strong[^>]*>RECUADRO CONCEPTUAL:\s*([^<]+)<\/strong><br><br>([\s\S]*?)<\/div>/gi,
    (m, title, body) => {
      const cleanBody = body.replace(/<br\s*[\/]?>/gi, '\n> ').replace(/<[^>]+>/g, '').trim();
      return `> **Definición ${capNum}.1 (${title.trim()})**\n> ${cleanBody}`;
    }
  );

  // Bloque: PROFUNDIZACIÓN TÉCNICA
  content = content.replace(
    /<div style="background-color: #fbf9f4; border-left: 5px solid #4a5160;[^"]*">\s*<strong[^>]*>PROFUNDIZACIÓN TÉCNICA:\s*([^<]+)<\/strong><br><br>([\s\S]*?)<\/div>/gi,
    (m, title, body) => {
      const cleanBody = body.replace(/<br\s*[\/]?>/gi, '\n> ').replace(/<[^>]+>/g, '').trim();
      return `> **Nota Técnica ${capNum}.1: ${title.trim()}**\n> ${cleanBody}`;
    }
  );

  // Bloque: PATRÓN DE IMPLEMENTACIÓN EN PRODUCCIÓN
  content = content.replace(
    /<div style="background-color: #fbf9f4; border-left: 5px solid #2f6b52;[^"]*">\s*<strong[^>]*>PATRÓN DE IMPLEMENTACIÓN EN PRODUCCIÓN:\s*([^<]+)<\/strong><br><br>([\s\S]*?)<\/div>/gi,
    (m, title, body) => {
      const cleanBody = body.replace(/<br\s*[\/]?>/gi, '\n> ').replace(/<[^>]+>/g, '').trim();
      return `> **Consideraciones de Producción ${capNum}.1: ${title.trim()}**\n> ${cleanBody}`;
    }
  );

  // Bloque: CONSIDERACIONES CRÍTICAS Y ANTI-PATRONES
  content = content.replace(
    /<div style="background-color: #fbf9f4; border-left: 5px solid #9b2c2c;[^"]*">\s*<strong[^>]*>CONSIDERACIONES CRÍTICAS Y ANTI-PATRONES<\/strong><br><br>([\s\S]*?)<\/div>/gi,
    (m, body) => {
      const cleanBody = body.replace(/<br\s*[\/]?>/gi, '\n> ').replace(/<[^>]+>/g, '').trim();
      return `> **Criterios de Fiabilidad y Anti-Patrones ${capNum}.1**\n> ${cleanBody}`;
    }
  );

  // Bloque: EJERCICIOS DE CONSOLIDACIÓN
  content = content.replace(
    /<div style="background-color: #fbf9f4; border-left: 5px solid #8a7a5c;[^"]*">\s*<strong[^>]*>EJERCICIOS DE CONSOLIDACIÓN \[AÑADIDO\]<\/strong><br><br>([\s\S]*?)<\/div>/gi,
    (m, body) => {
      const cleanBody = body.replace(/<br\s*[\/]?>/gi, '\n').replace(/<[^>]+>/g, '').trim();
      return `## [AÑADIDO] Preguntas de Revisión y Ejercicios Prácticos\n\n${cleanBody}`;
    }
  );

  // Limpieza general de remanentes <div ...> o </div>
  content = content.replace(/<div[^>]*>/gi, '').replace(/<\/div>/gi, '');
  content = content.replace(/<strong[^>]*>(.*?)<\/strong>/gi, '**$1**');
  content = content.replace(/<em[^>]*>(.*?)<\/em>/gi, '*$1*');
  content = content.replace(/<code[^>]*>(.*?)<\/code>/gi, '`$1`');
  content = content.replace(/<br\s*[\/]?>/gi, '\n');

  // Escribir archivo limpio en libro/capitulos
  fs.writeFileSync(filePath, content, 'utf8');

  // Sincronizar en D:\estudio\libro\capitulos
  if (fs.existsSync(MIRROR_DIR)) {
    fs.writeFileSync(path.join(MIRROR_DIR, file), content, 'utf8');
  }
});

console.log('✅ Todos los capítulos elevados a formato académico puro, sin HTML sucio y sin metáforas de IA.');
