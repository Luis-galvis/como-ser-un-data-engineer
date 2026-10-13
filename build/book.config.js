// Configuración editorial del libro. Es la única fuente de verdad para
// portada, página legal y estructura de partes.
module.exports = {
  title: 'Cómo ser un Data Engineer',
  titlePre: 'Cómo ser un',
  titleMain: 'DATA ENGINEER',
  subtitle: 'Una mirada a fondo de los fundamentos y los procesos prácticos para llegar de cero a Data Engineer',
  author: 'Luis Galvis',
  year: 2026,
  edition: 'Primera edición · Octubre de 2026',
  place: 'Ibagué, Tolima, Colombia',
  dedication:
    'A mis padres, por ser el cimiento inquebrantable, el sacrificio silencioso y la luz que impulsó cada uno de mis pasos. A mi pareja, por su amor incondicional, su infinita paciencia y su aliento constante en cada noche de estudio y creación. Y a todos los ingenieros y científicos que buscan comprender la esencia profunda de los datos más allá de las herramientas efímeras.',
  rights: '© 2026 Luis Galvis. Todos los derechos reservados.',
  rightsText:
    'Ninguna parte de esta obra puede ser reproducida, almacenada en sistemas de recuperación ni transmitida, total o parcialmente, por ningún medio, sin la autorización previa y por escrito del autor.',
  codeLicense:
    'Los fragmentos de código fuente (PySpark, SQL, Python, HCL) se distribuyen bajo la licencia de código abierto MIT y pueden reutilizarse libremente en entornos productivos, conservando el aviso de licencia.',
  // Cada parte empieza en el archivo indicado (orden alfabético de capitulos/).
  parts: [
    { title: 'Panorama y fundamentos de la profesión', start: '00-roadmap-y-panorama.md' },
    { title: 'Computación distribuida con Apache Spark', start: '03-de-hadoop-a-spark.md' },
    { title: 'Ingesta, formatos y esquemas', start: '07-ingesta-archivos.md' },
    { title: 'Transformación con la API DataFrame', start: '10-filtrado-condicionales.md' },
    { title: 'El motor interno y la optimización en Spark', start: '13-evaluacion-perezosa-y-dag.md' },
    { title: 'Arquitectura Lakehouse y transacciones ACID', start: '17-arquitectura-medallon.md' },
    { title: 'Procesamiento en tiempo real (streaming)', start: '21-fundamentos-streaming-kafka.md' },
    { title: 'Orquestación, modelado y DataOps', start: '23-orquestacion-airflow-adf.md' },
    { title: 'Observabilidad y calidad de datos', start: '27-observabilidad-monitoreo-logging.md' },
    { title: 'Arquitectura cloud, System Design e Inteligencia Artificial', start: '29-cloud-data-architecture-y-costos.md' },
    { title: 'Proyecto Integrador y Plataforma de Producción', start: '34-proyecto-integrador-capstone.md' },
    { title: 'Apéndices de Referencia, Glosario y Epílogo', start: '35-apendice-a-glosario-enciclopedico.md' },
  ],
  guide: [
    'Esta obra está concebida como un manual riguroso de ingeniería que acompaña la progresión integral desde los fundamentos de la computación analítica hasta el diseño de sistemas de datos a escala empresarial.',
    'El texto se organiza en doce partes temáticas. Comienza con los cimientos de Linux, Git, Docker, bases de datos relacionales OLTP, Python moderno y SQL declarativo avanzado; continúa con el cómputo distribuido en Apache Spark; profundiza en la optimización física de memoria y shuffles; aborda la arquitectura Lakehouse con Delta Lake y Unity Catalog; trata el procesamiento en tiempo real con Kafka y Structured Streaming; cubre DataOps, Terraform, observabilidad, calidad con Great Expectations, FinOps, System Design, plataformas empresariales (AWS, Azure Fabric, GCP, Databricks y Snowflake), seguridad regulatoria (GDPR y Ley 1581), persistencia políglota NoSQL, ingeniería de datos para IA y LLMs; culmina con un proyecto integrador (capstone) de extremo a extremo y se complementa con un glosario enciclopédico de términos, guías de estimación de escala y un epílogo de ingeniería.',
    'Cada capítulo incluye objetivos de aprendizaje, definiciones formales, notas de producción, anti-patrones de la industria, un resumen técnico, ejercicios de revisión con solución paso a paso y lecturas recomendadas canónicas. Se recomienda ejecutar los bloques de código y verificar los planes de ejecución en el laboratorio reproducible provisto.',
  ],
};
