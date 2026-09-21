# Databricks notebook source
# MAGIC %md
# MAGIC # EA2 — Despliegue y gobierno de una infraestructura de datos en la nube
# MAGIC
# MAGIC **Big Data (ISD-25)** · Ingeniería de Software y Datos · IU Digital de Antioquia
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Grupo** | *41* |
# MAGIC | **Integrantes** | Johnathan Andres Velez · Yuleny Pemberty Espinosa |
# MAGIC | **Caso de estudio** | *Wanderbricks* |
# MAGIC | **Fecha de entrega** | domingo 20 de septiembre |
# MAGIC | **🎥 Enlace al video** | https://drive.google.com/file/d/19hzCvdnj-5mcmGvtbwqILCnwTPXTT9rM/view?usp=drive_link |

# COMMAND ----------

# DBTITLE 1,Celda 2
# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Contexto y problema
# MAGIC
# MAGIC En la entrega anterior (EA1), en Wanderbricks dimos el primer paso para resolver el desorden que teníamos con la información de países (countries), destinos turísticos (destinations) y anfitriones (hosts). Logramos conectar y limpiar esas tres tablas dentro de Databricks para que el equipo de marketing y expansión pudiera ver "la foto completa", identificando dónde tenemos presencia, en qué lugares se concentran los anfitriones y hacia dónde se deben dirigir las campañas publicitarias para captar nuevos alojamientos.  Sin embargo, haber escrito el código para cruzar las tablas nos dejó ver un problema más grande: un notebook no es una plataforma de datos completa. Tener el código funcionando no sirve de mucho si los datos viven desorganizados, si cualquier persona puede modificar o borrar las tablas por error, o si dependemos de que alguien abra Databricks y le dé clic a "Run" celda por celda para que la información se actualice.  Como estudiantes que apenas estamos aprendiendo a manejar estas tecnologías de Big Data, en esta segunda entrega nos toca responder por el entorno donde vive esa información. Para que Wanderbricks pueda operar de manera real y segura, nuestra infraestructura debe resolver y soportar cuatro necesidades concretas:  Organizar los datos por capas (Arquitectura Medallón): No podemos dejar la información cruda revuelta con las tablas analíticas. Necesitamos estructurar el catálogo en esquemas claros (bronce y plata como mínimo). De esta forma, los datos crudos que llegan de las fuentes quedan resguardados en bronce, y los datos limpios, cruzados y enriquecidos con métricas derivadas quedan en plata, listos para consumo analítico.  Controlar los permisos para evitar desastres (Gobierno de Datos): El equipo de marketing necesita consultar cuántos anfitriones hay por país, pero no tiene por qué ver los datos personales crudos de los anfitriones ni tampoco tener permisos para modificar o borrar tablas. Con Unity Catalog debemos definir quién puede ver qué (GRANT), de modo que un error involuntario no dañe la información de producción.  Dejar de ejecutar todo a mano (Automatización con Jobs): El proceso de carga y transformación tiene que correr solo. Necesitamos programar tareas encadenadas (un Job) donde la transformación de la capa plata solo arranque si la ingesta de la capa bronce terminó bien. Si la ingesta falla, el flujo debe detenerse para no generar reportes incompletos.  Entender la infraestructura y los costos: Debemos justificar por qué nos conviene usar una plataforma gestionada como Databricks (PaaS) en lugar de ponernos a alquilar, instalar y configurar máquinas virtuales desde cero (IaaS), entendiendo qué parte del trabajo hace el proveedor por nosotros y cuál nos corresponde administrar a nosotros.

# COMMAND ----------

# DBTITLE 1,Celda 3
# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Descripción de los datos
# MAGIC
# MAGIC Analizamos la información de Wanderbricks identificando qué datos almacenamos, su volumen estimado, la frecuencia de actualización y qué perfil consume cada nivel.
# MAGIC
# MAGIC 2.1 Datos en la infraestructura
# MAGIC
# MAGIC Organizamos nuestras fuentes mediante la arquitectura Medallón en Unity Catalog:
# MAGIC
# MAGIC * **Entidades de origen:** Líneas de pedido (lineitem) de proveedores internacionales del dataset TPC-H.
# MAGIC * **Capa Bronce:** Guarda los datos crudos e inmutables enriquecidos con metadatos de control (`_ingesta_ts` y `_origen`).
# MAGIC * **Capa Plata:** Almacena tablas limpias, cruzadas con dimensiones (proveedores, países) y con métricas derivadas (retrasos, montos netos, clasificación de entregas tardías).
# MAGIC
# MAGIC **Alcance de esta entrega:** Para fines académicos y considerando las limitaciones de la versión gratuita de Databricks, implementamos únicamente el flujo automatizado **bronce → plata**. Las agregaciones de negocio (capa oro) quedan fuera del alcance de automatización, permitiéndonos demostrar gobierno, permisos y orquestación sin consumir cuota excesiva.
# MAGIC
# MAGIC 2.2 Volumen estimado 
# MAGIC Para efectos académicos trabajamos con muestras pequeñas para cuidar la cuota del entorno, pero proyectamos el siguiente comportamiento en un entorno de producción real:
# MAGIC
# MAGIC Entidad / Capa,Registros,Tamaño aprox.,Comportamiento
# MAGIC Países (countries),~200 - 250,< 1 MB,Prácticamente estático
# MAGIC Destinos (destinations),~10k - 50k,10 - 50 MB,Crecimiento moderado
# MAGIC Anfitriones (hosts),~100k - 500k,100 - 500 MB,Crecimiento diario continuo
# MAGIC Total (Delta Lake),Cientos de miles,1 - 5 GB,Formato Parquet comprimido y optimizado
# MAGIC
# MAGIC
# MAGIC 2.3 Frecuencia de actualizaciónTipo de proceso: Carga por lotes (batch).Frecuencia: Diaria (ejecutada en la madrugada, ej. 2:00 a. m.).Justificación: Actualizar los datos una vez por noche le entrega al negocio la información lista cada mañana, optimizando los minutos de cómputo y protegiendo la cuota diaria asignada.
# MAGIC
# MAGIC 2.4 Consumidores de la información
# MAGIC
# MAGIC * **Analistas de Operaciones:** Consultan la Capa Plata para identificar proveedores con mayor tasa de retraso, países problemáticos y montos en riesgo.
# MAGIC * **Ingenieros de Datos (Estudiantes):** Administramos las capas Bronce y Plata, asegurando el funcionamiento continuo del Job automatizado y la calidad de las transformaciones.
# MAGIC * **Científicos de Datos:** Revisan Plata en modo lectura para construir modelos predictivos de retrasos en entregas.
# MAGIC * **Auditores de Seguridad:** Verifican el cumplimiento de accesos mediante Unity Catalog y revisan los registros de auditoría en system tables.

# COMMAND ----------

# DBTITLE 1,Celda 4
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Decisiones de diseño y justificación
# MAGIC ### 3.1 Diagrama de la arquitectura
# MAGIC
# MAGIC *Fuentes → ingesta → almacenamiento → procesamiento → consumo.*
# MAGIC
# MAGIC ```
# MAGIC [ samples.tpch.lineitem ]
# MAGIC [ samples.tpch.supplier ]
# MAGIC [ samples.tpch.nation   ]
# MAGIC         ↓
# MAGIC    [ INGESTA BATCH ]
# MAGIC    2% sample + metadatos
# MAGIC         ↓
# MAGIC   ┌──────────────────────┐
# MAGIC   │   BRONCE (crudo)      │ ←── Administrado por EQUIPO (estudiantes)
# MAGIC   │ bigdata_grupo41.bronce │     - Notebooks (código transformación)
# MAGIC   │   - lineitem          │     - Jobs (orquestación)
# MAGIC   └──────────────────────┘     - Permisos (GRANT/REVOKE)
# MAGIC         ↓ Job Tarea 1           - Esquemas de datos
# MAGIC    [ SPARK JOIN ]
# MAGIC    + limpieza, tipado
# MAGIC    + métricas derivadas
# MAGIC         ↓
# MAGIC   ┌──────────────────────┐
# MAGIC   │   PLATA (limpia)      │ ←── Administrado por DATABRICKS (proveedor)
# MAGIC   │ bigdata_grupo41.plata  │     - Spark runtime (motor)
# MAGIC   │   - entregas          │     - Unity Catalog (metastore)
# MAGIC   │   (con retrasos,      │     - Autoescalado de clusters
# MAGIC   │    montos, país)       │     - Almacenamiento Delta Lake
# MAGIC   └──────────────────────┘     - Actualizaciones / parches
# MAGIC         ↓                     - Lineaje automático
# MAGIC    [ CONSUMO ]
# MAGIC    SQL Warehouse / notebooks
# MAGIC    (analistas consultan plata)
# MAGIC ```
# MAGIC
# MAGIC **División de responsabilidades:**
# MAGIC
# MAGIC * **Equipo (nosotros):** Código de transformación, lógica de limpieza, reglas de negocio, permisos, Jobs.
# MAGIC * **Databricks (proveedor PaaS):** Infraestructura Spark, Unity Catalog, autoescalado, almacenamiento, disponibilidad.
# MAGIC
# MAGIC ### 3.2 Matriz de roles
# MAGIC
# MAGIC *Qué puede hacer cada rol sobre cada capa en un entorno real.*
# MAGIC
# MAGIC | Rol | Bronce | Plata |
# MAGIC |---|---|---|
# MAGIC | Analista de Operaciones | Sin acceso (datos crudos sensibles) | SELECT (consulta entregas, retrasos, montos) |
# MAGIC | Ingeniero de Datos | MODIFY (corrige ingesta, reprocesa lotes) | MODIFY (ajusta reglas de limpieza y transformación) |
# MAGIC | Administrador | ALL PRIVILEGES | ALL PRIVILEGES |
# MAGIC
# MAGIC **Nota:** En este proyecto académico implementamos únicamente bronce y plata. En producción real, una capa oro agregaría métricas de negocio (KPIs por país, proveedor o periodo) con permisos de solo lectura para analistas y ejecutivos.
# MAGIC
# MAGIC ### 3.3 Especificación del equivalente IaaS
# MAGIC
# MAGIC *Se diseña, no se implementa. Máquinas, dimensionamiento, software, red, almacenamiento y esfuerzo estimado.*
# MAGIC
# MAGIC **Arquitectura mínima viable (IaaS en AWS):**
# MAGIC
# MAGIC | Componente | Especificación | Justificación |
# MAGIC |---|---|---|
# MAGIC | **Compute (Spark)** | 1 master (m5.xlarge, 4 vCPU, 16 GB) + 2 workers (m5.large, 2 vCPU, 8 GB cada uno) | Volumen actual: 600K filas. Workers para paralelizar joins. |
# MAGIC | **Sistema Operativo** | Ubuntu Server 22.04 LTS | Compatible con Spark 3.x, soporte largo plazo. |
# MAGIC | **Spark** | Apache Spark 3.5.0 standalone | Instalar manualmente: descargar binarios, configurar spark-env.sh, levantar master/workers, abrir puertos 7077, 8080. |
# MAGIC | **Metastore** | Hive Metastore 3.1 + PostgreSQL 14 (t3.micro) | Almacenar esquemas, particiones. Requiere instalar Hive, configurar `hive-site.xml`, sincronizar con Spark. |
# MAGIC | **Orquestador** | Apache Airflow 2.7 (t3.medium) | Reemplazo del Job nativo. Instalar con pip, configurar webserver/scheduler, escribir DAGs en Python para encadenar tareas bronce → plata. |
# MAGIC | **Almacenamiento** | S3 bucket (Standard, 10 GB iniciales) + EBS para logs (20 GB gp3) | Delta Lake sobre S3. Configurar credenciales IAM, instalar conector hadoop-aws. |
# MAGIC | **Red** | VPC privada, security groups para inter-VM, NLB si se expone SQL endpoint | Configurar reglas: Spark (7077), Hive (9083), Airflow (8080), SSH (22). |
# MAGIC | **Gobierno / Permisos** | Apache Ranger 2.3 (t3.medium adicional) | Unity Catalog no existe en open source. Ranger replica GRANT/REVOKE pero requiere integración manual con Hive y políticas XML. |
# MAGIC
# MAGIC **Esfuerzo de puesta en marcha (estimado):**
# MAGIC
# MAGIC * **Semana 1-2:** Aprovisionar VMs, configurar VPC/security groups, instalar y configurar Spark standalone (master + workers), probar cluster con `spark-shell`.
# MAGIC * **Semana 3:** Instalar PostgreSQL, Hive Metastore, conectar Spark al metastore, crear esquemas bronce/plata manualmente con DDL.
# MAGIC * **Semana 4:** Instalar Airflow, configurar conexión Spark, escribir DAGs para orquestar ingesta → transformación, debuggear fallos de red/permisos.
# MAGIC * **Semana 5:** Instalar Ranger, definir políticas de acceso, integrar con Hive (plugins), testear permisos.
# MAGIC * **Total:** **~5 semanas** de trabajo a tiempo completo para un ingeniero con experiencia previa en estas tecnologías.
# MAGIC
# MAGIC **Esfuerzo operativo continuo:**
# MAGIC
# MAGIC * **Semanal:** Monitorear logs de Spark (driver/executor crashes), ajustar memoria/cores manualmente según carga, revisar espacio en disco.
# MAGIC * **Mensual:** Aplicar parches de seguridad al OS (apt upgrade), actualizar Spark/Hive/Airflow (breaking changes requieren pruebas), optimizar tablas Delta (VACUUM, OPTIMIZE) manualmente con scripts cron.
# MAGIC * **Trimestral:** Evaluar si agregar/quitar workers según crecimiento de datos (no hay autoescalado — cada cambio requiere modificar configuración y reiniciar cluster).
# MAGIC
# MAGIC **Costo mensual estimado (AWS us-east-1, on-demand, sin Reserved Instances):**
# MAGIC
# MAGIC * Compute: master (m5.xlarge) + 2 workers (m5.large) + Airflow (t3.medium) + Ranger (t3.medium) + PostgreSQL (t3.micro) = ~\$350/mes si corren 24/7. Reducir a ~\$100/mes si solo corren 8 hrs/día (requiere scripting para start/stop automático).
# MAGIC * Storage: S3 (10 GB) + EBS (60 GB total) = ~\$15/mes.
# MAGIC * **Total:** **\$115-365/mes** + tiempo humano (80-160 hrs/mes de administración y monitoreo).
# MAGIC
# MAGIC **Conclusión:** Para un proyecto académico de 2 meses con un equipo de 2 estudiantes sin experiencia previa en administración de sistemas distribuidos, el IaaS es **inviable**. La puesta en marcha consumiría todo el tiempo del proyecto, dejando cero días para implementar la lógica de negocio o aprender gobierno de datos.
# MAGIC
# MAGIC ### 3.4 Comparación IaaS / PaaS / SaaS
# MAGIC
# MAGIC | Criterio | IaaS | PaaS | SaaS |
# MAGIC |---|---|---|---|
# MAGIC | Control | **Alto**: Administramos SO, Spark, Hive, red, seguridad, parches. Libertad total pero responsabilidad total. | **Medio**: Databricks administra la plataforma Spark, Unity Catalog y runtime. Nosotros administramos notebooks, Jobs, permisos y datos. | **Bajo**: El proveedor administra toda la infraestructura y la aplicación. Solo configuramos parámetros de negocio y consumimos. |
# MAGIC | Tiempo hasta el primer resultado | **Semanas**: Aprovisionar VMs, instalar Spark/Hadoop, configurar Hive Metastore, montar Airflow, configurar red y seguridad. | **Horas**: Crear catálogo, esquemas y escribir el primer notebook. La plataforma ya está lista. | **Minutos**: Acceder, cargar datos y consultar. Sin configuración de infraestructura. |
# MAGIC | Esfuerzo operativo | **Alto**: Actualizaciones de SO, parches de seguridad, monitoreo de cluster, ajuste manual de capacidad, backups, alta disponibilidad. | **Bajo**: Solo Jobs, datos y permisos. Databricks maneja autoescalado, actualizaciones, disponibilidad y optimizaciones. | **Mínimo**: El proveedor opera todo. Nosotros solo usamos la interfaz. |
# MAGIC | Costo | **Variable**: Pago por VM + almacenamiento + red + licencias (Spark es open source pero Hive, Airflow requieren administración). Desperdicio en capacidad ociosa si no optimizamos. | **Optimizado**: Pago por DBU (compute) + almacenamiento. Autoescalado reduce desperdicio. Sin costo de administración de infraestructura. | **Predecible**: Suscripción fija mensual o por usuario. Puede ser más caro a largo plazo pero sin sorpresas. |
# MAGIC | Escalabilidad | **Manual**: Provisionar más VMs, reconfigurar cluster, ajustar Spark manualmente. Proceso lento y propenso a errores. | **Automática**: Autoescalado nativo. El cluster crece y decrece según demanda. Serverless compute disponible. | **Transparente**: El proveedor escala por nosotros. Capacidad ilimitada desde la perspectiva del usuario. |
# MAGIC | Gobierno | **DIY**: Implementar permisos en Hive, configurar Ranger o similar, auditoría manual, lineaje requiere herramientas adicionales. | **Integrado**: Unity Catalog nativo con GRANT/REVOKE SQL estándar, lineaje automático, auditoría en system tables. | **Limitado**: Gobierno predefinido por el proveedor. Poco control sobre políticas personalizadas. |
# MAGIC
# MAGIC **Conclusión para el proyecto académico:**
# MAGIC
# MAGIC Para este caso de estudio y las restricciones del equipo, **PaaS (Databricks Community Edition simulando un entorno productivo) es la opción óptima**:
# MAGIC
# MAGIC * **IaaS quedaría descartado** porque somos estudiantes en formación, sin experiencia en administración de infraestructura distribuida (VMs, redes, Spark bare-metal, Hive Metastore, Airflow). Montar esa pila desde cero nos tomaría semanas y desviaría el foco del objetivo académico: aprender gobierno de datos, arquitectura medallón y automatización con Jobs. El esfuerzo operativo continuo (parches de seguridad, monitoreo 24/7, optimización manual de clusters) no es viable para un equipo de dos personas con otras asignaturas.
# MAGIC
# MAGIC * **SaaS sería insuficiente** porque necesitamos control total sobre el código de transformación (PySpark, Python, SQL personalizado para joins y métricas derivadas), la arquitectura de capas (bronce → plata con reglas propias) y las políticas de gobierno específicas (GRANT/REVOKE por esquema). Una solución SaaS tipo Tableau, Power BI o Google Data Studio solo sirve para consumir datos ya preparados y visualizarlos — no para ingeniería de datos ni orquestación de pipelines.
# MAGIC
# MAGIC * **PaaS (Databricks) balancea ambos extremos**: nos da control total sobre la lógica de negocio (notebooks con código Python/SQL, Jobs encadenados con dependencias, permisos granulares por esquema), mientras Databricks administra la complejidad técnica subyacente: Spark runtime preconfigurado, autoescalado de clusters, Unity Catalog con lineaje automático, y actualizaciones de seguridad. 
# MAGIC
# MAGIC   El tiempo hasta el primer resultado fue **horas** (crear catálogo, esquemas, escribir transformaciones bronce → plata, configurar Job y probarlo), no semanas. El costo es optimizado porque solo pagamos por DBUs consumidos durante la ejecución (en nuestro caso, versión gratuita con límites). El gobierno está integrado nativamente: cada GRANT se refleja instantáneamente sin configurar roles externos ni bases auxiliares.
# MAGIC
# MAGIC   Para un equipo pequeño (dos estudiantes), volumen moderado (600K filas ≈ 1 GB), carga batch diaria y restricciones de cuota, **PaaS maximiza el aprendizaje y minimiza la fricción operativa**. Nos permite concentrarnos en la lógica de transformación y gobierno — que es lo que debemos aprender — sin perder tiempo instalando software de infraestructura.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Implementación
# MAGIC ### 4.1 Organización del entorno

# COMMAND ----------

CATALOGO = "bigdata_grupo41"

for capa in ["bronce", "plata", "oro"]:
    print(f"--- {capa.upper()} ---")
    try:
        display(spark.sql(f"SHOW TABLES IN {CATALOGO}.{capa}"))
    except Exception as e:
        print(f"  (esquema no creado todavía: {str(e).splitlines()[0]})\n")

try:
    display(spark.sql(f"SHOW VOLUMES IN {CATALOGO}.bronce"))
except Exception as e:
    print(f"(volúmenes no disponibles: {str(e).splitlines()[0]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Permisos
# MAGIC
# MAGIC *Al menos dos sentencias GRANT con niveles distintos sobre objetos distintos.*

# COMMAND ----------

# Asegurar que los esquemas existen antes de otorgar permisos
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.bronce")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.plata")

# GRANT 1 — analistas de marketing / negocio: solo consultan resultados agregados, nunca modifican
spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOGO} TO `account users`")

# GRANT 2 — corregido: antes MODIFY sobre plata se otorgaba a `account users` (o sea, a TODOS
# los usuarios de la cuenta, incluidos los analistas), lo cual contradice la matriz de roles de
# la sección 3.2 (solo el equipo de datos debería poder reprocesar plata).
# Separamos el permiso: todos pueden LEER plata (los analistas también la consultan, según 2.4),
# pero solo el equipo de datos (nosotros) puede escribirla/reprocesarla.
# Usar el correo real registrado en Databricks (no el institucional @correo.iudigital.edu.co)
EQUIPO_DATOS = [
    "johnathan.velez@est.iudigital.edu.co",   # correo registrado en Databricks
    # "yuleny.pemberty@est.iudigital.edu.co",  # descomentar y ajustar cuando el segundo integrante esté registrado
]

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOGO}.plata TO `account users`")
spark.sql(f"GRANT SELECT ON SCHEMA {CATALOGO}.plata TO `account users`")       # lectura para analistas/científicos de datos
for correo in EQUIPO_DATOS:
    spark.sql(f"GRANT MODIFY ON SCHEMA {CATALOGO}.plata TO `{correo}`")       # solo el equipo de datos puede escribir/reprocesar

# Evidencia obligatoria
display(spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOGO}.plata"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Linaje
# MAGIC
# MAGIC ![WhatsApp Image 2026-09-17 at 7.52.02 PM_1789692744646.jpeg](./WhatsApp Image 2026-09-17 at 7.52.02 PM_1789692744646.jpeg "WhatsApp Image 2026-09-17 at 7.52.02 PM_1789692744646.jpeg")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Automatización
# MAGIC
# MAGIC *Un Job con al menos dos tareas encadenadas y una programación definida.
# MAGIC Insertar la captura de una ejecución exitosa e indicar el identificador del Job.*

# COMMAND ----------

from pyspark.sql import functions as F

# --- PLATA: limpia, unida y con la métrica derivada
lineas = spark.table(f"{CATALOGO}.bronce.lineitem").alias("l")
prov   = spark.table("samples.tpch.supplier").alias("s")
paises = spark.table("samples.tpch.nation").alias("n")

(lineas
   .join(prov,   F.col("l.l_suppkey")   == F.col("s.s_suppkey"),   "inner")
   .join(paises, F.col("s.s_nationkey") == F.col("n.n_nationkey"), "inner")
   .filter(F.col("l_extendedprice") > 0)
   .select(
       F.col("l.l_orderkey").alias("pedido_id"),
       F.col("s.s_suppkey").alias("proveedor_id"),
       F.trim(F.col("s.s_name")).alias("proveedor"),
       F.trim(F.col("n.n_name")).alias("pais"),
       F.col("l.l_commitdate").alias("fecha_comprometida"),
       F.col("l.l_receiptdate").alias("fecha_recibido"),
       F.col("l.l_extendedprice").alias("precio"),
       F.col("l.l_discount").alias("descuento"),
   )
   .withColumn("monto_neto", F.round(F.col("precio") * (1 - F.col("descuento")), 2))
   .withColumn("dias_retraso", F.datediff("fecha_recibido", "fecha_comprometida"))
   .withColumn("entrega_tardia", F.col("dias_retraso") > 0)
   .write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(f"{CATALOGO}.plata.entregas"))

print(f"plata.entregas: {spark.table(f'{CATALOGO}.plata.entregas').count():,} filas")

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "bigdata_grupo41"   # corregido: antes decía "abastecimiento_demo" (resto de una plantilla),
                                # lo que habría escrito la ingesta en un catálogo distinto al documentado
                                # en el resto del notebook (celdas 5, 7, 13).

(spark.table("samples.tpch.lineitem")
   .sample(fraction=0.02, seed=42)
   .withColumn("_ingesta_ts", F.current_timestamp())
   .withColumn("_origen", F.lit("samples.tpch.lineitem"))
   .write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(f"{CATALOGO}.bronce.lineitem"))

print(f"Bronce actualizada: {spark.table(f'{CATALOGO}.bronce.lineitem').count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ![WhatsApp Image 2026-09-17 at 7.53.04 PM_1789692820030.jpeg](./WhatsApp Image 2026-09-17 at 7.53.04 PM_1789692820030.jpeg "WhatsApp Image 2026-09-17 at 7.53.04 PM_1789692820030.jpeg")

# COMMAND ----------

# DBTITLE 1,Cell 14
from datetime import datetime

CATALOGO = "bigdata_grupo41"

for capa, tabla in [("bronce", "lineitem"), ("plata", "entregas")]:
    df = spark.table(f"{CATALOGO}.{capa}.{tabla}")
    hist = spark.sql(f"DESCRIBE HISTORY {CATALOGO}.{capa}.{tabla}").first()
    print(f"{capa}.{tabla:<12} {df.count():>10,} filas   última escritura: {hist['timestamp']}")

# COMMAND ----------

# DBTITLE 1,Sección 5
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Resultados
# MAGIC
# MAGIC La implementación de la infraestructura de datos con gobierno y automatización fue exitosa. A continuación se presentan los resultados obtenidos en cada uno de los componentes clave:
# MAGIC
# MAGIC ### 5.1 Organización del entorno (Arquitectura Medallón)
# MAGIC
# MAGIC **✅ Logrado:** Se creó el catálogo `bigdata_grupo41` con esquemas `bronce` y `plata` en Unity Catalog.
# MAGIC
# MAGIC * **Capa Bronce:** 1 tabla (`lineitem`) con datos crudos del dataset TPC-H enriquecidos con metadatos de control (`_ingesta_ts` y `_origen`).
# MAGIC * **Capa Plata:** 1 tabla (`entregas`) con datos limpios, cruzados con dimensiones de proveedores y países, y con métricas derivadas de negocio.
# MAGIC * **Capa Oro:** No implementada en esta entrega (fuera del alcance académico).
# MAGIC
# MAGIC **Evidencia:** Celda 6 muestra el inventario con `SHOW TABLES` confirmando la existencia de `bronce.lineitem` y `plata.entregas`.
# MAGIC
# MAGIC **Alcance de EA2:** El Job automatizado implementado en esta entrega procesa **bronce → plata** (transformación de `lineitem` a `entregas` con joins a `samples.tpch.supplier` y `samples.tpch.nation`, métricas derivadas y limpieza de strings).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.2 Gobierno de datos (Permisos granulares)
# MAGIC
# MAGIC **✅ Logrado:** Se aplicaron políticas de acceso diferenciadas mediante Unity Catalog:
# MAGIC
# MAGIC * **Nivel Catálogo:**
# MAGIC   - `GRANT USE CATALOG` → Otorgado a `account users` para permitir el acceso básico al catálogo `bigdata_grupo41`.
# MAGIC
# MAGIC * **Esquema `plata`:**
# MAGIC   - `GRANT USE SCHEMA` y `GRANT SELECT` → Todos los usuarios de la cuenta (incluyendo analistas y científicos de datos) pueden **navegar y leer** las entregas ya limpias.
# MAGIC   - `GRANT MODIFY` → Otorgado únicamente a los correos del **equipo de datos** (integrantes registrados), no a `account users`, para que solo el equipo pueda **escribir, corregir y reprocesar** la capa plata.
# MAGIC
# MAGIC **Corrección aplicada tras revisión:** En una primera versión, el `GRANT MODIFY` sobre `plata` se había otorgado por error a `account users` (todos los usuarios de la cuenta), lo que contradecía la matriz de roles de la sección 3.2. Se corrigió para que `MODIFY` quede restringido a los correos del equipo de datos, y `SELECT` se dejó abierto a todos porque analistas y científicos de datos también consultan plata (ver 2.4).
# MAGIC
# MAGIC **Nota sobre el esquema `bronce`:** Aunque el esquema fue creado (Celda 8), no se otorgaron permisos explícitos de lectura sobre él a `account users` — solo el equipo de datos tiene acceso implícito como propietarios. Esto preserva la seguridad de los datos crudos.
# MAGIC
# MAGIC **Evidencia:** Celda 8 muestra los comandos `GRANT` ejecutados y la verificación con `SHOW GRANTS` confirmando que los permisos están activos.
# MAGIC
# MAGIC **Impacto:** En un entorno productivo, esto previene que analistas de negocio modifiquen o eliminen accidentalmente datos crudos o intermedios, mientras mantienen acceso de lectura a los datos que necesitan.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.3 Trazabilidad (Lineaje automático)
# MAGIC
# MAGIC **✅ Logrado:** Unity Catalog generó automáticamente el grafo de lineaje que muestra las dependencias entre tablas.
# MAGIC
# MAGIC **Evidencia:** Celda 9 contiene la captura del lineaje desde la interfaz de Databricks.
# MAGIC
# MAGIC **Utilidad:** El lineaje permite:
# MAGIC * Rastrear el origen de cualquier dato en plata hasta su fuente en bronce.
# MAGIC * Identificar qué tablas downstream se afectarían si se modifica una tabla upstream.
# MAGIC * Auditar transformaciones para cumplimiento regulatorio.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.4 Automatización (Job encadenado funcionando)
# MAGIC
# MAGIC **✅ Logrado:** Se creó y ejecutó exitosamente un Databricks Job con 2 tareas encadenadas:
# MAGIC
# MAGIC 1. **Tarea 1 - Ingesta Bronce:** Carga `samples.tpch.lineitem` (2% sample) → `bigdata_grupo41.bronce.lineitem` con metadatos `_ingesta_ts` y `_origen`.
# MAGIC 2. **Tarea 2 - Transformación Plata:** Join de `bronce.lineitem` + `samples.tpch.supplier` + `samples.tpch.nation` → `bigdata_grupo41.plata.entregas` con métricas derivadas:
# MAGIC    - `monto_neto` = precio × (1 - descuento)
# MAGIC    - `dias_retraso` = datediff(fecha_recibido, fecha_comprometida)
# MAGIC    - `entrega_tardia` = (dias_retraso > 0)
# MAGIC
# MAGIC **Programación:** Configurado para ejecutarse diariamente a las 2:00 AM (carga batch nocturna).
# MAGIC
# MAGIC **Evidencia:** 
# MAGIC * Celda 13 muestra la captura de la ejecución exitosa del Job desde la interfaz de Databricks.
# MAGIC * Celda 14 muestra la verificación post-ejecución:
# MAGIC   - `bronce.lineitem`: **599,820 filas** procesadas
# MAGIC   - `plata.entregas`: **599,820 filas** transformadas
# MAGIC   - Última escritura: **2026-09-09 03:41** (ambas capas actualizadas en la misma ventana de ejecución)
# MAGIC
# MAGIC **Dependencias:** La Tarea 2 solo se ejecuta si la Tarea 1 termina exitosamente. Si la ingesta falla, el Job se detiene y no genera reportes con datos incompletos.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.5 Calidad y volumen de datos procesados
# MAGIC
# MAGIC **✅ Logrado:** Se procesaron exitosamente **599,820 registros** (2% sample del dataset TPC-H lineitem).
# MAGIC
# MAGIC **Transformaciones aplicadas en plata:**
# MAGIC * Filtrado de registros inválidos: `l_extendedprice > 0`
# MAGIC * Limpieza de strings: `TRIM()` en nombres de proveedores y países
# MAGIC * Joins inner: Solo registros con proveedores y países válidos
# MAGIC * Derivación de métricas de negocio: monto neto, días de retraso, clasificación de entregas tardías
# MAGIC
# MAGIC **Formato de almacenamiento:** Delta Lake (Parquet comprimido) con soporte ACID, time travel y schema evolution.
# MAGIC
# MAGIC **Tamaño estimado:** ~80-120 MB por capa (formato comprimido). Proyección para producción (100% del dataset): ~4-6 GB.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.6 Resumen de cumplimiento de requisitos
# MAGIC
# MAGIC | Requisito | Estado | Evidencia |
# MAGIC |-----------|--------|----------|
# MAGIC | Arquitectura Medallón (bronce/plata) | ✅ Completo | Celda 6: esquemas y tablas creados |
# MAGIC | Permisos granulares (GRANT) | ✅ Completo | Celda 8: 2 GRANT sobre oro y plata |
# MAGIC | Lineaje automático | ✅ Completo | Celda 9: captura del grafo |
# MAGIC | Job encadenado (≥2 tareas) | ✅ Completo | Celdas 11-14: código + ejecución |
# MAGIC | Programación temporal | ✅ Completo | Job configurado para 2:00 AM diario |
# MAGIC | Comparación IaaS/PaaS/SaaS | ✅ Completo | Sección 3.4: tabla + conclusión justificada |
# MAGIC | Datos procesados correctamente | ✅ Completo | Celda 14: 599,820 filas en ambas capas |
# MAGIC
# MAGIC **Tiempo total de implementación:** ~6 horas (vs. 5+ semanas estimadas para equivalente IaaS).
# MAGIC
# MAGIC **Resultado:** Infraestructura de datos funcional, gobernada y automatizada lista para operación continua.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Conclusiones
# MAGIC
# MAGIC ### Lo que logró funcionar
# MAGIC
# MAGIC **Arquitectura ordenada (Bronce y Plata):** Guardamos los datos crudos en Bronce (tabla `lineitem` con 599,820 filas, 2% sample de TPC-H) enriquecidos con metadatos de control (`_ingesta_ts` y `_origen`), y generamos la capa Plata (tabla `entregas`) con tres métricas derivadas (monto neto, días de retraso y entrega tardía) cruzando proveedores y países, sin modificar la fuente original.
# MAGIC
# MAGIC **Proceso automatizado:** El Job funcionó en secuencia: la Tarea 2 (transformación a Plata) inició automáticamente solo después de que la Tarea 1 (ingesta a Bronce) terminó exitosamente, procesando las 599,820 filas en la misma ventana de ejecución.
# MAGIC
# MAGIC **Permisos granulares funcionales:** Separamos correctamente los permisos sobre `plata`: todos pueden leer (`SELECT`), pero solo el equipo de datos puede escribir/reprocesar (`MODIFY`). Esto previene que analistas modifiquen accidentalmente datos intermedios.
# MAGIC
# MAGIC **Ahorro de tiempo:** Crear este pipeline automatizado en la nube nos tomó ~6 horas, frente a las 5+ semanas estimadas para configurar el equivalente IaaS (instalar Spark standalone, Hive Metastore, Airflow, Ranger, etc. sobre máquinas virtuales).
# MAGIC
# MAGIC ### Lo que no nos salió bien
# MAGIC
# MAGIC **Errores en el código inicial:** En la primera versión, las celdas tenían errores que bloqueaban la ejecución:
# MAGIC - Faltaba el import `from pyspark.sql import functions as F` en la Celda 11, causando `NameError`.
# MAGIC - Los correos en `EQUIPO_DATOS` no coincidían con los registrados en Databricks (`@correo.iudigital.edu.co` vs `@est.iudigital.edu.co`), causando `PRINCIPAL_DOES_NOT_EXIST`.
# MAGIC - Se intentaba otorgar permisos sobre el esquema `oro` que no existía, causando `SCHEMA_NOT_FOUND`.
# MAGIC
# MAGIC **Capa Oro no implementada:** El alcance académico de EA2 solo cubrió Bronce → Plata. La capa Oro (con agregaciones de negocio tipo KPIs por país/proveedor/periodo) quedó fuera del flujo automatizado.
# MAGIC
# MAGIC **Inconsistencia documental:** La documentación inicial (sección 5) mencionaba tablas como `bookings_bronce`, `reviews_bronce` y volúmenes que no existen — texto copiado de otro proyecto que no coincidía con las tablas reales (`lineitem` y `entregas`).
# MAGIC
# MAGIC ### Lo que haríamos distinto
# MAGIC
# MAGIC **Validar imports y dependencias antes de ejecutar:** Verificar que todas las celdas tengan sus imports completos y que las variables/tablas necesarias existan antes de referenciarlas.
# MAGIC
# MAGIC **Estandarizar nombres desde el inicio:** Definir el nombre del catálogo (`bigdata_grupo41`) en una celda de configuración al principio y reutilizarlo en todas las celdas, evitando que código de prueba guarde datos en catálogos incorrectos.
# MAGIC
# MAGIC **Documentar lo que realmente implementamos:** Escribir la sección 5 (Resultados) DESPUÉS de ejecutar el código, no antes, para que la documentación refleje exactamente las tablas y objetos creados.
# MAGIC
# MAGIC **Pipeline completo en producción:** En un entorno real, extenderíamos el Job con una Tarea 3 que genere la capa Oro a partir de Plata, completando el flujo Bronce → Plata → Oro de manera automática. Ejemplo: agregar `entregas` por país y mes para generar métricas de rendimiento de proveedores.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Reparto del trabajo y uso de IA
# MAGIC
# MAGIC | Integrante | De qué se encargó | Qué sustenta en el video |
# MAGIC |---|---|---|
# MAGIC |Yuleny Pemberty Espinosa | | |
# MAGIC |Johnathan Andres Velez | | |
# MAGIC | | | |
# MAGIC
# MAGIC **Uso de asistentes de IA:** *(indicar en qué partes se usó el Databricks Assistant u otra
# MAGIC herramienta. Está permitido; lo que se evalúa es que cada integrante pueda explicar
# MAGIC cualquier línea del código en el video.)*
# MAGIC
# MAGIC > Este reparto debe coincidir con lo que cada persona demuestra en el video y con el
# MAGIC > historial de commits del repositorio. Las tres fuentes se contrastan al calificar.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📹 Preguntas obligatorias de sustentación
# MAGIC
# MAGIC Cada integrante responde estas tres preguntas en su intervención del video:
# MAGIC
# MAGIC 1. ¿Qué parte de esta arquitectura administra el proveedor y cuál administran ustedes?
# MAGIC 2. Muestre un GRANT que ejecutó y explique a quién le está dando qué, y por qué.
# MAGIC 3. Si tuvieran que montar esto sobre máquinas virtuales, ¿qué sería lo primero que se les complicaría?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Antes de entregar
# MAGIC
# MAGIC - [ ] El diagrama de arquitectura está incluido y descrito
# MAGIC - [ ] Los dos GRANT están ejecutados y el SHOW GRANTS muestra el resultado
# MAGIC - [ ] El Job tiene dos o más tareas, está programado y hay evidencia de ejecución
# MAGIC - [ ] La comparación IaaS/PaaS/SaaS termina en una conclusión, no en una tabla suelta
# MAGIC - [ ] El enlace del video está en la portada y abre desde otra cuenta
# MAGIC - [ ] Todo confirmado en /ea2 y el HTML subido a Canvas