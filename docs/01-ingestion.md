Perfecto. Investigaré una arquitectura en AWS que te permita, mediante la línea de comandos, descargar masivamente archivos NetCDF del catálogo THREDDS de MeteoGalicia para un rango de años, extraer variables de viento (dirección y velocidad) y geolocalización (latitud y longitud), y almacenarlos en formato Parquet accesible desde Athena. Te informaré en cuanto tenga un diseño detallado y factible.


# Arquitectura en AWS para Descarga y Procesamiento Masivo de NetCDF de MeteoGalicia

## Descripción General de la Solución

Se propone una **arquitectura serverless en AWS** que permite, mediante la línea de comandos (CLI), descargar en masa archivos NetCDF del catálogo THREDDS de MeteoGalicia (WRF\_HIST d03) para un rango de años indicado. La solución extrae de cada archivo las variables requeridas – **latitud, longitud** (o coordenadas equivalentes de la malla), **dirección** y **velocidad del viento** a 10 m – y almacena los resultados en formato **Parquet** en **Amazon S3**. Estos archivos Parquet quedan registrados en un catálogo de datos (AWS Glue Data Catalog) para ser consultados con **Amazon Athena**.

La arquitectura prioriza la **simplicidad** y puede ejecutarse totalmente vía CLI, sin necesidad de interfaces gráficas. A continuación se describen los componentes utilizados, el flujo de trabajo de la solución y los pasos de implementación con ejemplos de comandos y scripts.

## Componentes de la Arquitectura

* **AWS S3** – Almacenamiento de los datos resultantes. Los archivos Parquet generados se guardan en un bucket de S3, organizados por fecha o año para facilitar su consulta con Athena. S3 ofrece alta durabilidad y permite que Athena acceda a los datos directamente.

* **AWS Lambda** – Funciones serverless en Python encargadas de la **descarga y procesamiento** de los archivos NetCDF. Cada función Lambda descargará un archivo desde el THREDDS, extraerá las variables de interés usando librerías open-source (e.g. **xarray** o **netCDF4**), y guardará los datos procesados en Parquet en S3. Se usa Lambda por su facilidad de ejecución sin gestionar servidores y porque las tareas de transformación son relativamente ligeras (pueden completarse en menos de 15 minutos por archivo, dentro del límite de Lambda).
* **Optional polygon region filtering** – The Lambda processing function supports filtering grid points to a user-defined polygon region. If the Step Functions input includes a `polygon` field with a list of `[lon, lat]` vertex pairs, only data points inside the polygon will be written to S3. This enables users to limit the data ingestion to an arbitrary geographic region.

* **AWS Step Functions** – Orquestación del proceso mediante una **máquina de estados**. Step Functions permite coordinar múltiples tareas Lambda de forma secuencial o paralela. Aquí se emplea para lanzar descargas y conversiones en paralelo sobre el rango de fechas solicitado, aprovechando el estado *Map* de Step Functions para **procesar múltiples archivos en paralelo**. Esto acelera la descarga masiva al ejecutar una función Lambda por cada archivo (ej. por día) de forma simultánea.

* **AWS Glue / Athena Data Catalog** – Catálogo de datos para **registrar la estructura** de los archivos Parquet en S3. Actualmente el pipeline no ejecuta ningún crawler automático; en su lugar, se crean/actualizan las tablas mediante sentencias DDL en Athena (por ejemplo, `CREATE EXTERNAL TABLE` más `MSCK REPAIR TABLE`). Este registro facilita que Amazon Athena **consulte los datos directamente** usando SQL cuando sea necesario.

* **Amazon Athena** – Servicio de consulta **SQL serverless** sobre los datos Parquet en S3. Athena permite explorar y analizar las direcciones y velocidades de viento por coordenada y tiempo sin necesidad de cargar los datos manualmente. Gracias a que los datos están en formato Parquet (columnares), las consultas Athena serán eficientes. *(Athena en sí no requiere configuración en la arquitectura; se usa posteriormente para consultar los resultados.)*

* **(Opcional) AWS EC2** – Aunque la solución principal usa Lambda y Step Functions, se considera la opción de una instancia EC2 para casos de simplicidad extrema. Un servidor EC2 con los scripts necesarios podría realizar todo el proceso (descarga, conversión y carga a S3) en un único entorno. Sin embargo, esta aproximación implica administrar una máquina y su ciclo de vida, por lo que se prioriza la variante serverless a menos que se requiera EC2 por alguna dependencia específica.

## Flujo de Trabajo Orquestado (paso a paso)

A continuación se detalla el flujo de ejecución de la arquitectura propuesta, desde la entrada del usuario hasta la disponibilidad de los datos en Athena:

1. **Inicio vía CLI:** El usuario desencadena el proceso mediante la CLI de AWS. Por ejemplo, puede invocar la ejecución de la máquina de estados de Step Functions pasando parámetros como año inicial y final. Ejemplo de comando:

   ```bash
   aws stepfunctions start-execution \
       --state-machine-arn arn:aws:states:eu-west-1:123456789012:stateMachine:DescargaProcesaNetCDF \
       --input '{ "anio_inicio": 2010, "anio_fin": 2015 }'
   ```

   Este comando inicia la orquestación para descargar y procesar archivos entre 2010 y 2015.

2. **Generación dinámica de URLs:** La primera tarea (ej. una función Lambda “GenerarLista”) determina qué archivos NetCDF del catálogo THREDDS deben descargarse. Dado que los archivos están organizados por fecha en la URL, la Lambda puede iterar por todas las fechas en el rango especificado y formar las URLs de descarga. Por ejemplo, el catálogo WRF\_HIST/d03 sigue un patrón predecible de directorios por año/mes y nombre de archivo por fecha. Se implementa un bucle sobre fechas desde *fecha\_inicio* hasta *fecha\_fin*, incrementando día a día (y potencialmente dos veces al día, e.g. 00Z y 12Z). La Lambda construye cada URL de archivo según el patrón observado (año, mes, día, hora) y las añade a una lista.

   *Justificación:* El manual de uso de THREDDS de MeteoGalicia sugiere recorrer las fechas y sustituir en la URL los componentes de año, mes y día. Por ejemplo, un *script* en Python puede definir una fecha inicial, una fecha final y un paso diario para iterar automáticamente por todas las fechas. En cada iteración se formatea la fecha en la cadena de URL correspondiente al archivo en el servidor THREDDS (usando la ruta HTTPServer). De esta forma, el proceso es dinámico y no requiere conocer de antemano la estructura interna del catálogo (simplemente aprovecha el formato consistente de nombres). En el siguiente fragmento (tomado del manual) se ilustra esta lógica de forma simplificada:

   ```python
   from datetime import datetime, timedelta
   fecha = datetime(2010, 1, 1)
   fin = datetime(2010, 1, 5)
   delta = timedelta(days=1)
   while fecha <= fin:
       year = fecha.strftime("%Y"); month = fecha.strftime("%m"); day = fecha.strftime("%d")
       file_url = f"http://mandeo.meteogalicia.es/thredds/fileServer/modelos/WRF_HIST/d03/{year}/{month}/wrf_arw_det_history_d03_{year+month+day}_0000.nc4"
       outfile = f"wrf_d03_{year+month+day}_0000.nc4"
       # Descargar el fichero
       urllib.request.urlretrieve(file_url, outfile)
       fecha += delta
   ```

   En este ejemplo (adaptado de MeteoGalicia) se recorre del 1 al 5 de enero de 2010 formando las URLs diarias. Nótese que para incluir ejecuciones de las 12:00 UTC, podría ampliarse el bucle para dos descargas por día (cambiando la parte `_0000.nc4` a `_1200.nc4` para la segunda corrida). La Lambda **GenerarLista** devuelve la lista completa de URLs de archivos a descargar.

3. **Descarga y procesamiento en paralelo:** Step Functions recibe la lista de URLs y entra en un estado *Map*, el cual lanza una **función Lambda de procesamiento** por cada URL (en paralelo). Cada instancia de esta Lambda ejecuta las siguientes acciones para su archivo asignado:

   * **Descarga del archivo NetCDF:** utilizando la URL proporcionada. Esto se puede hacer con una petición HTTP (por ejemplo usando la librería `requests` de Python o `urllib`). Alternativamente, se podría usar directamente el enlace OPeNDAP del THREDDS con xarray, pero la forma más sencilla es descargar el archivo completo vía HTTP(S) (endpoint **fileServer** del THREDDS) a almacenamiento temporal (en Lambda, `/tmp` dispone de hasta 10 GB de espacio efímero).
   * **Lectura y extracción de variables:** una vez descargado el `.nc4`, la Lambda lo abre con una librería Python apropiada (por ejemplo, **xarray** con el motor netCDF4). Xarray facilita manipular datos multidimensionales; permite cargar solo las variables necesarias para ahorrar memoria. Se extraen las variables de **latitud** y **longitud** de cada punto de grilla (muchos archivos WRF incluyen variables `lat`/`lon` en cada punto) y las variables de **dirección del viento** (`dir`) y **velocidad del viento** (`mod`) a 10 metros. Estas dos últimas pueden venir pre-calculadas en el archivo (como indica el catálogo, `dir = wind direction at 10m`, `mod = wind module at 10m`). Si no estuvieran directamente, podrían calcularse a partir de componentes U y V (por ejemplo `dir = arctan2(U,V)` y `vel = sqrt(U^2+V^2)`), pero en nuestro caso MeteoGalicia ya provee `dir` y `mod` para cada punto.
   * **Transformación a formato tabular:** la Lambda combina esas variables en una estructura tabular, por ejemplo convirtiendo el xarray Dataset a un **DataFrame de pandas**. Cada fila representará un punto en un tiempo determinado, con columnas: latitud, longitud, dirección\_viento, velocidad\_viento, y posiblemente timestamp de la salida. Dado que cada archivo contiene múltiples tiempos (ej. salidas horarias de una corrida WRF), se incluirá también el tiempo correspondiente a cada registro en la tabla. Si el volumen por archivo es muy grande, se podría procesar en fragmentos (por ejemplo por cada tiempo) para no exceder memoria, pero en general un Lambda con suficiente memoria (p. ej. 2048 MB) debería manejar un archivo WRF de \~66 MB con \~96 tiempos sin problemas.
   * **Escritura en Parquet y carga a S3:** la Lambda serializa el DataFrame resultante en formato **Parquet** (por ejemplo, usando pandas + pyarrow). Un fragmento ilustrativo basado en un ejemplo de AWS sería:

     ```python
     df.to_parquet("/tmp/result.parquet")  # Escribir datos locales en Parquet:contentReference[oaicite:11]{index=11}
     s3_client = boto3.client('s3')
     s3_client.upload_file("/tmp/result.parquet", BUCKET_DESTINO, clave_s3)
     ```

     Donde `clave_s3` podría ser algo como `datos_wrf/ano=2010/mes=01/dia=01/run=00/parcial.parquet`. Es decir, se **organizan los Parquet en particiones por fecha** (y hora de corrida) para optimizar futuras consultas Athena. Cada Lambda coloca su resultado en S3 bajo la ruta correspondiente. El enfoque de usar Parquet en S3 es recomendado porque Athena puede leer Parquet de manera eficiente mediante su **SerDe** nativo. En este punto, cada Lambda ha cumplido su tarea y finaliza.

4. **Consolidación y registro en catálogo:** Una vez que todas las Lambdas del paso anterior terminan (es decir, tras procesar todos los archivos del rango), Step Functions puede opcionalmente ejecutar un paso final de consolidación. Dado que ya escribimos directamente en Parquet, no hace falta una transformación adicional, pero sí registrar los datos en el catálogo ejecutando una sentencia DDL en Athena (o Glue) como:

     ```bash
     aws athena start-query-execution --query-string "CREATE EXTERNAL TABLE IF NOT EXISTS wind_data_meteo (lat DOUBLE, lon DOUBLE, dir DOUBLE, vel DOUBLE, tiempo TIMESTAMP) PARTITIONED BY (anio INT, mes INT) STORED AS PARQUET LOCATION 's3://<bucket>/datos_wrf/'" --result-configuration OutputLocation=s3://<bucket-log>/
     ```

     Lo anterior es un ejemplo simplificado: en la práctica se debe ajustar el esquema exactamente y luego ejecutar `MSCK REPAIR TABLE` si se usan particiones. Este registro en el catálogo es importante para que Athena conozca dónde están los datos y qué columnas existen.

5. **Consulta de los datos con Athena:** Con los datos disponibles en S3 y la tabla registrada en Glue/Athena, el usuario puede, desde la CLI o cualquier herramienta compatible, lanzar consultas SQL. Por ejemplo, usando la CLI de Athena:

   ```bash
   aws athena start-query-execution --query-string "SELECT avg(vel) AS vel_promedio, direccion_viento FROM wind_data_meteo WHERE anio=2015 GROUP BY direccion_viento;" --result-configuration OutputLocation=s3://<bucket-temp>/
   ```

   Este comando calculatorio (ejemplo) devolvería la velocidad media del viento para 2015 por dirección, y guardará los resultados en un bucket S3 de salida. Athena se conecta al dataset en Parquet sin necesidad de cargar los enormes NetCDF originales, logrando tiempos de respuesta mucho menores. El usuario podría igualmente conectarse con Amazon QuickSight u otra herramienta BI para visualizar resultados, ya que Athena expone los datos mediante SQL estándar.

6. **Terminación y limpieza:** Dado que se trata de una **ejecución puntual**, una vez obtenidos los resultados, se pueden deshabilitar o eliminar los recursos creados. Las funciones Lambda y la máquina de Step Functions se pueden borrar vía CLI (`aws lambda delete-function`, `aws stepfunctions delete-state-machine`) si no se planea reutilizarlas. Los archivos NetCDF descargados solo residieron en almacenamiento efímero (o en S3 temporal si así se decidió), por lo que no hay necesidad de conservarlos una vez convertidos. Los datos Parquet finales pueden mantenerse en S3 para futuras consultas hasta que el usuario decida borrarlos.

## Detalles de Implementación y Ejemplos CLI

A fin de garantizar la **ejecutabilidad por CLI**, todos los componentes y pasos mencionados pueden gestionarse mediante comandos AWS CLI y scripts. A continuación se listan consideraciones de implementación y ejemplos concretos:

* **Despliegue de la función Lambda de procesamiento:** Se implementará en Python e incluirá las dependencias necesarias (xarray, netCDF4, pandas, etc.). Dado que algunas librerías como netCDF4 dependen de bibliotecas nativas (HDF5), una estrategia es empaquetar la función como una *imagen de contenedor Lambda*. Por ejemplo, se puede construir una imagen Docker basada en `amazonlinux:2` con Python 3.8, instalando `xarray` y `netcdf4` via pip, y luego subirla al Amazon ECR. El despliegue vía CLI sería:

  ```bash
  aws lambda create-function --function-name ProcesaNetCDF \
      --package-type Image \
      --code ImageUri=<account>.dkr.ecr.eu-west-1.amazonaws.com/lambda-netcdf:latest \
      --role <execution-role-arn> \
      --memory-size 3000 --timeout 900
  ```

  (Ajustando la memoria y timeout según el tamaño de archivos a procesar; 900 seg = 15 min es el máx. permitido). Esta Lambda utilizará `event` de entrada con la URL o info del archivo a procesar.

* **Definición de la Step Function (State Machine):** Se puede crear usando AWS CLI proporcionando una definición en JSON o Amazon States Language. Un ejemplo simplificado de definición en pseudocódigo:

  ```json
  {
    "StartAt": "GenerarLista",
    "States": {
      "GenerarLista": {
        "Type": "Task",
        "Resource": "arn:aws:lambda:...:function:GenerarLista",
        "Next": "ProcesarArchivos"
      },
      "ProcesarArchivos": {
        "Type": "Map",
        "ItemsPath": "$.lista_urls",
        "Iterator": {
          "StartAt": "ProcesarUno",
          "States": {
            "ProcesarUno": {
              "Type": "Task",
              "Resource": "arn:aws:lambda:...:function:ProcesaNetCDF",
              "End": true
            }
          }
        }
      }
    }
  }
  ```

  En este esquema, *GenerarLista* produce un array `lista_urls` en la salida, que alimenta el estado *Map* llamado *ProcesarArchivos*. Este lanza *ProcesaNetCDF* para cada URL en paralelo y finaliza cuando todas las ejecuciones terminan. La creación de esta máquina se haría con un comando CLI como:

  ```bash
  aws stepfunctions create-state-machine --name "DescargaProcesaNetCDF" \
     --definition file://state-machine.json --role-arn <StepFunctionsRoleARN>
  ```

  Donde `state-machine.json` contiene la definición JSON.

* **Manejo de errores y reintentos:** Step Functions permite configurar reintentos automáticos en los estados Task. Por ejemplo, si una descarga falla (posible si la conexión se corta), la Lambda *ProcesaNetCDF* puede lanzar una excepción; Step Functions podría reintentar esa tarea hasta N veces. Esto añade robustez frente a errores transitorios. Adicionalmente, las Lambdas deberían validar que los datos extraídos tienen el formato esperado (por ejemplo, verificar que existen las variables `lat`, `lon`, `dir`, `mod`). En caso de que en algún archivo falte la variable `dir` o `mod`, la Lambda podría calcularlas a partir de `u` y `v` antes de continuar.

* **Consideraciones de rendimiento:** La descarga desde THREDDS y procesamiento con Python en Lambdas es adecuado para una **ejecución puntual** y un volumen moderado de archivos. Gracias a la paralelización, si se solicitan, por ejemplo, 5 años de datos (\~5\*365 ≈ 1825 archivos, con \~2 corridas por día serían \~3650 archivos), Step Functions podrá lanzar cientos de Lambdas concurrentes (limitado por las cuotas de concurrencia de la cuenta). Esto acelerará significativamente el proceso en comparación a hacerlo secuencialmente en una sola instancia EC2. Cada Lambda maneja un archivo de \~50-100 MB y produce Parquet más compacto; el tráfico de salida desde THREDDS puede ser el factor más lento. Se recomienda filtrar en la Lambda solo las variables necesarias antes de convertir a Parquet para reducir uso de memoria y almacenamiento.

* **Seguridad y costos:** Todos los componentes se ejecutan en la cuenta AWS del usuario. Se debe asignar un rol IAM a las Lambdas con permisos para escribir en S3 y para invocar Glue (si corresponde). Los datos en S3 pueden cifrarse por defecto con SSE-S3 o una CMK de KMS. En cuanto a costos, esta arquitectura es **costo-efectiva** para procesamiento por lotes: se paga por uso de Lambda (por milisegundos), Step Functions (por estado ejecutado) y las consultas Athena. No hay infraestructura permanente corriendo. Estudios de AWS han demostrado que este tipo de pipeline escalable y serverless es económico y eficiente para volúmenes grandes de datos meteorológicos.

## Justificación Técnica de la Arquitectura

La arquitectura propuesta combina servicios gestionados de AWS para lograr un **pipeline de ETL (extracción-transformación-carga) completamente automatizado** y ejecutable vía CLI. A continuación, se resumen las razones de diseño y ventajas clave:

* **Simplicidad y Automatización:** Usando Step Functions y Lambda, evitamos trabajo manual o intervenciones en consola gráfica. Todo se define como código (IaC) y puede iniciarse con un solo comando CLI. Esto cumple con el requisito de no usar interfaces gráficas y facilita reproducir la ejecución en cualquier momento. La orquestación con Step Functions permite encapsular la lógica de bucle/iteración sobre fechas dentro de AWS, en lugar de depender de un script externo corriendo en local.

* **Escalabilidad y Paralelismo:** La utilización de Step Functions con un estado *Map* y Lambdas paralelas significa que la solución escala casi linealmente con el número de archivos a procesar. Si el rango de años es grande (p. ej. décadas de datos), esta arquitectura puede procesar múltiples archivos simultáneamente, acortando drásticamente el tiempo total. AWS Lambda maneja la infraestructura subyacente, permitiendo lanzar cientos de funciones en paralelo sin que el usuario gestione clústeres ni colas. En contraste, una solución monolítica en EC2 tendría que procesar secuencialmente o implementar su propio multi-threading, siendo más compleja de manejar.

* **Uso de herramientas especializadas (open-source):** Al apoyarnos en librerías como **xarray** o **netCDF4** dentro de las Lambdas, aprovechamos herramientas probadas para manejo de NetCDF. Xarray simplifica la selección de variables y conversión a estructuras pandas, lo que encaja perfectamente con el objetivo de llevar datos a formato Parquet tabular. Estas bibliotecas abiertas evitan tener que escribir parsers manuales de NetCDF, reduciendo errores y tiempo de desarrollo. Cabe señalar que es necesario incluir dichas librerías en la capa o imagen de Lambda, pero una vez hecho, el desarrollo del código es relativamente conciso gracias a la expressividad de estas herramientas.

* **Formato Parquet y Athena:** Convertir los datos a Parquet ofrece dos grandes beneficios: reducción de espacio y optimización para consultas analíticas. Parquet es un formato columnar comprimido; almacenar solo las columnas de interés (lat, lon, dir, vel, tiempo) ahorra mucho espacio comparado con los NetCDF originales que contienen decenas de variables. Además, Athena lee Parquet de forma muy eficiente, escaneando solo las columnas necesarias para cada consulta. Esto habilita análisis interactivos (o integraciones con AWS Athena y Amazon QuickSight) sobre años de datos de viento con tiempos de respuesta manejables, algo inviable directamente sobre cientos de archivos NetCDF dispersos. El resultado es un pequeño **Data Lake** especializado en S3 que responde a SQL.

* **Flexibilidad para extensiones:** Aunque la solución está pensada para ejecuciones puntuales, su arquitectura es reutilizable. Si mañana se quisiera automatizar descargas diarias de nuevos pronósticos, se podría adaptar la misma estructura (por ejemplo, triggereando la Step Function con un evento programado que añade el archivo del día). Asimismo, si se necesitaran más variables (ej. temperatura, precipitación), se pueden incluir en el procesamiento simplemente agregándolas en el xarray/pandas antes de generar el Parquet. La arquitectura admite modificaciones sin cambiar su esencia.

* **Opción alternativa con EC2 (consideraciones):** Se contempló usar una instancia EC2 para hacer todo el procesamiento en un solo lugar, controlada por CLI (por ejemplo iniciándola con *user data* que ejecute un script al arranque, y terminándola al concluir). Si bien esto simplificaría la arquitectura en número de componentes, implica mantener un servidor (actualizaciones, parcheo, etc.) y dimensionarlo para el peor caso de carga. Dado que AWS Lambda y Step Functions proveen un entorno administrado y escalable automáticamente, la solución serverless es preferible para un caso one-shot como este. Solo se recomendaría EC2 si, por ejemplo, las dependencias no encajan en Lambda (lo cual no es el caso, ya que Lambda soporta contenedores con casi cualquier librería). Priorizar la simplicidad no necesariamente significa **menos componentes**, sino menos intervención humana; en ese sentido, la combinación Step Functions + Lambda es sumamente simple de ejecutar (una sola llamada CLI dispara todo), a la vez que descompone el problema en partes manejables y con mínima supervisión.

En resumen, la arquitectura propuesta aprovecha servicios AWS integrados para lograr una **descarga masiva automatizada y procesamiento eficiente** de datos NetCDF, cumpliendo con los requisitos del usuario. La solución recorre dinámicamente el catálogo THREDDS para formar URLs de descarga, procesa cada archivo con herramientas científicas especializadas, y almacena los resultados en un data lake interno listo para análisis con Athena. Todo el pipeline puede desplegarse y ejecutarse mediante CLI, asegurando reproducibilidad y evitándose pasos manuales. Esta aproximación ha sido destacada por AWS como escalable y rentable para grandes volúmenes de datos meteorológicos, y proporciona a MeteoGalicia (o al usuario final) un mecanismo poderoso para explotar sus datos de viento históricos con la flexibilidad de la nube.

**Fuentes:** La estrategia de iterar fechas para construir URLs de THREDDS está basada en las recomendaciones del manual de MeteoGalicia. El uso de AWS Step Functions con Lambdas paralelas para datos meteorológicos sigue patrones validados en arquitecturas serverless de AWS. La conversión a Parquet y consulta mediante Athena se apoya en las capacidades nativas de AWS y librerías Python de análisis de datos. Estas referencias respaldan la robustez técnica de la solución aquí presentada.
