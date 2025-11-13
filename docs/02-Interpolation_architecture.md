Gracias. Investigaré cómo implementar los métodos de interpolación (IDW y Regression-Kriging) descritos en tu documento, sobre la base de datos ya cargada en AWS, utilizando exclusivamente la CLI de AWS. Incluiré el cálculo de métricas RSR y BIAS, guardaré los resultados en formato Parquet accesible desde Athena, y consideraré cómo aumentar la resolución de la malla espacial original.

Te avisaré en cuanto tenga los scripts y la arquitectura listos para revisión.


# Solución Propuesta: Interpolación Espacial con IDW y Regression-Kriging

## Arquitectura General de la Solución

La solución se basa en un flujo 100% automatizado mediante la CLI de AWS, integrando varios servicios cloud. En términos generales, el proceso sigue estos pasos:

* **Datos de entrada:** Los datos meteorológicos (velocidad y dirección del viento) ya están almacenados en **Amazon S3** en formato Parquet y accesibles a través de **Amazon Athena**. Esto permite consultas eficientes sin mover grandes volúmenes de datos.
* **Cómputo en contenedor:** Se implementarán los algoritmos de interpolación IDW y Regression-Kriging en un script (Python o R) dentro de un contenedor Docker. Este contenedor se ejecutará en AWS de forma serverless usando **AWS ECS (Fargate)** o **AWS Batch**, o alternativamente en una función **AWS Lambda** con imagen de contenedor (si el volumen de datos lo permite). Amazon ECS permite lanzar tareas en contenedores para procesar datos y almacenar los resultados en S3. No se usará ninguna interfaz gráfica; todo se orquesta con comandos de la CLI.
* **Salida y almacenamiento:** Los resultados de las interpolaciones (mallas de viento a alta resolución) se guardarán como archivos Parquet en S3. Estos archivos se organizan en una estructura particionada (por ejemplo, por fecha o región) para facilitar consultas posteriores desde Athena. También se calcularán métricas de ajuste (RSR y Bias) durante el proceso y se registrarán para evaluar la calidad de las interpolaciones.

Esta arquitectura permite escalabilidad y reproducibilidad: los datos permanecen centralizados en S3/Athena, el cómputo se aisla en contenedores que pueden ejecutarse on-demand, y los resultados se almacenan nuevamente en el data lake (S3) listos para ser analizados con Athena u otras herramientas.

## Preparación de los Datos y Entorno vía CLI

Antes de ejecutar las interpolaciones, se preparan los datos de entrada y el entorno de cómputo mediante CLI:

* **Consulta de datos con Athena:** Si es necesario filtrar o preprocesar los datos meteorológicos, se puede utilizar Athena vía CLI. Por ejemplo, para extraer datos de cierto rango de fechas o área geográfica a un archivo intermedio en S3:

  ```bash
  aws athena start-query-execution \
    --query-string "SELECT * FROM mydb.wind_data WHERE fecha='2025-05-16'" \
    --result-configuration OutputLocation=s3://mi-bucket/consulta-previa/
  ```

  Este comando ejecuta una query en Athena (usando la base de datos y tabla registradas en Glue Data Catalog) y guarda los resultados en S3 en formato CSV/Parquet. (Athena por defecto genera resultados en CSV, pero podemos crear tablas Parquet con CTAS si fuera necesario). También podemos omitir este paso y leer los datos Parquet directamente desde S3 en el script de interpolación, ya que Athena ya tiene los datos en S3.

* **Generación de la malla de alta resolución:** A partir de los datos originales (por ejemplo, coordenadas de estaciones o centroides de celdas de malla gruesa), se define la malla de destino más fina. Esto puede hacerse dinámicamente en el script de cómputo. Por ejemplo, si la malla original tiene resolución de 0.1° en latitud/longitud, la nueva malla podría ser de 0.05° (el doble de resolución). El script calculará el mínimo y máximo de latitud y longitud de los puntos originales y generará una **grilla** de puntos interpolados con el incremento deseado. En pseudocódigo:

  ```python
  # Suponiendo que tenemos lat_min, lat_max, lon_min, lon_max de los datos originales
  import numpy as np
  res = 0.05  # resolución deseada en grados
  grid_lats = np.arange(lat_min, lat_max+res, res)
  grid_lons = np.arange(lon_min, lon_max+res, res)
  # Generar todas las combinaciones de lat, lon
  grid_points = [(lat, lon) for lat in grid_lats for lon in grid_lons]
  ```

  De esta forma obtenemos la malla de puntos donde estimaremos las variables de viento a alta resolución. Este conjunto de puntos no estaba en los datos originales y por tanto requiere interpolación.

* **Configuración del entorno de cómputo:** Se prepara un contenedor Docker que contendrá todas las dependencias necesarias (librerías de Python o R, AWS SDK, etc.) y los scripts de interpolación. A continuación se detalla la construcción del contenedor y la preparación para su ejecución.

## Implementación del Método IDW (Inverse Distance Weighting)

**Inverse Distance Weighting (IDW)** es un método determinístico de interpolación espacial que asigna valores a ubicaciones desconocidas haciendo un promedio ponderado de los valores en puntos cercanos conocidos, donde los pesos son inversamente proporcionales a la distancia. En otras palabras, los puntos de medición más cercanos al punto interpolado tienen mayor influencia en el valor estimado que los puntos más lejanos.

Para nuestro caso (variables de viento), aplicaremos IDW por separado a la velocidad y a la dirección del viento:

* **IDW para velocidad del viento:** Tomamos las ubicaciones de entrada (por ejemplo, estaciones meteorológicas o celdas originales) con su valor de velocidad. Para cada punto de la malla fina, calculamos la distancia a todas las estaciones conocidas; luego calculamos un promedio ponderado de las velocidades conocidas usando un peso \$w\_i = 1/d\_i^p\$, donde \$d\_i\$ es la distancia a la estación *i* y *p* es un factor de potencia (típicamente \$p=2\$). Se suele aplicar una normalización de los pesos para que sumen 1. Si un punto de la malla cae exactamente en la ubicación de una estación conocida (distancia cero), se le asigna exactamente el valor observado en esa estación para evitar divisiones por cero.

* **IDW para dirección del viento:** La dirección es una variable circular (0-360°); hacer promedio directo puede ser problemático debido al cambio de 359° a 0°. Una solución práctica es convertir la dirección a componentes vectoriales (u, v) antes de la interpolación. Por ejemplo, \$u = V \* \sin(\theta)\$ y \$v = V \* \cos(\theta)\$, donde \$V\$ es la velocidad y \$\theta\$ la dirección en radianes. Se pueden interpolar u y v con IDW separadamente y luego recomponer la dirección interpolada = \$\arctan2(u, v)\$ y velocidad interpolada = \$\sqrt{u^2+v^2}\$. Sin embargo, dado que aquí nos piden interpolar *por separado* dirección y velocidad, podríamos interpolar la dirección asumiendo que las diferencias son pequeñas; si fuera necesario, se puede aplicar la conversión a componentes para mayor robustez.

A continuación, se ilustra una posible implementación de IDW en Python para una variable escalar (por ejemplo, velocidad), usando arrays de Numpy para eficiencia:

```python
import numpy as np

def idw_interpolate(x_obs, y_obs, vals_obs, x_targets, y_targets, power=2):
    """
    Interpolación IDW simple.
    x_obs, y_obs: arrays de coordenadas conocidas (por ejemplo, longitudes y latitudes de estaciones).
    vals_obs: valores conocidos en esas coordenadas (p.ej. velocidades).
    x_targets, y_targets: coordenadas de los puntos objetivo (malla fina).
    power: exponente de la distancia para ponderación.
    """
    vals_interp = []
    for (xt, yt) in zip(x_targets, y_targets):
        # Calcular distancias de (xt, yt) a todos los puntos conocidos
        dist = np.sqrt((x_obs - xt)**2 + (y_obs - yt)**2)
        # Si existe distancia cero (mismo punto), usar valor conocido directamente
        if np.any(dist == 0):
            vals_interp.append(vals_obs[dist.argmin()])  # toma el valor del punto coincidente
        else:
            # Calcular pesos como 1/dist^power
            w = dist**(-power)
            w = w / np.sum(w)
            # Valor interpolado = promedio ponderado
            vals_interp.append(np.sum(w * vals_obs))
    return np.array(vals_interp)
```

En este código, iteramos sobre cada punto de la malla y calculamos el peso relativo de cada estación. Para optimización, se podría limitar la interpolación a un número fijo de vecinos más cercanos en lugar de usar **todos** los puntos (ya que puntos muy lejanos aportan peso casi nulo). También podría vectorizarse más el cálculo usando operaciones de matrices Numpy para evitar bucles en Python, dependiendo del tamaño de los datos.

**Nota:** Si se prefiere, en R existe la función `idw` del paquete **gstat** que realiza esta interpolación directamente, lo que sería equivalente a lo anterior pero aprovechando código optimizado en C++.

## Implementación del Método Regression-Kriging

**Regression-Kriging (RK)** es un método de interpolación geoestadística que combina dos enfoques: primero ajusta un modelo de regresión global utilizando variables auxiliares, y luego aplica kriging (interpolación estadística) a los residuales de esa regresión. En términos simples, RK aprovecha tendencias explicables en los datos (por ejemplo, dependencia de la altitud, latitud, etc.) mediante regresión, y deja que el kriging capture los patrones espaciales restantes no explicados (correlación espacial de los residuales). Este método es matemáticamente equivalente al **kriging universal o kriging con deriva externa**.

En nuestro caso, podríamos no tener muchas variables auxiliares aparte de la ubicación (latitud/longitud) y quizás la altitud del sitio o alguna variable meteorológica grande escala. Una estrategia común es usar las coordenadas como predictores en la regresión (lo que captura un plano inclinable, es decir, una tendencia global norte-sur o este-oeste) y luego krigear los residuales para añadir la variación local.

Pasos para aplicar Regression-Kriging a, por ejemplo, la velocidad del viento:

1. **Ajuste de la regresión (tendencia global):** Usamos un modelo de regresión lineal múltiple donde la variable dependiente es la velocidad del viento y las independientes podrían ser la latitud, longitud y otras variables auxiliares disponibles. Por ejemplo, un modelo simple:
   $V(\text{viento}) = \beta_0 + \beta_1 \cdot \text{lat} + \beta_2 \cdot \text{lon} + \epsilon$
   donde \$\epsilon\$ es el residuo. Este modelo daría una estimación inicial \$\hat{V}\_{\text{reg}}(x,y)\$ para cualquier coordenada \$(x,y)\$. Si hubiera más variables (por ejemplo, elevación del terreno, rugosidad, etc.), podrían incluirse para mejorar la regresión.

2. **Cálculo de residuales:** Para cada punto conocido (estación o celda original) calculamos el residual = valor observado \$-\$ valor predicho por la regresión \$\epsilon\_i = V\_{\text{obs},i} - \hat{V}\_{\text{reg},i}\$. Idealmente, estos residuales contienen la estructura espacial que la regresión no explicó.

3. **Kriging de residuales:** Se aplica un kriging ordinario sobre los residuales \$\epsilon\_i\$ en las ubicaciones conocidas. El kriging es un interpolador geoestadístico que asume correlación espacial: básicamente estima el valor en un punto no muestreado como una combinación ponderada de los residuales conocidos, donde los pesos se derivan de un modelo de variograma (función que describe la dependencia espacial de la variable). Para ejecutar kriging necesitamos elegir un modelo de variograma (ej.: esférico, exponencial, etc.) y ajustarlo a los datos de residuo. Muchas librerías (como **PyKrige** en Python o **gstat** en R) pueden estimar automáticamente un variograma a partir de los datos.

4. **Predicción final:** La estimación por Regression-Kriging en un punto de la malla es:
   $\text{Predicción RK} = \underbrace{\hat{V}_{\text{reg}}(x,y)}_{\text{tendencia regresión}} + \underbrace{\hat{\epsilon}_{\text{kriging}}(x,y)}_{\text{residuo interpolado}}.$
   Es decir, sumamos la tendencia global predicha por el modelo de regresión y la corrección local proveniente del kriging de residuales.

En código Python, podríamos implementar esto usando bibliotecas como **scikit-learn** para la regresión y **PyKrige** para el kriging. Por ejemplo:

```python
import numpy as np
from sklearn.linear_model import LinearRegression
from pykrige.ok import OrdinaryKriging

# Datos conocidos (coordenadas y valor observado)
X_coords = np.column_stack((lat_obs, lon_obs))  # matriz Nx2 con latitud y longitud
y_vals = wind_speed_obs  # valores de velocidad observados

# 1. Ajustar modelo de regresión lineal
reg = LinearRegression().fit(X_coords, y_vals)
trend_pred_obs = reg.predict(X_coords)  # predicción en puntos conocidos
residuals = y_vals - trend_pred_obs     # residuales en puntos conocidos

# 2. Ajustar kriging ordinario a los residuales
OK = OrdinaryKriging(lat_obs, lon_obs, residuals, variogram_model='exponential',
                     verbose=False, enable_plotting=False)
# (PyKrige estima automáticamente los parámetros del variograma si no se le proporcionan)

# 3. Aplicar la regresión + kriging a la malla fina
X_grid = np.column_stack((lat_grid, lon_grid))       # coordenadas de malla
trend_pred_grid = reg.predict(X_grid)                # tendencia por regresión en malla
residual_kriged, ss = OK.execute('points', lat_grid, lon_grid)  # kriging de residuales
residual_kriged = np.array(residual_kriged)

# 4. Sumar para obtener predicción final RK en la malla
wind_speed_rk = trend_pred_grid + residual_kriged
```

En este ejemplo:

* Usamos una regresión lineal simple en lat/lon. En un caso real, podríamos mejorar el modelo (por ejemplo incluir términos cuadráticos o variables adicionales si existen).
* `OrdinaryKriging` de PyKrige se emplea para modelar y predecir los residuales. Aquí hemos usado un variograma exponencial por defecto; PyKrige permite ajustar automáticamente los parámetros (sill, range, nugget) al proporcionar los datos de entrenamiento.
* `OK.execute('points', lat_grid, lon_grid)` devuelve la estimación krigeada de los residuales para cada punto de la malla (`residual_kriged`) y opcionalmente la varianza de predicción (`ss` que podríamos ignorar o usar para incertidumbre).
* Finalmente sumamos la tendencia y el residuo interpolado para cada punto de la malla (`wind_speed_rk`).

Se repetiría un proceso similar para la **dirección del viento**. Para la dirección, podríamos aplicar Regression-Kriging directamente sobre los valores angulares si la variabilidad no cruza el 0°/360°, o nuevamente trabajar en componentes vectoriales. Por simplicidad, supongamos que tratamos la dirección como variable aproximadamente continua (teniendo cuidado con la interpretación de errores).

**Nota:** En R, una implementación análoga sería usar `gstat::krige()` con la opción de fórmula de regresión (drift externo). Por ejemplo, `krige(formula = wind ~ lat + lon, data=datos, model=variograma, newdata=malla)` realizaría internamente un universal kriging equivalente al regression-kriging descrito.

## Cálculo de Métricas de Evaluación (RSR y Bias)

Para evaluar la calidad del ajuste de los métodos de interpolación, calculamos dos métricas: **RSR** y **Bias**.

* **RSR (RMSE-Standard Deviation Ratio):** Es la razón entre la raíz del error cuadrático medio (RMSE) de las predicciones y la desviación estándar de los valores observados. Normaliza el RMSE para que sea adimensional y comparable: un RSR más bajo indica mejor desempeño (0 sería perfecto). Por ejemplo, según Moriasi et al. (2007), un RSR < 0.5 se considera *muy bueno*, 0.5-0.6 *bueno*, 0.6-0.7 *satisfactorio*, >0.7 *insatisfactorio*.

* **Bias (Error Medio o Sesgo):** Podemos definirlo como la diferencia promedio entre valores predichos y observados. En particular, el **Mean Bias Error (MBE)** se calcula como el promedio de (predicción \$-\$ observado) para todos los puntos. Si el sesgo es cercano a 0, significa que no hay tendencia sistemática a sobrestimar o subestimar; un bias positivo indica sobreestimación promedio y negativo subestimación.

Para calcular estas métricas en nuestro contexto de interpolación, debemos comparar las predicciones del modelo contra datos "verdaderos". Dado que estamos interpolando puntos sin observación directa, la evaluación común se hace mediante **validación cruzada** con los puntos originales. Un enfoque típico es la validación *leave-one-out* (LOO):

* Para cada punto de medición original, se oculta su valor y se predice usando los demás puntos como entrada del interpolador. Se repite para todos los puntos, obteniendo así un conjunto de predicciones interpoladas en ubicaciones donde sí conocemos el valor real (pero que no se usaron para esa predicción).
* Con este conjunto de predicciones vs observaciones podemos calcular RMSE, bias, RSR, etc.

Otra opción es separar aleatoriamente un porcentaje de estaciones como conjunto de validación, interpolar sin ellas y comparar. En cualquier caso, el script de interpolación puede realizar este procedimiento automáticamente y al final imprimir o guardar las métricas calculadas.

Ejemplo de cálculo de métricas en Python, continuando el flujo anterior:

```python
from math import sqrt

# Supongamos que tenemos listas/arrays: obs_vals (observados) y pred_vals (predichos por el modelo en validación cruzada)
def calcular_metricas(obs_vals, pred_vals):
    # Calcular residuo en cada punto
    errors = pred_vals - obs_vals
    # Bias = promedio de error
    bias = errors.mean()
    # RMSE = raiz del error cuadrático medio
    rmse = sqrt(np.mean(errors**2))
    # RSR = RMSE / std(obs)
    rsr = rmse / np.std(obs_vals)
    return bias, rmse, rsr

bias_idw, rmse_idw, rsr_idw = calcular_metricas(obs_vals, pred_vals_idw)
bias_rk, rmse_rk, rsr_rk = calcular_metricas(obs_vals, pred_vals_rk)
print(f"IDW -> RMSE: {rmse_idw:.3f}, RSR: {rsr_idw:.3f}, Bias: {bias_idw:.3f}")
print(f"RK  -> RMSE: {rmse_rk:.3f}, RSR: {rsr_rk:.3f}, Bias: {bias_rk:.3f}")
```

En este fragmento se asume que `pred_vals_idw` y `pred_vals_rk` son arrays de las predicciones hechas en validación para cada punto observado correspondiente en `obs_vals`. Los resultados se podrían guardar en un archivo de log o en S3 (por ejemplo, un pequeño CSV/JSON con las métricas).

Estas métricas ayudan a comparar objetivamente el desempeño de IDW vs Regression-Kriging. Por ejemplo, un RSR más bajo y Bias más cercano a 0 en el método RK indicaría que la combinación de regresión + kriging está capturando mejor el comportamiento del viento que IDW, o viceversa.

## Contenedor Docker para la Ejecución

Para asegurar reproducibilidad y facilitar la ejecución en AWS (ya sea en ECS Fargate, Batch o Lambda), empacaremos toda la lógica en un contenedor Docker. A continuación se muestra un ejemplo de **Dockerfile** que prepara el entorno usando Python:

```Dockerfile
# Usar una imagen base de Python 3.10 slim (ligera)
FROM python:3.10-slim

# Instalamos las dependencias necesarias
RUN pip install --no-cache-dir boto3 pandas pyarrow numpy scikit-learn pykrige

# Copiar los scripts al contenedor
WORKDIR /app
COPY interpolacion.py ./interpolacion.py

# Comando de entrada por defecto al ejecutar el contenedor
ENTRYPOINT ["python", "/app/interpolacion.py"]
```

**Explicación:**

* Usamos una imagen oficial de Python. Si usáramos R, podríamos optar por una imagen de Rocker (R) e instalar paquetes como `gstat`, `sf`, etc., pero en este ejemplo seguimos con Python.
* Instalamos paquetes: `boto3` (para acceso a S3/Athena si se requiere dentro del script), `pandas` y `pyarrow` (para manejar dataframes y Parquet), `numpy`, `scikit-learn` (para regresión) y `pykrige` (para kriging).
* Se copia el script `interpolacion.py` al contenedor y se define que al ejecutar el contenedor, corra el script automáticamente. Este script incluirá la lógica de leer datos, realizar IDW y RK, calcular métricas y guardar resultados.

Un posible **script Python (`interpolacion.py`)** resumido sería:

```python
import os, boto3, pyarrow.parquet as pq
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from pykrige.ok import OrdinaryKriging

# 1. Leer datos de viento desde S3 (Athena)
s3 = boto3.client('s3')
bucket = "mi-bucket-datos"
key = "datos/viento.parquet"  # ruta en S3 de los datos de entrada
obj = s3.get_object(Bucket=bucket, Key=key)
df = pd.read_parquet(obj['Body'])  # cargar parquet en DataFrame

# Suponiendo que df tiene columnas: lat, lon, wind_speed, wind_dir
lat_obs = df['lat'].values
lon_obs = df['lon'].values
speed_obs = df['wind_speed'].values
dir_obs = df['wind_dir'].values

# 2. Generar malla de alta resolución
lat_min, lat_max = lat_obs.min(), lat_obs.max()
lon_min, lon_max = lon_obs.min(), lon_obs.max()
res = 0.05  # por ejemplo
lat_grid = np.arange(lat_min, lat_max+res, res)
lon_grid = np.arange(lon_min, lon_max+res, res)
grid_lat_coords, grid_lon_coords = np.meshgrid(lat_grid, lon_grid)
grid_lat_flat = grid_lat_coords.flatten()
grid_lon_flat = grid_lon_coords.flatten()

# 3. Interpolación IDW para velocidad y dirección
speed_idw = idw_interpolate(lat_obs, lon_obs, speed_obs, grid_lat_flat, grid_lon_flat, power=2)
dir_idw   = idw_interpolate(lat_obs, lon_obs, dir_obs, grid_lat_flat, grid_lon_flat, power=2)

# 4. Interpolación Regression-Kriging para velocidad (ejemplo similar para dirección)
# 4a. Regresión lineal en coords -> velocidad
X = np.column_stack((lat_obs, lon_obs))
reg = LinearRegression().fit(X, speed_obs)
trend_speed_obs = reg.predict(X)
residuals = speed_obs - trend_speed_obs

# 4b. Kriging de residuales
OK = OrdinaryKriging(lat_obs, lon_obs, residuals, variogram_model='spherical')
residuals_interp, _ = OK.execute('points', grid_lat_flat, grid_lon_flat)
residuals_interp = np.array(residuals_interp)

# 4c. Sumar tendencia predicha + residuo krigeado
trend_speed_grid = reg.predict(np.column_stack((grid_lat_flat, grid_lon_flat)))
speed_rk = trend_speed_grid + residuals_interp

# (Repetir 4a-4c para dirección del viento si aplica Regression-Kriging también a dirección)

# 5. Empaquetar resultados en DataFrame y guardar a Parquet
result_df = pd.DataFrame({
    "lat": grid_lat_flat,
    "lon": grid_lon_flat,
    "wind_speed_idw": speed_idw,
    "wind_speed_rk": speed_rk,
    "wind_dir_idw": dir_idw
    # "wind_dir_rk": dir_rk  (si implementamos RK para dirección)
})
# Añadir columnas de partición, p.ej. fecha/hora si corresponde
result_df["year"] = 2025; result_df["month"] = 5; result_df["day"] = 16

# Guardar a Parquet localmente
result_df.to_parquet("salida_interpolacion.parquet", index=False)

# Subir a S3 en ruta particionada
s3.upload_file("salida_interpolacion.parquet", Bucket="mi-bucket-datos", 
               Key="resultados_interpolados/year=2025/month=05/day=16/salida.parquet")

print("Interpolación completada y resultados guardados en S3.")
```

*El código anterior está simplificado para ilustrar los pasos principales.* En una versión completa, incluiríamos manejo de errores, argumentos (por ejemplo para fecha/resolución dinámicos), y cálculo de métricas RSR/Bias quizá en el mismo script. También, para grandes volúmenes de datos, se podrían procesar por partes (ej. por bloques de la malla) para no exceder memoria.

Notemos que guardamos los resultados en `s3://mi-bucket-datos/resultados_interpolados/year=2025/month=05/day=16/` siguiendo un esquema de particiones por fecha (año, mes, día). Esto permitirá definir una tabla externa en Athena sobre ese prefijo de S3 y filtrar fácilmente por fecha. Si quisiéramos particionar por otra dimensión (ej. variable, método, etc.), podríamos ajustar la ruta o agregar más columnas de partición.

## Ejecución Paso a Paso usando AWS CLI

A continuación se describen los pasos para implementar y ejecutar esta solución usando la línea de comandos de AWS, asumiendo que ya se tiene configurado AWS CLI con credenciales adecuadas:

1. **Construir y publicar la imagen de Docker:** En nuestro equipo local (o CI) construimos el contenedor y lo subimos al registro de contenedores de AWS (ECR):

   ```bash
   # Crear un repositorio ECR (una sola vez)
   aws ecr create-repository --repository-name wind-interpolation

   # Obtener credenciales de login para ECR y hacer login con Docker
   aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.eu-west-1.amazonaws.com

   # Construir la imagen Docker localmente
   docker build -t wind-interpolation:latest .
   # Etiquetar la imagen con la URL del repositorio ECR
   docker tag wind-interpolation:latest <aws_account_id>.dkr.ecr.eu-west-1.amazonaws.com/wind-interpolation:latest
   # Subir la imagen al registro ECR
   docker push <aws_account_id>.dkr.ecr.eu-west-1.amazonaws.com/wind-interpolation:latest
   ```

2. **Lanzar la tarea de cómputo en AWS:** Usaremos AWS ECS con Fargate para ejecutar el contenedor sin necesidad de gestionar servidores. Primero, necesitamos una definición de tarea ECS que use nuestra imagen. Podemos crear un fichero JSON `task-def.json` con los parámetros (nombre de imagen ECR, CPU/memoria, roles, etc.). Un ejemplo muy básico de definición (en JSON) podría ser:

   ```json
   {
     "family": "wind-interpolation-task",
     "networkMode": "awsvpc",
     "cpu": "1024",
     "memory": "2048",
     "requiresCompatibilities": [ "FARGATE" ],
     "executionRoleArn": "arn:aws:iam::...:role/ecsTaskExecutionRole",
     "containerDefinitions": [
       {
         "name": "windinterp",
         "image": "<aws_account_id>.dkr.ecr.eu-west-1.amazonaws.com/wind-interpolation:latest",
         "essential": true,
         "logConfiguration": { "logDriver": "awslogs", "options": { "awslogs-group": "/ecs/windinterp", "awslogs-region": "eu-west-1", "awslogs-stream-prefix": "ecs" } }
       }
     ]
   }
   ```

   Registramos esta definición de tarea con:

   ```bash
   aws ecs register-task-definition --cli-input-json file://task-def.json
   ```

   Luego, ejecutamos la tarea en Fargate:

   ```bash
   aws ecs run-task --cluster default \
       --launch-type FARGATE \
       --task-definition wind-interpolation-task:1 \
       --network-configuration "awsvpcConfiguration={subnets=[subnet-123],securityGroups=[sg-456],assignPublicIp=ENABLED}"
   ```

   En este comando, sustituya `subnet-123` y `sg-456` por el subnet ID y security group ID adecuados de su VPC. La tarea Fargate arrancará el contenedor y ejecutará el script de interpolación automáticamente (gracias al `ENTRYPOINT` definido). Podemos monitorear el progreso revisando los logs de CloudWatch (`/ecs/windinterp` en el ejemplo) o consultando el estado de la tarea:

   ```bash
   aws ecs describe-tasks --cluster default --tasks <task-id>
   ```

   *Nota:* Como alternativa a ECS Fargate, podríamos usar **AWS Batch** para trabajos periódicos o de gran volumen, definiendo un ambiente de cómputo y cola de trabajos, luego usando `aws batch submit-job` para lanzar la tarea. El contenedor es el mismo; Batch facilita la gestión si hubiera muchos trabajos o programación. Si el procesamiento fuera ligero, también se podría desplegar el contenedor como una función **AWS Lambda** (vía imagen de contenedor) y usar `aws lambda invoke` para ejecutarla. Sin embargo, dado que Kriging e IDW pueden ser intensivos en cálculo, ECS/Fargate o Batch son más apropiados para permitir mayor tiempo de ejecución y memoria.

3. **Almacenamiento y consulta de resultados:** Cuando la tarea termina, los resultados interpolados estarán en S3. Podemos verificar la presencia de los archivos Parquet generados:

   ```bash
   aws s3 ls s3://mi-bucket-datos/resultados_interpolados/year=2025/month=05/day=16/
   ```

   Deberíamos ver el archivo (o archivos) Parquet con las estimaciones. Para habilitar consultas desde Athena, necesitamos una tabla que apunte a este prefix de S3. Si ya tenemos un catálogo Glue, podemos crear la tabla particionada. Por ejemplo, mediante la CLI de Athena podemos ejecutar una consulta DDL como:

   ```bash
   aws athena start-query-execution --query-string "
     CREATE EXTERNAL TABLE IF NOT EXISTS mydb.wind_interpolated (
       lat DOUBLE, lon DOUBLE,
       wind_speed_idw DOUBLE, wind_speed_rk DOUBLE,
       wind_dir_idw DOUBLE
     )
     PARTITIONED BY (year INT, month INT, day INT)
     STORED AS PARQUET
     LOCATION 's3://mi-bucket-datos/resultados_interpolados/'" \
     --result-configuration OutputLocation=s3://mi-bucket-datos/athena-ddl/
   ```

   *(Agregar `wind_dir_rk` en las columnas si también se interpoló con RK).* Luego, agregar las particiones (por ejemplo usando `MSCK REPAIR TABLE mydb.wind_interpolated` o un `ALTER TABLE ADD PARTITION` específico):

   ```bash
   aws athena start-query-execution --query-string "MSCK REPAIR TABLE mydb.wind_interpolated" \
       --result-configuration OutputLocation=s3://mi-bucket-datos/athena-ddl/
   ```

   Ahora los datos están listos para ser consultados. Por ejemplo:

   ```bash
   aws athena start-query-execution --query-string \
       "SELECT lon, lat, wind_speed_idw, wind_speed_rk 
        FROM mydb.wind_interpolated 
        WHERE year=2025 AND month=5 AND day=16 
        LIMIT 10;" \
       --result-configuration OutputLocation=s3://mi-bucket-datos/athena-query-results/
   ```

   Los resultados de la consulta se guardarán en S3 (en la ruta especificada). Podemos usar `aws athena get-query-execution`/`get-query-results` para obtener el estado y los datos, o simplemente descargar el CSV resultante de S3.

4. **Automatización completa (opcional):** Todos los comandos anteriores pueden integrarse en un **script bash** para ejecutar el proceso de punta a punta. Por ejemplo, un script `run_interpolation.sh` podría:

   1. Ejecutar la consulta Athena (o no, si leemos directamente del parquet).
   2. Enviar la tarea Batch/ECS.
   3. Esperar a que termine (polling del estado de job/task).
   4. Reparar la tabla de Athena y/o notificar la disponibilidad de resultados.

   Esto permitiría lanzar todo el flujo con un solo comando, haciendo la solución reproducible y fácil de programar en el tiempo (por ej., vía cron, AWS Scheduler, etc.).

## Consideraciones Finales

La solución propuesta aprovecha los servicios administrados de AWS para manejar datos y cómputo sin interfaces gráficas, logrando así un proceso automatizado y escalable. Usar datos en formato Parquet y particiones optimiza costos y performance en Athena. La interpolación IDW proporciona un método rápido de estimar valores desconocidos mediante ponderación por distancia, mientras que Regression-Kriging aporta un enfoque más sofisticado al incorporar tendencias globales y correlación espacial de residuos. Al implementar ambos, podemos comparar sus resultados y métricas. Las métricas RSR y Bias calculadas permiten cuantificar el ajuste: RSR normaliza el error respecto a la desviación estándar observada y Bias muestra el sesgo medio de las predicciones.

En resumen, la arquitectura sería: **S3/Athena (datos) -> ECS/Lambda (cómputo en contenedor de IDW+RK) -> S3/Athena (resultados)**, todo orquestado por CLI. Esta solución de línea de comandos posibilita integrar el flujo en pipelines automatizados, garantizando reproducibilidad en la generación de mallas de viento más detalladas a partir de los datos originales.
