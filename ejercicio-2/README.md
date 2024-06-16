# Ejercicio 2 - Pyspark (articles.csv)

##### Elaborado por: Sandra Palmer (sandra.palmer2000@gmail.com)
--
El proyecto tiene como objetivo resolver el Challenge-Ejercicio 2 de una manera en la que las funciones involucradas sean 100% reutilizables. Esto debido a que la lectura, transformacion y escritura de datos son funciones recurrentes para el área de datos. 
La solución presentada no solo es una solución expecifica del Challenge-Ejercicio2, si no que puede ser reutilizada para casos similares, e incluso con el desarrollo de mas funciones genéricas la solucion puede abarcar casos mas complejos, asi como lectura y escritura de diversas fuentes y formatos de datos. 


## Estructura del Proyecto
El proyecto se encuentra estructurado de la siguiente manera:

- /files: En esta carpeta se encuentra el archivo fuente input/articles.csv y los archivos resultantes de la ejecucion. La salida product_type/ corresponde al diccionario del paso 1y2 del ejercicio. La salida articles_final_output/ corresponde al join entre articles y product_type del paso 3.  
- /src: Corresponde al codigo fuente del challenge1 
    * main.py: archivo principal para la ejecucion, en este se establece el flujo general
    * /common/transform_service.py: Servicio principal para las llamadas a las funciones de transformacion de los datos
    * /common/frame_utils.py: Funciones genericas correspondientes a un dataframe de spark
    * /common/common_utils.py: Funciones genericas puras de python
- /data-config: Corresponde a las configuraciones especificas por cada archivo a transformar
    * product_type: Configuracion para lectura del archivo articles.csv, transformacion y escritura como diccionario
    * articles_final: Configuracion para lectura del archivo articles.csv y product_type, transformacion(join) de ambos archivos y escritura


## Requerimientos
    python => 3.0
    spark => 3.0



# Puntos de mejora al proyecto
- Logging de funciones para la trazabilidad del proceso
- Validacion del archivo de configuracion
- Validacion del archivo(s) fuente, antes de entrar a transformaciones
- Más funciones de transformacion para estandarización de datos
- Manejo de errores, severidad, excepciones y alertas


**Junio 2024**