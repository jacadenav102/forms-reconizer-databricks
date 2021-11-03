# Data Liquida : Contraloria General de la Republica

Proyecto para implementar la API Form Reconizer de Azure para la extracción de información de los 
documentos de la Contraloría General de la Republica.

## Descripción

Esta libraría esta constituida por una serie de herramientas que sirven para la extracción de información 
de los diversos dodocumentos que posee la Contraloria Generala de la Republica. No solamente se implementa
la API Forms Reconizer de Azure, sino también provee un conjunto de herramientas que ejecuten un proceso
automatico para le etracción de termino en forma de llave valor. También existen técnicas y procesos que 
permiten  la extraciión cruda del texto y refinarlo para mejorar el rendimiento de la extracción de la 
información.

La arquitectura objetivo de esta solución esta basada en la nube, en especifico sobre Azure Databricks. Por esta
razón, la mayoría de las transformaciones estan basado en el uso de Apache Spark y del lago de datos de GEN2 de 
Azure. Si bien es posible hacer uso de estas herramienta en una teminal loca, se recomienda el uso de la arquitectura
señalada. 

### Contenido

    - analyticsProcess: Contiene los procesos analítico donde se hace uso de los servicios de las API de Azure.
    - dataManangment: Contiene utilidades para el manejo de los datos de la solución. En paricular los documentso en formato PDF.
    - rawExtraction: Contiene los procesos de extración cruda del texto de los documentos ingresados.
    - resultValidation: Métodos de validación del proceso auto-supervizado de Azure Forms Reconizer.
    - weakSupervision: Métodos para el etiquetado de texto basado en supervisión debul y reglas euristicas. 
    - setDeltaLake: Métodos que facilitan la configuración de GEN2 y Databricks.


## Iniciando el Proyecto

### Dependencias

- pyspark
- pandas
- azure-ai-formrecognizer
- azure.ai.textanalytics
- six
- koalas
- numpy
- pandas
- spacy

### Instalación

pip install .\dataLiquida-0.1-py3-none-any.whl

## Autores

- Antonio Cadena: jcadena@igerencia.com
