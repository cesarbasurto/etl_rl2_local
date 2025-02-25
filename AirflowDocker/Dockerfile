# Usamos la versión que quieras de Apache Airflow
FROM apache/airflow:2.10.4

# usuario 'airflow'
USER airflow
RUN pip install --upgrade pip
RUN pip install great-expectations==0.18.22
RUN pip install psycopg2-binary
RUN pip install pyyaml

# Cambiamos a root temporalmente para instalar paquetes
USER root

# Instalamos Java (OpenJDK) en el contenedor
RUN apt-get update && apt-get install -y openjdk-17-jre gdal-bin libgdal-dev python3-gdal && apt-get clean


# volvemos al usuario 'airflow'
USER airflow