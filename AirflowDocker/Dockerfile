# Usamos la versión que quieras de Apache Airflow
FROM apache/airflow:2.10.4

# usuario 'airflow'
USER airflow
RUN pip install --upgrade great-expectations==0.18.22 pip psycopg2-binary pyyaml

# Cambiamos a root temporalmente para instalar paquetes
USER root

# Instalamos Java (OpenJDK) en el contenedor
RUN apt-get update && \
    apt-get install -y \
        gdal-bin \
        libgdal-dev \
        openjdk-17-jre \
        python3-gdal && \
    apt-get clean
# volvemos al usuario 'airflow'
USER airflow