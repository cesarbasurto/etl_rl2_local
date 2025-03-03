import os
import json
import zipfile
import requests
import subprocess
import psycopg2
import shutil
from datetime import datetime
import sqlalchemy
import pandas as pd
import re
import yaml  # Requiere PyYAML para leer archivos YAML
import logging

# Cambiamos el nivel a INFO para que se vea en Airflow
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Módulos locales SQL
from etl_rl2_sql import (
    estructura_intermedia,
    transformacion_datos,
    validar_estructura,
    importar_al_modelo
)

# ------------------------- VARIABLES GLOBALES -------------------------
CONFIG_PATH = "/opt/airflow/etl/Config.json"
TEMP_FOLDER = "/opt/airflow/etl/temp"
ILI2DB_JAR_PATH = "/opt/airflow/etl/libs/ili2pg-5.1.0.jar"
MODEL_DIR = "/opt/airflow/etl/models/ladm_rl2/"
EPSG_SCRIPT = "/opt/airflow/etl/scripts/insert_ctm12_pg.sql"
GX_DIR = "/opt/airflow/dags/gx"



# ------------------------- FUNCIONES UTILITARIAS -------------------------

def leer_configuracion():
    """Lee la configuración desde Config.json."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
        logging.info("Configuración cargada correctamente.")
        return config
    except Exception as e:
        logging.error(f"Error leyendo la configuración: {e}")
        raise

def ejecutar_sql(sql, params=None):
    """Ejecuta un script SQL completo en la base de datos."""
    config = leer_configuracion()
    db_config = config["db"]
    logging.info(f"Ejecutando SQL:\n{sql[:300]}...")  # Muestra solo primeros 300 caracteres
    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["db_name"]
    )
    try:
        with conn.cursor() as cursor:
            if params:
                logging.info(f"Parámetros: {params}")
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
        conn.commit()
        logging.info("Script SQL ejecutado correctamente.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error ejecutando SQL: {e}")
        raise
    finally:
        conn.close()

def ejecutar_sql_archivo(archivo_sql):
    """Ejecuta un archivo SQL completo en la base de datos."""
    logging.info(f"Ejecutando SQL desde archivo: {archivo_sql}")
    if not os.path.exists(archivo_sql):
        raise FileNotFoundError(f"Archivo SQL no encontrado: {archivo_sql}")
    with open(archivo_sql, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    ejecutar_sql(sql_content)

def obtener_conexion_postgres():
    """Obtiene una conexión a la base de datos PostgreSQL."""
    logging.info("Obteniendo conexión a PostgreSQL...")
    config = leer_configuracion()
    db_config = config["db"]
    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["db_name"]
    )
    conn.autocommit = True
    return conn

def validar_conexion_postgres():
    """Valida la conexión a PostgreSQL, crea la base de datos si no existe e instala extensiones."""
    logging.info("Validando conexión a PostgreSQL...")
    config = leer_configuracion()
    db_config = config["db"]
    try:
        logging.info("Conectando a la base 'postgres' para verificar/arfw_etl_rl2'...")
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database="postgres"
        )
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'arfw_etl_rl2';")
            if not cursor.fetchone():
                logging.info("La base 'arfw_etl_rl2' no existe. Creándola...")
                cursor.execute("CREATE DATABASE arfw_etl_rl2;")
                logging.info("Base 'arfw_etl_rl2' creada exitosamente.")
            else:
                logging.info("La base 'arfw_etl_rl2' ya existe.")
        conn.close()

        logging.info("Conectando a 'arfw_etl_rl2' para instalar extensiones si no existen...")
        conn_arfw = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database="arfw_etl_rl2"
        )
        conn_arfw.autocommit = True
        with conn_arfw.cursor() as cursor_arfw:
            cursor_arfw.execute("SELECT current_database();")
            db_name = cursor_arfw.fetchone()[0]
            logging.info(f"Conexión exitosa a: {db_name}")
            cursor_arfw.execute("CREATE EXTENSION IF NOT EXISTS plpgsql;")
            cursor_arfw.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            cursor_arfw.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
            logging.info("Extensiones instaladas correctamente en 'arfw_etl_rl2'.")
        conn_arfw.close()
        return True
    except psycopg2.Error as e:
        logging.error(f"Error en la conexión: {e}")
        raise

def limpiar_carpeta_temporal():
    """Limpia la carpeta TEMP antes de cada ejecución."""
    logging.info("Limpiando carpeta temporal...")
    if os.path.exists(TEMP_FOLDER):
        for item in os.listdir(TEMP_FOLDER):
            item_path = os.path.join(TEMP_FOLDER, item)
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            except Exception as e:
                logging.error(f"Error eliminando {item_path}: {e}")
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    logging.info("Carpeta TEMP limpiada correctamente.")

def restablecer_esquema_insumos():
    logging.info("Restableciendo esquema 'insumos'...")
    try:
        ejecutar_sql("DROP SCHEMA IF EXISTS insumos CASCADE; CREATE SCHEMA insumos;")
        logging.info("Esquema 'insumos' restablecido correctamente.")
    except Exception as e:
        logging.error(f"Error restableciendo esquema 'insumos': {e}")
        raise

def restablecer_esquema_estructura_intermedia():
    logging.info("Restableciendo esquema 'estructura_intermedia'...")
    try:
        ejecutar_sql("DROP SCHEMA IF EXISTS estructura_intermedia CASCADE; CREATE SCHEMA estructura_intermedia;")
        logging.info("Esquema 'estructura_intermedia' restablecido correctamente.")
    except Exception as e:
        logging.error(f"Error restableciendo esquema 'estructura_intermedia': {e}")
        raise

def restablecer_esquema_ladm():
    logging.info("Restableciendo esquema 'ladm'...")
    try:
        ejecutar_sql("DROP SCHEMA IF EXISTS ladm CASCADE; CREATE SCHEMA ladm;")
        logging.info("Esquema 'ladm' restablecido correctamente.")
    except Exception as e:
        logging.error(f"Error restableciendo esquema 'ladm': {e}")
        raise

def clean_sql_script(script):
    """Elimina comentarios de bloque y devuelve el script limpio."""
    logging.info("Limpiando comentarios en script SQL (si los hubiera).")
    cleaned = re.sub(r'/\*.*?\*/', '', script, flags=re.DOTALL)
    if "/*" in cleaned:
        cleaned = cleaned.split("/*")[0]
    return cleaned

def ejecutar_importar_estructura_intermedia():
    logging.info("Importando estructura_intermedia...")
    try:
        script_sql = estructura_intermedia()
        if isinstance(script_sql, str):
            script_sql = clean_sql_script(script_sql)
            ejecutar_sql(script_sql)
            logging.info("Estructura_intermedia importada correctamente.")
        else:
            logging.info("Estructura_intermedia importada por función interna.")
    except Exception as e:
        logging.error(f"Error importando estructura_intermedia: {e}")
        raise

def importar_insumos_desde_web():
    """Descarga, descomprime y sube a la base de datos los insumos declarados en la configuración."""
    logging.info("Iniciando importación de insumos...")
    limpiar_carpeta_temporal()
    config = leer_configuracion()
    db_config = config["db"]

    insumos_web = config.get("insumos_web", {})
    insumos_local = config.get("insumos_local", {})
    base_local = "/opt/airflow/etl"

    if not insumos_web:
        logging.error("No se encontraron 'insumos_web' en la configuración.")
        

    os.makedirs(TEMP_FOLDER, exist_ok=True)

    for key, url in insumos_web.items():
        zip_path = os.path.join(TEMP_FOLDER, f"{key}.zip")
        logging.info(f"Procesando insumo '{key}'...")

        # 1. Descargar (o usar archivo local) el ZIP
        zip_path = _obtener_archivo_zip(key, url, insumos_local, base_local, zip_path)

        # 2. Extraer el ZIP
        extract_folder = os.path.join(TEMP_FOLDER, key)
        os.makedirs(extract_folder, exist_ok=True)
        _extraer_zip(zip_path, extract_folder)

        # 3. Buscar archivo SHP
        shp_file = _buscar_shp_en_carpeta(extract_folder)
        if not shp_file:
            logging.error(f"No se encontró archivo SHP en {extract_folder} para '{key}'.")
            

        # 4. Importar el SHP a PostgreSQL
        _importar_shp_a_postgres(db_config, shp_file, f"insumos.{key}")
    logging.info("Proceso de importación de insumos finalizado.")


def _obtener_archivo_zip(key, url, insumos_local, base_local, zip_path):
    """
    Intenta descargar el ZIP desde la URL; si falla, busca un ZIP local de respaldo.
    Retorna la ruta final del ZIP a usar.
    """
    try:
        logging.info(f"Descargando insumo '{key}' desde {url}...")
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"Archivo '{key}.zip' descargado correctamente.")
        return zip_path
    except Exception as e:
        logging.error(f"Error descargando '{url}': {e}")
        # Fallback: usar archivo local
        return _validar_archivo_local(key, insumos_local, base_local)


def _validar_archivo_local(key, insumos_local, base_local):
    """
    Verifica si existe una ruta local para el ZIP y la retorna.
    Lanza excepción si no existe.
    """
    if key in insumos_local:
        local_zip_path = os.path.join(base_local, insumos_local[key].lstrip("/"))
        if os.path.exists(local_zip_path):
            logging.info(f"Usando archivo local para '{key}': {local_zip_path}")
            return local_zip_path
        logging.error(f"Archivo local para '{key}' no encontrado en {local_zip_path}.")
    logging.error(f"No se encontró entrada local para '{key}'.")


def _extraer_zip(zip_path, extract_folder):
    """Extrae el contenido de un archivo ZIP en la carpeta indicada."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        logging.info(f"Archivo extraído correctamente en: {extract_folder}")
    except zipfile.BadZipFile as e:
        logging.error(f"El archivo '{zip_path}' no es un ZIP válido: {e}")
        raise  # Mantén o cambia por otra excepción específica si lo deseas
    except Exception as e:
        logging.error(f"Error extrayendo '{zip_path}': {e}")
        raise  # Mantén o cambia por otra excepción específica si lo deseas


def _buscar_shp_en_carpeta(folder):
    """Recorre recursivamente la carpeta para encontrar el primer archivo SHP y devuelve su ruta."""
    for root, dirs, files in os.walk(folder):
        for file_name in files:
            if file_name.endswith(".shp"):
                return os.path.join(root, file_name)
    return None


def _importar_shp_a_postgres(db_config, shp_file, table_name):
    """Importa el SHP a la base de datos PostgreSQL usando OGR2OGR."""
    logging.info(f"Importando '{shp_file}' en la tabla '{table_name}'...")
    command = [
        "ogr2ogr", "-f", "PostgreSQL",
        f"PG:host={db_config['host']} port={db_config['port']} "
        f"dbname={db_config['db_name']} user={db_config['user']} password={db_config['password']}",
        shp_file,
        "-nln", table_name,
        "-overwrite",
        "-progress",
        "-lco", "GEOMETRY_NAME=geom",
        "-lco", "FID=gid",
        "-nlt", "PROMOTE_TO_MULTI",
        "-t_srs", "EPSG:9377"
    ]
    try:
        subprocess.run(command, capture_output=True, text=True, check=True)
        logging.info(f"Archivo '{shp_file}' importado correctamente en '{table_name}'.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error importando '{table_name}': {e.stderr}")
        raise


def ejecutar_migracion_datos_estructura_intermedia():
    logging.info("Migrando datos a estructura_intermedia...")
    try:
        script_sql = transformacion_datos()
        if isinstance(script_sql, str):
            ejecutar_sql(script_sql)
            logging.info("Migración a estructura_intermedia completada.")
        else:
            logging.info("Migración a estructura_intermedia completada por función interna.")
    except Exception as e:
        logging.error(f"Error migrando a estructura_intermedia: {e}")
        raise

def ejecutar_validacion_datos():
    logging.info("Validando datos en la estructura intermedia...")
    try:
        resultado = validar_estructura()
        if resultado is not None:
            if isinstance(resultado, bool) and not resultado:
                logging.error("Validación de datos falló.")
                raise
            elif isinstance(resultado, str):
                ejecutar_sql(resultado)
        logging.info("Validación de datos completada.")
    except Exception as e:
        logging.error(f"Error validando datos: {e}")
        raise

def ejecutar_migracion_datos_ladm():
    logging.info("Migrando datos al modelo LADM...")
    try:
        script_sql = importar_al_modelo()
        if isinstance(script_sql, str):
            ejecutar_sql(script_sql)
            logging.info("Migración a LADM completada.")
        else:
            logging.info("Migración a LADM completada por función interna.")
    except Exception as e:
        logging.error(f"Error migrando a LADM: {e}")
        raise

def exportar_datos_ladm_rl2():
    logging.info("Exportando datos del esquema 'ladm' a XTF (ili2db) ...")
    config = leer_configuracion()
    db_config = config["db"]
    ili2db_path = ILI2DB_JAR_PATH
    model_dir = MODEL_DIR
    xtf_folder = "/opt/airflow/etl/xtf"
    if not os.path.exists(xtf_folder):
        os.makedirs(xtf_folder)
    xtf_path = os.path.join(xtf_folder, "rl2.xtf")
    command = [
        "java",
        "-jar",
        ili2db_path,
        "--dbhost", db_config["host"],
        "--dbport", str(db_config["port"]),
        "--dbusr", db_config["user"],
        "--dbpwd", db_config["password"],
        "--dbdatabase", db_config["db_name"],
        "--dbschema", "ladm",
        "--export",
        "--exportTid",
        "--disableValidation",
        "--strokeArcs",
        "--modeldir", model_dir,
        "--models", "LADM_COL_v_1_0_0_Ext_RL2",
        "--iliMetaAttrs", "NULL",
        "--defaultSrsAuth", "EPSG",
        "--defaultSrsCode", "9377",
        xtf_path
    ]
    logging.info("Ejecutando exportación a XTF:")
    logging.info(" ".join(command))
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logging.info(f"Exportación a XTF completada: {result.stderr.strip()}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error exportando XTF: {e.stderr}")
        raise

# -------------- REPORTE DE TABLAS Y COLUMNAS (SOLO ESTRUCTURA) --------------

def report_schema_expectations(yaml_filename, schema):
    """
    Genera un reporte comparando las tablas y columnas esperadas (definidas en el YAML)
    con las encontradas en el esquema indicado.
    """
    logging.info(f"Generando reporte de estructura para {yaml_filename} en el esquema '{schema}'...")

    # 1. Cargar la configuración y las expectativas desde YAML
    data = _cargar_expectativas_desde_yaml(yaml_filename)
    expected_tables = _obtener_tablas_esperadas(data)

    # 2. Crear el engine de SQLAlchemy y obtener la lista de tablas en el esquema
    engine = _obtener_engine_sqlalchemy()
    actual_tables = _obtener_tablas_esquema(engine, schema)

    # 3. Comparar tablas y generar reporte base
    report_lines = _revisar_tablas_encontradas(schema, expected_tables, actual_tables)

    # 4. Para cada tabla esperada, revisar columnas
    report_lines += _revisar_columnas_tabla(engine, schema, expected_tables, actual_tables)

    # 5. Consolidar reporte y loguear
    final_report = "\n".join(report_lines)
    logging.info("Reporte de validación de estructura:\n" + final_report)
    return final_report


def _cargar_expectativas_desde_yaml(yaml_filename):
    """
    Lee el archivo YAML y devuelve los datos cargados.
    Lanza excepción si el archivo no existe o no tiene el formato esperado.
    """
    yaml_path = os.path.join(GX_DIR, yaml_filename)
    if not os.path.exists(yaml_path):
        logging.error(f"Archivo de expectativas {yaml_path} no existe.")

    with open(yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if "expectations" not in data:
        logging.error("El archivo de expectativas no contiene 'expectations'.")

    return data


def _obtener_tablas_esperadas(data):
    """
    Procesa el contenido del YAML y devuelve un diccionario con:
      - clave: nombre de tabla
      - valor: { 'expected_columns': set(...), 'exact_match': bool }
    """
    expected_tables = {}
    for exp in data["expectations"]:
        if exp.get("expectation_type") == "expect_table_columns_to_match_set":
            table = exp.get("meta", {}).get("table")
            if table:
                expected_tables[table] = {
                    "expected_columns": set(exp.get("kwargs", {}).get("column_set", [])),
                    "exact_match": exp.get("kwargs", {}).get("exact_match", False)
                }
    return expected_tables


def _obtener_engine_sqlalchemy():
    """
    Construye y devuelve un engine de SQLAlchemy para conectarse a la base de datos.
    """
    config = leer_configuracion()
    db_config = config["db"]
    db_user = db_config["user"]
    db_password = db_config["password"]
    db_host = db_config["host"]
    db_port = db_config["port"]
    db_name = "arfw_etl_rl2"  # Nombre fijo según lógica previa

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return sqlalchemy.create_engine(connection_string)


def _obtener_tablas_esquema(engine, schema):
    """
    Devuelve un conjunto con los nombres de las tablas encontradas en el esquema indicado.
    """
    query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema}';
    """
    df_tables = pd.read_sql(query, engine)
    return set(df_tables["table_name"].tolist())


def _revisar_tablas_encontradas(schema, expected_tables, actual_tables):
    """
    Genera la parte del reporte en la que se informa qué tablas 
    esperadas se encuentran o faltan en el esquema.
    """
    report_lines = [f"REPORTE DE ESTRUCTURA PARA EL ESQUEMA '{schema}':\n"]
    report_lines.append("Tablas esperadas vs. encontradas:")

    for table in expected_tables.keys():
        if table in actual_tables:
            report_lines.append(f"  {table}: ✔️")
        else:
            report_lines.append(f"  {table}: ❌ (Falta)")

    report_lines.append("\nDetalle de columnas para cada tabla:")
    return report_lines


def _revisar_columnas_tabla(engine, schema, expected_tables, actual_tables):
    """
    Para cada tabla, compara las columnas esperadas vs. las encontradas.
    Retorna la sección de reporte correspondiente.
    """
    report = []
    for table, exp_details in expected_tables.items():
        if table not in actual_tables:
            continue

        expected_cols = exp_details["expected_columns"]
        exact = exp_details["exact_match"]

        query_cols = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' 
              AND table_name = '{table}';
        """
        df_cols = pd.read_sql(query_cols, engine)
        actual_cols = set(df_cols["column_name"].tolist())

        if exact:
            report.append(_comparar_columnas_exactas(table, expected_cols, actual_cols))
        else:
            report.append(_comparar_columnas_subconjunto(table, expected_cols, actual_cols))

    return report


def _comparar_columnas_exactas(table, expected_cols, actual_cols):
    """
    Si exact_match es True, se espera que las columnas coincidan exactamente.
    Retorna el texto del reporte para esa tabla.
    """
    if actual_cols == expected_cols:
        return f"  {table}: Se esperaban {sorted(expected_cols)} y se encontraron exactamente. ✔️"
    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols
    resultado = [f"  {table}: ❌ Diferencias en columnas:"]
    if missing:
        resultado.append(f"    Faltantes: {sorted(missing)}")
    if extra:
        resultado.append(f"    Extras: {sorted(extra)}")
    return "\n".join(resultado)


def _comparar_columnas_subconjunto(table, expected_cols, actual_cols):
    """
    Si exact_match es False, se espera que las columnas esperadas sean 
    un subconjunto de las columnas reales.
    Retorna el texto del reporte para esa tabla.
    """
    if expected_cols.issubset(actual_cols):
        return f"  {table}: Se esperaba (subconjunto) {sorted(expected_cols)} y se encontraron. ✔️"
    missing = expected_cols - actual_cols
    return f"  {table}: ❌ Faltan columnas: {sorted(missing)}"


# ------------------------- DEFINICIÓN DEL DAG -------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 25)
}

with DAG(
    "etl_rl2_txf",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    inicio_etl = DummyOperator(task_id="Inicio_ETL_LADM_RL2")
    
    validar_conexion_postgres_task = PythonOperator(
        task_id="Validar_Conexion_Postgres",
        python_callable=validar_conexion_postgres
    )

    restablecer_esquema_insumos_task = PythonOperator(
        task_id="Restablecer_Esquema_Insumos",
        python_callable=restablecer_esquema_insumos
    )
    importar_insumos_task = PythonOperator(
        task_id="Importar_Insumos",
        python_callable=importar_insumos_desde_web
    )
    reporte_expectativas_insumos_task = PythonOperator(
        task_id="Reporte_Expectativas_Insumos",
        python_callable=lambda: report_schema_expectations("gx_insumos.yml", "insumos")
    )

    restablecer_estructura_intermedia_task = PythonOperator(
        task_id="Restablecer_Estructura_Intermedia",
        python_callable=restablecer_esquema_estructura_intermedia
    )
    importar_estructura_intermedia_task = PythonOperator(
        task_id="Importar_Estructura_Intermedia",
        python_callable=ejecutar_importar_estructura_intermedia
    )
    reporte_expectativas_estructura_task = PythonOperator(
        task_id="Reporte_Expectativas_Estructura",
        python_callable=lambda: report_schema_expectations("gx_estructura_intermedia.yml", "estructura_intermedia")
    )

    restablecer_esquema_ladm_task = PythonOperator(
        task_id="Restablecer_Esquema_LADM",
        python_callable=restablecer_esquema_ladm
    )
    importar_esquema_ladm_task = PythonOperator(
        task_id="Importar_Esquema_LADM",
        python_callable=importar_esquema_ladm_rl2
    )
    reporte_expectativas_ladm_task = PythonOperator(
        task_id="Reporte_Expectativas_LADM",
        python_callable=lambda: report_schema_expectations("gx_ladm.yml", "ladm")
    )

    migracion_datos_estructura_intermedia_task = PythonOperator(
        task_id="Migracion_Datos_Estructura_Intermedia",
        python_callable=ejecutar_migracion_datos_estructura_intermedia
    )
    validacion_datos_task = PythonOperator(
        task_id="Validacion_Datos",
        python_callable=ejecutar_validacion_datos
    )
    migracion_datos_ladm_task = PythonOperator(
        task_id="Migracion_Datos_LADM",
        python_callable=ejecutar_migracion_datos_ladm
    )
    reporte_expectativas_ladm_despues_task = PythonOperator(
        task_id="Reporte_Expectativas_LADM_Despues",
        python_callable=lambda: report_schema_expectations("gx_ladm.yml", "ladm")
    )
    exportar_datos_ladm_task = PythonOperator(
        task_id="Exportar_Datos_LADM",
        python_callable=exportar_datos_ladm_rl2
    )
    fin_etl = DummyOperator(task_id="Finaliza_ETL_LADM_RL2")

    # Cadena de ejecución
    inicio_etl >> validar_conexion_postgres_task
    validar_conexion_postgres_task >> [
        restablecer_esquema_insumos_task,
        restablecer_estructura_intermedia_task,
        restablecer_esquema_ladm_task
    ]
    restablecer_esquema_insumos_task >> importar_insumos_task >> reporte_expectativas_insumos_task
    restablecer_estructura_intermedia_task >> importar_estructura_intermedia_task >> reporte_expectativas_estructura_task
    restablecer_esquema_ladm_task >> importar_esquema_ladm_task >> reporte_expectativas_ladm_task
    [reporte_expectativas_insumos_task, reporte_expectativas_estructura_task, reporte_expectativas_ladm_task] >> migracion_datos_estructura_intermedia_task
    migracion_datos_estructura_intermedia_task >> validacion_datos_task
    validacion_datos_task >> migracion_datos_ladm_task
    migracion_datos_ladm_task >> reporte_expectativas_ladm_despues_task >> exportar_datos_ladm_task
    exportar_datos_ladm_task >> fin_etl
