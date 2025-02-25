import os
import sys
import json
import zipfile
import requests
import subprocess
import psycopg2
import shutil
from datetime import datetime, timedelta, date
import numpy as np
import sqlalchemy
import pandas as pd
import yaml
import great_expectations as ge
from great_expectations.dataset import PandasDataset
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler

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

# ------------------------------------------------------------------------------
# CONFIGURACIÓN
# ------------------------------------------------------------------------------
DB_CONNECTION_STRING_GE = "postgresql://ladm:123456@3.133.21.126:5432/arfw_etl_rl2"
EXPECTATIONS_FOLDER = "./gx/"
EXPECTATION_YML_PATH_LADM = os.path.join(EXPECTATIONS_FOLDER, "gx_insumos.yml")

# ------------------------------------------------------------------------------
# FUNCIÓN UNIFICADA ge_convert_for_yaml
# ------------------------------------------------------------------------------
def ge_convert_for_yaml(obj):
    """
    Convierte recursivamente objetos no serializables a tipos nativos de Python.
    Se fuerza a que las claves sean cadenas. Si el objeto no es de un tipo básico,
    se convierte a cadena.
    """
    try:
        from great_expectations.render.renderer_configuration import MetaNotesFormat
    except ImportError:
        MetaNotesFormat = None

    if isinstance(obj, (pd.Timestamp, datetime, date)):
        return obj.isoformat()
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.bool_)):
        return bool(obj)
    if MetaNotesFormat is not None and isinstance(obj, MetaNotesFormat):
        return str(obj)
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode(errors="ignore")
    if isinstance(obj, dict):
        return {str(k): ge_convert_for_yaml(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [ge_convert_for_yaml(item) for item in obj]
    if isinstance(obj, tuple):
        return tuple(ge_convert_for_yaml(item) for item in obj)
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    return str(obj)

# ------------------------------------------------------------------------------
# FUNCIONES UTILITARIAS (ETL y DB)
# ------------------------------------------------------------------------------
def leer_configuracion():
    """
    Lee la configuración desde el archivo Config.json y retorna un diccionario.
    Asegúrate de que la ruta CONFIG_PATH sea correcta.
    """
    config_path = "/opt/airflow/etl/Config.json"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        print("✅ Configuración leída correctamente.")
        return config
    except Exception as e:
        print(f"❌ Error al leer Config.json: {e}")
        raise

def ejecutar_script_sql(sql_script):
    """
    Ejecuta el script SQL recibido utilizando psycopg2.
    """
    config = leer_configuracion()
    db_config = config["db"]
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            dbname=db_config["db_name"],
            user=db_config["user"],
            password=db_config["password"]
        )
        cursor = conn.cursor()
        print("Ejecutando script SQL...")
        cursor.execute(sql_script)
        conn.commit()
        cursor.close()
        print("✅ Script ejecutado correctamente.")
    except Exception as e:
        print(f"❌ Error al ejecutar el script SQL: {e}")
        raise
    finally:
        if conn:
            conn.close()

def verificar_conexion_postgres():
    """
    Verifica la conexión a PostgreSQL usando la base 'postgres' para consultar el catálogo.
    """
    config = leer_configuracion()
    db_config = config["db"]
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            dbname="postgres"
        )
        print("✅ Conexión exitosa a PostgreSQL (base 'postgres').")
    except Exception as e:
        print(f"❌ Error al conectar a PostgreSQL: {e}")
        raise
    finally:
        if conn:
            conn.close()

def validar_o_crear_base_de_datos():
    """
    Valida si la base de datos 'arfw_etl_rl2' existe.
    Si no existe, la crea.
    """
    config = leer_configuracion()
    db_config = config["db"]
    db_name = db_config["db_name"]
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            dbname="postgres"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
        exists = cursor.fetchone()
        if exists:
            print(f"✅ La base de datos '{db_name}' ya existe.")
        else:
            print(f"⚠️ La base de datos '{db_name}' no existe. Creándola...")
            cursor.execute(f"CREATE DATABASE {db_name};")
            print(f"✅ Base de datos '{db_name}' creada correctamente.")
        cursor.close()
    except Exception as e:
        print(f"❌ Error al validar/crear la base de datos '{db_name}': {e}")
        raise
    finally:
        if conn:
            conn.close()

# ------------------- FUNCIONES DE ETL E INSUMOS ------------------------------
def eliminar_esquema(schema_name):
    """Elimina el esquema especificado (si existe) en la base de datos."""
    config = leer_configuracion()
    db_config = config["db"]
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            dbname=db_config["db_name"],
            user=db_config["user"],
            password=db_config["password"]
        )
        cursor = conn.cursor()
        cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
        conn.commit()
        cursor.close()
        print(f"✅ Esquema '{schema_name}' eliminado correctamente.")
    except Exception as e:
        print(f"❌ Error al eliminar el esquema '{schema_name}': {e}")
        raise
    finally:
        if conn:
            conn.close()

def crear_esquema(schema_name):
    """Crea el esquema especificado en la base de datos si no existe."""
    config = leer_configuracion()
    db_config = config["db"]
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            dbname=db_config["db_name"],
            user=db_config["user"],
            password=db_config["password"]
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        conn.commit()
        cursor.close()
        print(f"✅ Esquema '{schema_name}' creado/verificado correctamente.")
    except Exception as e:
        print(f"❌ Error al crear el esquema '{schema_name}': {e}")
        raise
    finally:
        if conn:
            conn.close()

def importar_esquema_ladm_rl2():
    """Importa el esquema LADM-RL2 en PostgreSQL usando ili2db con SRID 9377."""
    config = leer_configuracion()
    db_config = config["db"]
    ili2db_path = "/opt/airflow/etl/libs/ili2pg-5.1.0.jar"
    model_dir = "/opt/airflow/etl/models/ladm_rl2/"
    epsg_script = "/opt/airflow/etl/scripts/insert_ctm12_pg.sql"
    command = [
        "java", "-jar", ili2db_path,
        "--schemaimport", "--setupPgExt",
        "--dbhost", db_config["host"],
        "--dbport", str(db_config["port"]),
        "--dbusr", db_config["user"],
        "--dbpwd", db_config["password"],
        "--dbdatabase", db_config["db_name"],
        "--dbschema", "ladm",
        "--coalesceCatalogueRef", "--createNumChecks", "--createUnique",
        "--createFk", "--createFkIdx", "--coalesceMultiSurface",
        "--coalesceMultiLine", "--coalesceMultiPoint", "--coalesceArray",
        "--beautifyEnumDispName", "--createGeomIdx", "--createMetaInfo",
        "--expandMultilingual", "--createTypeConstraint",
        "--createEnumTabsWithId", "--createTidCol", "--smart2Inheritance",
        "--strokeArcs", "--createBasketCol",
        "--defaultSrsAuth", "EPSG",
        "--defaultSrsCode", "9377",
        "--preScript", epsg_script,
        "--postScript", "NULL",
        "--modeldir", model_dir,
        "--models", "LADM_COL_v_1_0_0_Ext_RL2",
        "--iliMetaAttrs", "NULL"
    ]
    print("Ejecutando comando ili2pg (importar_esquema_ladm_rl2):")
    print(" ".join(command))
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        print("✅ Esquema LADM-RL2 importado correctamente.")
        print("STDOUT:", result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al importar esquema LADM-RL2: {e.stderr}")
        raise

def limpiar_carpeta_temporal():
    """Limpia la carpeta temporal eliminando todos sus archivos y subcarpetas."""
    temp_folder = "../etl/temp"
    if os.path.exists(temp_folder):
        for item in os.listdir(temp_folder):
            item_path = os.path.join(temp_folder, item)
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            except Exception as e:
                print(f"❌ Error al eliminar {item_path}: {e}")
                raise Exception(f"Error al eliminar {item_path}: {e}")
    print("✅ Carpeta temporal limpiada.")

def importar_insumos_desde_web():
    """
    Descarga, extrae e importa cada SHP como tabla en el esquema 'insumos'.
    Si falta algún insumo (ni web ni local), se lanza excepción y se termina el proceso.
    """
    config = leer_configuracion()
    db_config = config["db"]
    insumos_web = config.get("insumos_web", {})
    insumos_local = config.get("insumos_local", {})
    temp_folder = "/opt/airflow/etl/temp"
    base_local = "/opt/airflow/etl/"

    if not insumos_web:
        error_message = "❌ No se encontraron 'insumos_web' en la configuración."
        print(error_message)
        raise Exception(error_message)

    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    for key, url in insumos_web.items():
        print(f"Descargando '{key}' desde {url}...")
        zip_path = os.path.join(temp_folder, f"{key}.zip")
        download_success = False
        try:
            response = requests.get(url, stream=True, timeout=10)
            response.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"✅ Archivo {zip_path} descargado correctamente desde web.")
            download_success = True
        except Exception as e:
            print(f"❌ Error al descargar '{url}': {e}")

        if not download_success:
            if key in insumos_local:
                local_file_relative = insumos_local[key].lstrip("/")
                local_zip_path = os.path.join(base_local, local_file_relative)
                if os.path.exists(local_zip_path):
                    print(f"✅ Usando archivo local para '{key}': {local_zip_path}")
                    zip_path = local_zip_path
                else:
                    error_message = f"❌ No se encontró archivo local para '{key}' en {local_zip_path}."
                    print(error_message)
                    raise Exception(error_message)
            else:
                error_message = f"❌ No se encontró entrada local para '{key}', omitiendo."
                print(error_message)
                raise Exception(error_message)

        extract_folder = os.path.join(temp_folder, key)
        if not os.path.exists(extract_folder):
            os.makedirs(extract_folder)
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_folder)
            print(f"✅ Archivo {zip_path} extraído en {extract_folder}.")
        except Exception as e:
            error_message = f"❌ Error al extraer {zip_path}: {e}"
            print(error_message)
            raise Exception(error_message)

        shp_file = None
        for root, dirs, files in os.walk(extract_folder):
            for file in files:
                if file.endswith(".shp"):
                    shp_file = os.path.join(root, file)
                    break
            if shp_file:
                break

        if not shp_file:
            error_message = f"❌ No se encontró archivo SHP en {extract_folder} para '{key}'."
            print(error_message)
            raise Exception(error_message)

        print(f"📥 Importando '{key}' en el esquema 'insumos'...")
        command = [
            "ogr2ogr", "-f", "PostgreSQL",
            f"PG:host={db_config['host']} port={db_config['port']} dbname={db_config['db_name']} user={db_config['user']} password={db_config['password']}",
            shp_file,
            "-nln", f"insumos.{key}",
            "-overwrite", "-progress",
            "-lco", "GEOMETRY_NAME=geom",
            "-lco", "FID=gid",
            "-nlt", "PROMOTE_TO_MULTI",
            "-t_srs", "EPSG:9377"
        ]
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            print(f"✅ '{key}' importado correctamente en el esquema 'insumos'.")
            print("STDOUT:", result.stdout)
        except subprocess.CalledProcessError as e:
            error_message = f"❌ Error al importar '{key}': {e.stderr}"
            print(error_message)
            raise Exception(error_message)

def ejecutar_estructura_intermedia():
    """Ejecuta la función estructura_intermedia() obtenida desde etl_rl2_sql."""
    sql_script = estructura_intermedia()
    if not sql_script:
        raise ValueError("El script SQL para la estructura intermedia está vacío o es None.")
    print("DEBUG: Script de estructura intermedia obtenido.")
    ejecutar_script_sql(sql_script)

def ejecutar_transformacion_datos():
    """Ejecuta la función transformacion_datos() obtenida desde etl_rl2_sql."""
    sql_script = transformacion_datos()
    if not sql_script:
        raise ValueError("El script SQL para la transformación de datos está vacío o es None.")
    print("DEBUG: Script de transformación de datos obtenido.")
    ejecutar_script_sql(sql_script)

def ejecutar_importacion_modelo():
    """Ejecuta la función importar_al_modelo() obtenida desde etl_rl2_sql."""
    sql_script = importar_al_modelo()
    if not sql_script:
        raise ValueError("El script SQL para importar al modelo está vacío o es None.")
    print("DEBUG: Script de importación al modelo obtenido.")
    ejecutar_script_sql(sql_script)

def exportar_datos_ladm_rl2():
    """Exporta los datos del esquema 'ladm' a un archivo XTF usando ili2db con SRID 9377 y --strokeArcs."""
    config = leer_configuracion()
    db_config = config["db"]
    ili2db_path = "../etl/libs/ili2pg-5.1.0.jar"
    model_dir = "../etl/models/ladm_rl2/"
    
    xtf_folder = "../etl/xtf"
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
        "--models", "LADM_COL_v_4_0_1_Nucleo;ISO19107_PLANAS_V3_1;LADM_COL_v_1_0_0_Ext_RL2",
        "--iliMetaAttrs", "NULL",
        "--defaultSrsAuth", "EPSG",
        "--defaultSrsCode", "9377",
        xtf_path
    ]
    print("Ejecutando exportación a XTF (con SRID 9377 y --strokeArcs):")
    print(" ".join(command))
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        print(f"✅ Se ha exportado el XTF exitosamente: {result.stderr.strip()}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al ejecutar ili2db.jar para exportar XTF: {e.stderr}")
        raise

def validate_schema_ge(schema_name, expectation_yml_path, connection_string):
    """
    Lee el archivo YAML de expectativas para el esquema indicado y,
    para cada tabla (hasta 1000 registros), ejecuta la validación usando GE.
    Antes de validar, se fuerza la conversión a texto de las columnas especiales,
    incluyendo aquellas con dtype datetime.
    """
    try:
        with open(expectation_yml_path, "r") as f:
            expectations = yaml.safe_load(f)

        engine = sqlalchemy.create_engine(connection_string)
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE lower(table_schema) = lower(%s);
        """
        df = pd.read_sql(query, engine, params=(schema_name,))
        tables = df["table_name"].tolist()
        print(f"✅ Tablas en el esquema '{schema_name}': {tables}")

        passed_tables = []
        failed_tables = {}

        for table in tables:
            if table not in expectations:
                error_message = f"No se encontró suite de expectativas para la tabla '{table}' en el esquema '{schema_name}'."
                print(f"❌ {error_message}")
                failed_tables[table] = error_message
                continue

            exp_suite = expectations[table]
            query_table = f'SELECT * FROM "{schema_name}"."{table}" LIMIT 1000'
            df_table = pd.read_sql(query_table, engine)
            
            # Forzar conversión: si el nombre de la columna indica ser especial
            # o si el dtype es datetime, convertimos a string.
            for col in df_table.columns:
                if (col.lower() == "t_ili_tid" or 
                    col.lower().startswith("fecha") or 
                    col.lower() == "geom" or 
                    pd.api.types.is_datetime64_any_dtype(df_table[col])):
                    df_table[col] = df_table[col].apply(lambda x: str(x) if x is not None else x)
                    print(f"[{table}] {col} convertida (validación): {df_table[col].head().tolist()}")

            dataset = PandasDataset(df_table)
            validation_results = dataset.validate(expectation_suite=exp_suite)

            if not validation_results.get("success", False):
                summaries = []
                for res in validation_results.get("results", []):
                    if not res.get("success", False):
                        exp_type = res.get("expectation_config", {}).get("expectation_type", "desconocida")
                        column = res.get("expectation_config", {}).get("kwargs", {}).get("column", "desconocida")
                        unexpected_count = res.get("result", {}).get("unexpected_count", "N/A")
                        partial_list = res.get("result", {}).get("partial_unexpected_list", [])
                        summaries.append(
                            f"Columna '{column}', expectativa '{exp_type}': {unexpected_count} valor(es) inesperado(s). Valores: {partial_list}"
                        )
                summary_msg = "\n".join(summaries)
                error_message = f"Validación GE fallida para la tabla '{table}'.\nResumen de errores:\n{summary_msg}"
                print(f"❌ {error_message}")
                failed_tables[table] = error_message
            else:
                print(f"✅ Validación GE exitosa para la tabla '{table}'.")
                passed_tables.append(table)

        print("\n=== Resumen de Validación ===")
        if passed_tables:
            print(f"Tablas que PASARON la validación: {passed_tables}")
        else:
            print("Ninguna tabla pasó la validación.")

        if failed_tables:
            print("Tablas con ERRORES en la validación:")
            for table, error in failed_tables.items():
                print(f"- {table}: {error}")
        else:
            print("Ninguna tabla falló la validación.")

    except Exception as e:
        print(f"❌ Error al validar el esquema '{schema_name}': {e}")
        raise

# ------------------------------------------------------------------------------
# DEFINICIÓN DEL DAG
# ------------------------------------------------------------------------------
default_args_etl = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 3)
}

with DAG(
    "etl_rl2_txf",
    default_args=default_args_etl,
    schedule_interval=None,
    catchup=False
) as dag:

    inicio_etl = DummyOperator(task_id="Inicio_ETL_LADM_RL2")

    validar_o_crear_db_task = PythonOperator(
        task_id="Validar_o_Crear_DB",
        python_callable=validar_o_crear_base_de_datos
    )

    eliminar_esquema_ladm_task = PythonOperator(
        task_id="Eliminar_Esquema_LADM",
        python_callable=lambda: eliminar_esquema("ladm")
    )

    importar_esquema_ladm_task = PythonOperator(
        task_id="Crear_modelo_LADM",
        python_callable=importar_esquema_ladm_rl2
    )

    limpiar_carpeta_temporal_task = PythonOperator(
        task_id="Limpiar_Carpeta_Temp",
        python_callable=limpiar_carpeta_temporal
    )

    eliminar_esquema_insumos_task = PythonOperator(
        task_id="Eliminar_Esquema_Insumos",
        python_callable=lambda: eliminar_esquema("insumos")
    )

    crear_esquema_insumos_task = PythonOperator(
        task_id="Crear_Esquema_Insumos",
        python_callable=lambda: crear_esquema("insumos")
    )

    importar_insumos_web_task = PythonOperator(
        task_id="Importar_Insumos_Web_local",
        python_callable=importar_insumos_desde_web
    )

    ejecutar_estructura_intermedia_task = PythonOperator(
        task_id="Crear_Estructura_Intermedia",
        python_callable=ejecutar_estructura_intermedia
    )

    ejecutar_transformacion_datos_task = PythonOperator(
        task_id="Ejecutar_Transformar_Datos",
        python_callable=ejecutar_transformacion_datos
    )

    ejecutar_importacion_modelo_task = PythonOperator(
        task_id="Ejecutar_Importar_Modelo",
        python_callable=ejecutar_importacion_modelo
    )

    validar_ladm_ge_task = PythonOperator(
        task_id="Validar_LADM_GE",
        python_callable=lambda: validate_schema_ge("ladm", EXPECTATION_YML_PATH_LADM, DB_CONNECTION_STRING_GE)
    )

    exportar_datos_xtf_task = PythonOperator(
        task_id="Exportar_XTF",
        python_callable=exportar_datos_ladm_rl2
    )

    fin_etl = DummyOperator(task_id="Finaliza_ETL_LADM_RL2")

    (
        inicio_etl 
        >> validar_o_crear_db_task 
        >> eliminar_esquema_insumos_task 
        >> crear_esquema_insumos_task 
        >> ejecutar_estructura_intermedia_task 
        >> eliminar_esquema_ladm_task 
        >> importar_esquema_ladm_task 
        >> limpiar_carpeta_temporal_task        
        >> importar_insumos_web_task 
        >> ejecutar_transformacion_datos_task 
        >> ejecutar_importacion_modelo_task 
        >> validar_ladm_ge_task 
        >> exportar_datos_xtf_task 
        >> fin_etl
    )
