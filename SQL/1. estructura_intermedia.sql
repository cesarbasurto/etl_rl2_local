/***********************************************************************************
             Creación de estructura de datos intermedia 
        	Migración del Ley 2  al modelo LADM_COL-LEY2
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)        
        email           : contacto@ceicol.com
 ***************************************************************************/
/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 **********************************************************************************/

--====================================================
-- Creación de la extensión postgis
--====================================================
CREATE EXTENSION IF NOT EXISTS postgis;

--====================================================
-- Creación de la extensión uuid
--====================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

--====================================================
-- Inserción del sistema origen unico nacional
--====================================================
INSERT into spatial_ref_sys (
  srid, auth_name, auth_srid, proj4text, srtext
)
values
  (
    9377,
    'EPSG',
    9377,
    '+proj=tmerc +lat_0=4.0 +lon_0=-73.0 +k=0.9992 +x_0=5000000 +y_0=2000000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs ',
    'PROJCRS["MAGNA-SIRGAS / Origen-Nacional", BASEGEOGCRS["MAGNA-SIRGAS", DATUM["Marco Geocentrico Nacional de Referencia", ELLIPSOID["GRS 1980",6378137,298.257222101, LENGTHUNIT["metre",1]]], PRIMEM["Greenwich",0, ANGLEUNIT["degree",0.0174532925199433]], ID["EPSG",4686]], CONVERSION["Colombia Transverse Mercator", METHOD["Transverse Mercator", ID["EPSG",9807]], PARAMETER["Latitude of natural origin",4, ANGLEUNIT["degree",0.0174532925199433], ID["EPSG",8801]], PARAMETER["Longitude of natural origin",-73, ANGLEUNIT["degree",0.0174532925199433], ID["EPSG",8802]], PARAMETER["Scale factor at natural origin",0.9992, SCALEUNIT["unity",1], ID["EPSG",8805]], PARAMETER["False easting",5000000, LENGTHUNIT["metre",1], ID["EPSG",8806]], PARAMETER["False northing",2000000, LENGTHUNIT["metre",1], ID["EPSG",8807]]], CS[Cartesian,2], AXIS["northing (N)",north, ORDER[1], LENGTHUNIT["metre",1]], AXIS["easting (E)",east, ORDER[2], LENGTHUNIT["metre",1]], USAGE[ SCOPE["unknown"], AREA["Colombia"], BBOX[-4.23,-84.77,15.51,-66.87]], ID["EPSG",9377]]'
  ) ON CONFLICT (srid) DO NOTHING;
  
--========================================
--Creación de esquema
--========================================
drop schema IF EXISTS estructura_intermedia CASCADE;

CREATE schema IF NOT EXISTS estructura_intermedia;

--========================================
--Fijar esquema
--========================================

set search_path to 
	estructura_intermedia,	--Nombre del esquema de estructura de datos intermedia
	public;


--=============================================
-- Área de reserva Ley 2
--=============================================
DROP TABLE IF exists rl2_uab_areareserva;

CREATE TABLE rl2_uab_areareserva (
	uab_identificador varchar(7) NOT NULL, -- Identificador interno con el cual se identifica el área de reserva de ley 2da
	uab_nombre_reserva varchar(150) NOT NULL, -- Nombre de la reserva de acuerdo con la Ley 2ª de 1959
	uab_nombre varchar(255) NULL, -- Nombre que recibe la unidad administrativa básica, en muchos casos toponímico, especialmente en terrenos rústicos.
	ue_geometria public.geometry(multipolygonz, 9377) NULL, -- Materialización del método createArea(). Almacena de forma permanente la geometría de tipo poligonal.
	interesado_tipo varchar(255) NOT NULL,
	interesado_nombre varchar(255) NULL, -- Nombre del interesado.
	interesado_tipo_interesado varchar(255) NOT NULL, -- Tipo de interesado
	interesado_tipo_documento varchar(255) NULL, -- Tipo de documento de identificación del interesado
	interesado_numero_documento varchar(255) NULL, -- Número del documento del interesado
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) null, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);
--=============================================
-- Zonificacion Ley 2
--=============================================
DROP TABLE IF exists rl2_uab_zonificacion;

CREATE TABLE rl2_uab_zonificacion (
	uab_identificador varchar(7) NOT NULL, -- Identificador interno para identificación de la Reserva Ley 2ª de 1959
	uab_tipo_zona varchar(255) NOT NULL, -- Define el tipo de zonificación de la Reserva Ley 2ª de 1959.
	uab_areareserva varchar(255) NOT NULL,
	uab_nombre varchar(255) NULL, -- Nombre que recibe la unidad administrativa básica, en muchos casos toponímico, especialmente en terrenos rústicos.
	ue_geometria public.geometry(multipolygonz, 9377) NULL, -- Materialización del método createArea(). Almacena de forma permanente la geometría de tipo poligonal.
	interesado_tipo varchar(255) NOT NULL,
	interesado_nombre varchar(255) NULL, -- Nombre del interesado.
	interesado_tipo_interesado varchar(255) NOT NULL, -- Tipo de interesado
	interesado_tipo_documento varchar(255) NULL, -- Tipo de documento de identificación del interesado
	interesado_numero_documento varchar(255) NULL, -- Número del documento del interesado
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) null, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);

--=============================================
-- Compensacion Ley 2
--=============================================
DROP TABLE IF exists rl2_uab_compensacion;

CREATE TABLE rl2_uab_compensacion (
	uab_identificador varchar(15) NOT NULL, -- Interno con el cual se identifica la reserva a la cual se está haciendo la compensación
	uab_expediente varchar(100) NOT NULL, -- Número del expediente interno relacionado con la compensación
	uab_observaciones text NULL, -- Nombre del proyecto objeto de la sustracción
	uab_areareserva varchar(255) NOT NULL,
	uab_sustraccion varchar(255) NULL,
	uab_nombre varchar(255) NULL, -- Nombre que recibe la unidad administrativa básica, en muchos casos toponímico, especialmente en terrenos rústicos.
	ue_geometria public.geometry(multipolygonz, 9377) NULL, -- Materialización del método createArea(). Almacena de forma permanente la geometría de tipo poligonal.
	interesado_tipo varchar(255) NOT NULL,
	interesado_nombre varchar(255) NULL, -- Nombre del interesado.
	interesado_tipo_interesado varchar(255) NOT NULL, -- Tipo de interesado
	interesado_tipo_documento varchar(255) NULL, -- Tipo de documento de identificación del interesado
	interesado_numero_documento varchar(255) NULL, -- Número del documento del interesado
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) NULL, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);

--=============================================
-- Sustraccion Ley 2
--=============================================
DROP TABLE IF exists rl2_uab_sustraccion;

CREATE TABLE rl2_uab_sustraccion (
	uab_identificador varchar(15) NOT NULL, -- Identificador interno para identificación de la sustracción definitiva o temporal de la Reserva Ley 2ª de 1959
	uab_expediente varchar(100) NOT NULL,
	uab_tipo_sustraccion varchar(255) NOT NULL, -- Dominio que define el tipo de sustracción realikzado a la Reserva Ley 2ª de 1959
	uab_tipo_causal varchar(255) NULL, -- Dominio de datos que establece la causal de sustracción realizada a la Reserva Ley 2ª de 1959
	uab_sustrajo varchar(255) NOT NULL, -- Corresponde a la sigla de la entidad que aprobó el área de sustracción
	uab_detalle_sustrajo varchar(255) NULL,
	uab_fin_sustraccion date NULL, -- Corresponde a la fecha en la que finaliza la sustracción (aplica para las sustracciones temporales)
	uab_tipo_sector varchar(255) NOT NULL, -- Identifica el sector que realizó la solicitud de sustracción
	uab_detalle_sector varchar(255) NULL, -- Cuando el tipo de sector sea Otro, se podrá especificar el detalle de este sector
	uab_observaciones text NULL, -- Observaciones generales de la sustracción
	uab_areareserva varchar(255) NULL,
	uab_nombre_areareserva varchar(255) NULL,
	uab_compensacion varchar(255) NULL,	
	uab_nombre varchar(255) NULL, -- Nombre que recibe la unidad administrativa básica, en muchos casos toponímico, especialmente en terrenos rústicos.
	ue_geometria public.geometry(multipolygonz, 9377) NULL, -- Materialización del método createArea(). Almacena de forma permanente la geometría de tipo poligonal.
	interesado_tipo varchar(255) NOT NULL,
	interesado_nombre varchar(255) NULL, -- Nombre del interesado.
	interesado_tipo_interesado varchar(255) NOT NULL, -- Tipo de interesado
	interesado_tipo_documento varchar(255) NULL, -- Tipo de documento de identificación del interesado
	interesado_numero_documento varchar(255) NULL, -- Número del documento del interesado
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) NULL, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);


--=============================================
-- Fuente Administrativa Ley 2
--=============================================
DROP TABLE IF exists rl2_fuenteadministrativa;

CREATE TABLE rl2_fuenteadministrativa (
	uab_identificador varchar(15) NOT NULL, -- Interno con el cual se identifica la reserva a la cual se está haciendo la compensación
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_numero int4 null,
	fuente_administrativa_anio int4 null
);

--=============================================
-----------------------------------------------
-- Funciones de homologacion Ley 2
-----------------------------------------------
--=============================================

--=============================================
-- Funcion de homologacion a texto
--=============================================

DROP FUNCTION IF exists homologar_texto;

CREATE OR REPLACE FUNCTION homologar_texto(texto_input TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN upper(
        regexp_replace(
            translate(trim(texto_input), 'áéíóúÁÉÍÓÚ', 'aeiouAEIOU'), 
            '\s+', '', 
            'g'
        )
    );
END;
$$ LANGUAGE plpgsql;

--=============================================
-- Funcion de homologacion a numero
--=============================================

DROP FUNCTION IF exists estructura_intermedia.homologar_numero;

CREATE OR REPLACE FUNCTION estructura_intermedia.homologar_numero(texto TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN regexp_replace(texto, '[^0-9]', '', 'g');
END;
$$ LANGUAGE plpgsql;


