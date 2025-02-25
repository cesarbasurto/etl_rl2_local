/**********************************************************************************
            ETL de tranformación de insumos a estructura de datos intermedia
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

--========================================
--Fijar esquema
--========================================
set search_path to 
	estructura_intermedia, -- Esquema de estructura de datos intermedia
	public;


--========================================
-- Área de reserva Ley 2
--========================================
INSERT INTO estructura_intermedia.rl2_uab_areareserva(
	uab_identificador, 
	uab_nombre_reserva, 
	uab_nombre, 
	ue_geometria, 
	interesado_tipo, 
	interesado_nombre, 
	interesado_tipo_interesado, 
	interesado_tipo_documento, 
	interesado_numero_documento, 
	fuente_administrativa_tipo, 
	fuente_administrativa_estado_disponibilidad, 
	fuente_administrativa_tipo_formato, 
	fuente_administrativa_fecha_documento_fuente, 
	fuente_administrativa_nombre, 
	ddr_tipo_resposabilidad,
	ddr_tipo_derecho)	
SELECT 
	upper(id) AS uab_identificador, 
	nom_ley2 AS uab_nombre_reserva, 
	'rl2_uab_areareserva'::varchar(255) AS uab_nombre, 
	ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
	'Regulador'::varchar(255) AS interesado_tipo,
	'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre, 
	'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado, 
	'NIT'::varchar(255) AS interesado_tipo_documento, 
	'830.115.395-1'::varchar(255) AS interesado_numero_documento, 
	'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
	'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad, 
	'Documento'::varchar(255) AS fuente_administrativa_tipo_formato, 
	to_date('1959-01-17','YYYY-MM-DD') AS fuente_administrativa_fecha_documento_fuente, 
	'Ley 2 de 1959'::varchar(255) AS fuente_administrativa_nombre,
	null AS ddr_tipo_resposabilidad,
	'Realinderar'::varchar(255) AS ddr_tipo_derecho
FROM insumos.area_reserva_ley2;

--========================================
-- Zonificación Ley 2
--========================================
INSERT INTO estructura_intermedia.rl2_uab_zonificacion(
	uab_identificador, 
	uab_tipo_zona, 
	uab_areareserva, 
	uab_nombre, 
	ue_geometria, 
	interesado_tipo, 
	interesado_nombre, 
	interesado_tipo_interesado, 
	interesado_tipo_documento, 
	interesado_numero_documento, 
	fuente_administrativa_tipo, 
	fuente_administrativa_estado_disponibilidad, 
	fuente_administrativa_tipo_formato, 
	fuente_administrativa_fecha_documento_fuente, 
	fuente_administrativa_nombre, 
	ddr_tipo_resposabilidad, 
	ddr_tipo_derecho)
SELECT 
	('ZNL2_' || row_number() OVER ())::varchar(255) AS uab_identificador,
	case
		when homologar_texto(replace(tipo_zoni,' ','_'))=homologar_texto('Tipo_A') then 'Tipo_A'
		when homologar_texto(replace(tipo_zoni,' ','_'))=homologar_texto('Tipo_B') then 'Tipo_B'
		when homologar_texto(replace(tipo_zoni,' ','_'))=homologar_texto('Tipo_C') then 'Tipo_C'
		when homologar_texto(replace(tipo_zoni,' ','_'))=homologar_texto('AREAS_CON_PREVIA_DECISION_DE_ORDENAMIENTO') then 'Area_Previa_Decision_Ordenamiento'
		else 'No aplica'
	end AS uab_tipo_zona,
	upper(id_ley2)::varchar(255) AS uab_areareserva,
	'rl2_uab_zonificacion' AS uab_nombre,
	ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
	'Regulador'::varchar(255) AS interesado_tipo,
	'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre, 
	'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado, 
	'NIT'::varchar(255) AS interesado_tipo_documento, 
	'830.115.395-1'::varchar(255) AS interesado_numero_documento, 
	'Documento_Publico.' AS fuente_administrativa_tipo,	       
	'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad, 
	'Documento'::varchar(255) AS fuente_administrativa_tipo_formato, 
	null AS fuente_administrativa_fecha_documento_fuente,
	soporte::varchar(255) AS fuente_administrativa_nombre,
	'Zonificar'::varchar(255) AS ddr_tipo_resposabilidad,
	 null AS ddr_tipo_derecho
FROM insumos.zonificacion_ley2;

--========================================
-- Compensación Ley 2
--========================================
INSERT INTO estructura_intermedia.rl2_uab_compensacion(
	uab_identificador, 
	uab_expediente, 
	uab_observaciones, 
	uab_areareserva, 
	uab_sustraccion,
	uab_nombre, 
	ue_geometria, 
	interesado_tipo, 
	interesado_nombre, 
	interesado_tipo_interesado, 
	interesado_tipo_documento, 
	interesado_numero_documento, 
	fuente_administrativa_tipo, 
	fuente_administrativa_estado_disponibilidad, 
	fuente_administrativa_tipo_formato, 
	fuente_administrativa_fecha_documento_fuente, 
	fuente_administrativa_nombre, 
	ddr_tipo_resposabilidad, 
	ddr_tipo_derecho)
SELECT 
	('CML2_' || row_number() OVER ())::varchar(255) AS uab_identificador,
	expediente AS uab_expediente,
	proyecto AS uab_observaciones,
	upper(id_reserva)::varchar(255) AS uab_areareserva,
	null as uab_sustraccion,
	'rl2_uab_compensacion' AS uab_nombre,
	ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
	'Solicitante'::varchar(255) AS interesado_tipo,
	'Sin información'::varchar(255) AS interesado_nombre, 
	'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado, 
	null AS interesado_tipo_documento, 
	null AS interesado_numero_documento, 
	'Documento_Publico.' AS fuente_administrativa_tipo,
	'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad, 
	'Documento'::varchar(255) AS fuente_administrativa_tipo_formato, 
	fecha_acto AS fuente_administrativa_fecha_documento_fuente, 
	acto_admin::varchar(255) AS fuente_administrativa_nombre,
	'Compensar'::varchar(255) AS ddr_tipo_resposabilidad,
	null AS ddr_tipo_resposabilidad
FROM insumos.compensacion_ley2
WHERE left(id_reserva, 4) = 'LEY2';


--========================================
-- Sustracción Ley 2
--========================================
INSERT INTO estructura_intermedia.rl2_uab_sustraccion(
	uab_identificador, 
	uab_expediente,
	uab_tipo_sustraccion, 
	uab_tipo_causal, 
	uab_sustrajo, 
	uab_detalle_sustrajo, 
	uab_fin_sustraccion, 
	uab_tipo_sector, 
	uab_detalle_sector, 
	uab_observaciones, 
	uab_areareserva,
	uab_nombre_areareserva,
	uab_compensacion, 
	uab_nombre, 	
	ue_geometria, 
	interesado_tipo, 
	interesado_nombre, 
	interesado_tipo_interesado, 
	interesado_tipo_documento, 
	interesado_numero_documento, 
	fuente_administrativa_tipo, 
	fuente_administrativa_estado_disponibilidad, 
	fuente_administrativa_tipo_formato, 
	fuente_administrativa_fecha_documento_fuente, 
	fuente_administrativa_nombre, 
	ddr_tipo_resposabilidad, 
	ddr_tipo_derecho)	
WITH sustracciones_definitivas_limpieza AS (
	SELECT 
		('SDL2_' || row_number() OVER ())::varchar(255) AS uab_identificador,
		CASE
	        WHEN homologar_texto(sustrajo) = homologar_texto('INCODER') THEN 'INCODER'
	        WHEN homologar_texto(sustrajo) = homologar_texto('INCORA') THEN 'INCORA'
	        WHEN homologar_texto(sustrajo) = homologar_texto('INDERENA') THEN 'INDERENA'
	        WHEN homologar_texto(sustrajo) = homologar_texto('MADS') THEN 'MADS'
	        WHEN homologar_texto(sustrajo) = homologar_texto('MAVDT') THEN 'MAVDT'
	        ELSE 'Otro'
	    END  AS uab_sustrajo,
	    CASE
		    WHEN sector = 'Actividad Agropecuaria' THEN 'Reforma_Agraria'
		    WHEN sector = 'Adjudicación a Colonos' THEN 'Reforma_Agraria'
		    WHEN sector = 'Asuntos indígenas' THEN 'Reforma_Agraria'
		    WHEN sector = 'Base militar' THEN 'Otro'
		    WHEN sector = 'Colonización' THEN 'Reforma_Agraria'
		    WHEN sector = 'Constitución Resguardo indígena' THEN 'Otro'
		    WHEN sector = 'Creación del Distrito de Conservación de Suelos y Aguas' THEN 'Otro'
		    WHEN sector = 'Destinar tierras actividad agropecuaria' THEN 'Reforma_Agraria'
		    WHEN sector = 'Distrito de riego y asentamiento' THEN 'Otro'
		    WHEN sector = 'Distrito de riego y reasentamiento' THEN 'Otro'
		    WHEN sector = 'Distritos de conservacion' THEN 'Otro'
		    WHEN sector = 'Fines especiales zona costera' THEN 'Otro'
		    WHEN sector = 'Generación Electrica' THEN 'Energia'
		    WHEN sector = 'Generación eléctrica' THEN 'Energia'
		    WHEN sector = 'Hidrocarburos' THEN 'Hidrocarburos'
		    WHEN sector = 'Inciso 2 Artículo 210 Decreto Ley 2811 de 1974' THEN 'Inciso_Segundo'
		    WHEN sector = 'Infraestructura' THEN 'Infraestructura_Transporte'
		    WHEN sector = 'Infraestructura sanitaria y ambiental' THEN 'Infraestructura_Transporte'
		    WHEN sector = 'Infraestructura vial' THEN 'Infraestructura_Transporte'
		    WHEN sector = 'Minería' THEN 'Mineria'
		    WHEN sector = 'Reasentamiento' THEN 'Otro'
		    WHEN sector = 'Registro Áreas Urbanas, Expansion Urbana, Equipamiento' THEN 'Area_Urbana_Expansion_Rural'
		    WHEN sector = 'Restitución de tierras' THEN 'Restitucion_Tierras'
		    WHEN sector = 'Titulación / colonos' THEN 'Reforma_Agraria'
		    WHEN sector = 'Titulacion de baldíos' THEN 'Reforma_Agraria'
		    WHEN sector = 'Titulación de baldíos' THEN 'Reforma_Agraria'
		    WHEN sector = 'Titulación de baldíos/ Asuntos indigenas' THEN 'Reforma_Agraria'
		    WHEN sector = 'Titulación de baldíos/ Conservación' THEN 'Reforma_Agraria'
		    WHEN sector = 'Transmisión eléctrica' THEN 'Energia'
		    WHEN sector = 'VIS y VIP' THEN 'Vivienda_VIS_VIP'
		    ELSE 'Otro'
		end	 AS uab_tipo_sector,
		*
	FROM insumos.sustracciones_definitivas_ley2
	where nom_ley2 not like '%/%'
),sustracciones_temporal_limpieza AS (
	SELECT 
		id AS uab_identificador,
		CASE
	        WHEN homologar_texto(sustrajo) = homologar_texto('INCODER') THEN 'INCODER'
	        WHEN homologar_texto(sustrajo) = homologar_texto('INCORA') THEN 'INCORA'
	        WHEN homologar_texto(sustrajo) = homologar_texto('INDERENA') THEN 'INDERENA'
	        WHEN homologar_texto(sustrajo) = homologar_texto('MADS') THEN 'MADS'
	        WHEN homologar_texto(sustrajo) = homologar_texto('MAVDT') THEN 'MAVDT'
	        ELSE 'Otro'
	    END  AS uab_sustrajo,
		CASE
		    WHEN sector = 'Actividad Agropecuaria' THEN 'Reforma_Agraria'
		    WHEN sector = 'Adjudicación a Colonos' THEN 'Reforma_Agraria'
		    WHEN sector = 'Asuntos indígenas' THEN 'Reforma_Agraria'
		    WHEN sector = 'Base militar' THEN 'Otro'
		    WHEN sector = 'Colonización' THEN 'Reforma_Agraria'
		    WHEN sector = 'Constitución Resguardo indígena' THEN 'Otro'
		    WHEN sector = 'Creación del Distrito de Conservación de Suelos y Aguas' THEN 'Otro'
		    WHEN sector = 'Destinar tierras actividad agropecuaria' THEN 'Reforma_Agraria'
		    WHEN sector = 'Distrito de riego y asentamiento' THEN 'Otro'
		    WHEN sector = 'Distrito de riego y reasentamiento' THEN 'Otro'
		    WHEN sector = 'Distritos de conservacion' THEN 'Otro'
		    WHEN sector = 'Fines especiales zona costera' THEN 'Otro'
		    WHEN sector = 'Generación Electrica' THEN 'Energia'
		    WHEN sector = 'Generación eléctrica' THEN 'Energia'
		    WHEN sector = 'Hidrocarburos' THEN 'Hidrocarburos'
		    WHEN sector = 'Inciso 2 Artículo 210 Decreto Ley 2811 de 1974' THEN 'Inciso_Segundo'
		    WHEN sector = 'Infraestructura' THEN 'Infraestructura_Transporte'
		    WHEN sector = 'Infraestructura sanitaria y ambiental' THEN 'Infraestructura_Transporte'
		    WHEN sector = 'Infraestructura vial' THEN 'Infraestructura_Transporte'
		    WHEN sector = 'Minería' THEN 'Mineria'
		    WHEN sector = 'Reasentamiento' THEN 'Otro'
		    WHEN sector = 'Registro Áreas Urbanas, Expansion Urbana, Equipamiento' THEN 'Area_Urbana_Expansion_Rural'
		    WHEN sector = 'Restitución de tierras' THEN 'Restitucion_Tierras'
		    WHEN sector = 'Titulación / colonos' THEN 'Reforma_Agraria'
		    WHEN sector = 'Titulacion de baldíos' THEN 'Reforma_Agraria'
		    WHEN sector = 'Titulación de baldíos' THEN 'Reforma_Agraria'
		    WHEN sector = 'Titulación de baldíos/ Asuntos indigenas' THEN 'Reforma_Agraria'
		    WHEN sector = 'Titulación de baldíos/ Conservación' THEN 'Reforma_Agraria'
		    WHEN sector = 'Transmisión eléctrica' THEN 'Energia'
		    WHEN sector = 'VIS y VIP' THEN 'Vivienda_VIS_VIP'
		    ELSE 'Otro'
		END	 AS uab_tipo_sector,
		CASE
	       WHEN id = 'STL2_0080' THEN 'Resolución 0936 de 2020'
	       ELSE acto_admin
	    END  AS fuente_administrativa_nombre,
		*
	FROM insumos.sustracciones_temporales_ley2
	where nom_ley2 not like '%/%'
)
SELECT 
	id::varchar(255) AS uab_identificador,
	'Sin información'::varchar(255) uab_expediente,
	'Temporal'::varchar(255) AS uab_tipo_sustraccion,
	NULL AS uab_tipo_causal,
	uab_sustrajo,
 	case 
 		when uab_sustrajo='Otro' then sustrajo
 		else null
 	end  AS uab_detalle_sustrajo,
 	fecha_fin AS uab_fin_sustraccion,
 	uab_tipo_sector,
 	case 
 		when uab_tipo_sector='Otro' then sector
 		else null
 	end uab_detalle_sector,
 	observacio AS uab_observaciones,
 	upper(id_ley2) AS uab_areareserva,
 	nom_ley2 AS uab_nombre_areareserva,
	NULL AS uab_compensacion, 
	'rl2_uab_sustraccion' AS uab_nombre,
	ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
	'Solicitante'::varchar(255) AS interesado_tipo,
	coalesce(solicitant,'Sin información')::varchar(255) AS interesado_nombre, 
	'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado,
	NULL AS interesado_tipo_documento,
	NULL AS interesado_numero_documento,
	'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
	'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad, 
	'Documento'::varchar(255) AS fuente_administrativa_tipo_formato, 
	fecha_acto AS fuente_administrativa_fecha_documento_fuente, 
	fuente_administrativa_nombre::varchar(255) AS fuente_administrativa_nombre,
	null AS ddr_tipo_resposabilidad,
	'Sustraer'::varchar(255) as ddr_tipo_derecho
FROM sustracciones_temporal_limpieza
UNION
SELECT 
	uab_identificador,
	'Sin información'::varchar(255) uab_expediente,
	'Definitiva'::varchar(255) AS uab_tipo_sustraccion,
	NULL AS uab_tipo_causal,
 	uab_sustrajo,
 	case 
 		when uab_sustrajo='Otro' then sustrajo
 		else null
 	end  AS uab_detalle_sustrajo,
 	null AS uab_fin_sustraccion,
 	uab_tipo_sector,
 	case 
 		when uab_tipo_sector='Otro' then sector
 		else null
 	end uab_detalle_sector,
 	observacio AS uab_observaciones,
 	upper(id_ley2) AS uab_areareserva,
 	nom_ley2 AS uab_nombre_areareserva,
 	NULL AS uab_compensacion, 
	'rl2_uab_sustraccion' AS uab_nombre,
	ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
	'Solicitante'::varchar(255) AS interesado_tipo,
	coalesce(solicitant,'Sin información')::varchar(255) AS interesado_nombre, 
	'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado,
	NULL AS interesado_tipo_documento,
	NULL AS interesado_numero_documento, 
	'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
	'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad, 
	'Documento'::varchar(255) AS fuente_administrativa_tipo_formato, 
	fecha_acto AS fuente_administrativa_fecha_documento_fuente, 
	acto_admin::varchar(255) AS fuente_administrativa_nombre,
	null AS ddr_tipo_resposabilidad,
	'Sustraer'::varchar(255) as ddr_tipo_derecho
FROM sustracciones_definitivas_limpieza;


--========================================
-- Fuentes Administrativas de Áreas de Reserva
--========================================
INSERT INTO estructura_intermedia.rl2_fuenteadministrativa(
	uab_identificador, 
	fuente_administrativa_fecha_documento_fuente, 
	fuente_administrativa_tipo_formato, 
	fuente_administrativa_numero, 
	fuente_administrativa_anio
)
WITH areareserva_acto AS (
	SELECT uab_identificador,
	       fuente_administrativa_fecha_documento_fuente,
	       substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1) AS fuente_administrativa_tipo,
	       substring(cl.fuente_administrativa_nombre, position(' ' IN cl.fuente_administrativa_nombre) + 1, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)) AS numero_acto,
	       substring(cl.fuente_administrativa_nombre, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4) AS anio_acto
	FROM estructura_intermedia.rl2_uab_areareserva cl
)
SELECT uab_identificador,
       fuente_administrativa_fecha_documento_fuente,
       CASE 
		    WHEN fuente_administrativa_tipo= 'Resolución' THEN 'Resolucion'
		    ELSE fuente_administrativa_tipo
		END AS fuente_administrativa_tipo,
       estructura_intermedia.homologar_numero(numero_acto::text)::int4 AS fuente_administrativa_numero,
       estructura_intermedia.homologar_numero(anio_acto::text)::int4 AS fuente_administrativa_anio
FROM areareserva_acto;

--========================================
-- Fuentes Administrativas de Zonificación
--========================================
INSERT INTO estructura_intermedia.rl2_fuenteadministrativa(
	uab_identificador, 
	fuente_administrativa_fecha_documento_fuente, 
	fuente_administrativa_tipo_formato, 
	fuente_administrativa_numero, 
	fuente_administrativa_anio
)
WITH zonificacion_acto AS (
	SELECT uab_identificador,
	       fuente_administrativa_fecha_documento_fuente,
	       substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1) AS tipo_acto,
	       substring(cl.fuente_administrativa_nombre, position(' ' IN cl.fuente_administrativa_nombre) + 1, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)) AS numero_acto,
	       substring(cl.fuente_administrativa_nombre, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4) AS anio_acto
	FROM estructura_intermedia.rl2_uab_zonificacion cl
)
SELECT uab_identificador,
       fuente_administrativa_fecha_documento_fuente,
       CASE 
           WHEN tipo_acto = 'Resolución' THEN 'Resolucion'
           ELSE tipo_acto
       END AS fuente_administrativa_tipo,
       estructura_intermedia.homologar_numero(numero_acto::text)::int4 AS fuente_administrativa_acto,
       estructura_intermedia.homologar_numero(anio_acto::text)::int4 AS fuente_administrativa_anio
FROM zonificacion_acto;

--========================================
-- Fuentes Administrativas de Sustracciones
--========================================
INSERT INTO estructura_intermedia.rl2_fuenteadministrativa(
	uab_identificador, 
	fuente_administrativa_fecha_documento_fuente, 
	fuente_administrativa_tipo_formato, 
	fuente_administrativa_numero, 
	fuente_administrativa_anio
)
WITH split_acto_sustraccion AS (
	SELECT 
		uab_identificador,
		fuente_administrativa_fecha_documento_fuente,
		cl.fuente_administrativa_nombre AS acto,
		trim(unnest(string_to_array(cl.fuente_administrativa_nombre, '/'))) AS fuente_administrativa_nombre
	FROM estructura_intermedia.rl2_uab_sustraccion cl   
), homologado_acto_sustraccion AS (
	SELECT 
		uab_identificador,
		acto,
		fuente_administrativa_fecha_documento_fuente,
		CASE 
			WHEN fuente_administrativa_nombre LIKE 'Resolución 41 de 1964 (R. 199 de 1964)' THEN 'Resolución 41 de 1964'	    
			WHEN fuente_administrativa_nombre LIKE 'Res 400_2' THEN 'Resolución 400 de 2017'
			WHEN fuente_administrativa_nombre LIKE 'Res_1526' THEN 'Resolución 1526 de 2017'
			WHEN fuente_administrativa_nombre LIKE 'Res1246_17' THEN 'Resolución 1246 de 2017'
			WHEN fuente_administrativa_nombre LIKE 'Res 1833_2017' THEN 'Resolución 1833 de 2017'
			WHEN fuente_administrativa_nombre LIKE 'Resolución 1743 2014' THEN 'Resolución 1743 de 2014'
			WHEN fuente_administrativa_nombre LIKE 'Res_563_2013' THEN 'Resolución 563 de 2013'
			WHEN fuente_administrativa_nombre LIKE 'Res_503 _2016' THEN 'Resolución 503 de 2016'
			WHEN fuente_administrativa_nombre LIKE 'Resolución 282  2014' THEN 'Resolución 282 de 2014'
			WHEN fuente_administrativa_nombre LIKE 'Res 536 dfe 2019' THEN 'Resolución 536 de 2019'
			WHEN fuente_administrativa_nombre LIKE 'Res 1833_17_' THEN 'Resolución 1833 de 2017'
			WHEN fuente_administrativa_nombre LIKE 'Res 1526_12' THEN 'Resolución 1526 de 2012'
			WHEN fuente_administrativa_nombre LIKE 'Resolución 1085' THEN 'Resolución 1085 de 2012'
			ELSE fuente_administrativa_nombre
		END AS fuente_administrativa_nombre
	FROM split_acto_sustraccion
), sustraccion_acto AS (
	SELECT 
		uab_identificador,
		acto,
		fuente_administrativa_fecha_documento_fuente,
		substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1) AS tipo_acto,
		substring(cl.fuente_administrativa_nombre, position(' ' IN cl.fuente_administrativa_nombre) + 1, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)) AS numero_acto,
		substring(cl.fuente_administrativa_nombre, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4) AS anio_acto
	FROM homologado_acto_sustraccion cl  
)
SELECT 
	uab_identificador,
	fuente_administrativa_fecha_documento_fuente,
	CASE 
		WHEN tipo_acto = 'Resolucíon' THEN 'Resolucion'
		WHEN tipo_acto = 'Resolución' THEN 'Resolucion'
		WHEN tipo_acto = 'Resoluciión' THEN 'Resolucion'
		WHEN tipo_acto = 'Res' THEN 'Resolucion'
		ELSE tipo_acto
	END AS fuente_administrativa_tipo,
	estructura_intermedia.homologar_numero(numero_acto::text)::int4 AS fuente_administrativa_numero,
	estructura_intermedia.homologar_numero(anio_acto::text)::int4 AS fuente_administrativa_anio
FROM sustraccion_acto;


--========================================
-- Fuentes Administrativas de Compensaciones
--========================================
INSERT INTO estructura_intermedia.rl2_fuenteadministrativa(
	uab_identificador, 
	fuente_administrativa_fecha_documento_fuente, 
	fuente_administrativa_tipo_formato, 
	fuente_administrativa_numero, 
	fuente_administrativa_anio
)
WITH clean_acto_compensacion AS (
	SELECT 
		uab_identificador,
		uab_areareserva,
		fuente_administrativa_fecha_documento_fuente,
		CASE 	    	
			WHEN fuente_administrativa_nombre LIKE 'Resolucion 033, 350, 1021, 1156, de 2009' 
			THEN 'Resolucion 033 de 2009/Resolucion 350 de 2009/Resolucion 1021 de 2009/Resolucion 1156 de 2009'
			WHEN fuente_administrativa_nombre LIKE 'Resolucion 624 y 1346 de 2009'
			THEN 'Resolucion 624 de 2009/Resolucion 1346 de 2009'
			ELSE fuente_administrativa_nombre
		END AS fuente_administrativa_nombre
	FROM estructura_intermedia.rl2_uab_compensacion cl 
), split_acto_compensacion AS (
	SELECT 
		uab_identificador,
		uab_areareserva,
		fuente_administrativa_fecha_documento_fuente,
		cl.fuente_administrativa_nombre AS acto,
		trim(unnest(string_to_array(cl.fuente_administrativa_nombre, '/'))) AS fuente_administrativa_nombre
	FROM clean_acto_compensacion cl 
), compensacion_acto AS (
	SELECT 
		uab_identificador,
		fuente_administrativa_fecha_documento_fuente,
		substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1) AS fuente_administrativa_tipo,
		substring(cl.fuente_administrativa_nombre, position(' ' IN cl.fuente_administrativa_nombre) + 1, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)) AS fuente_administrativa_numero,
		substring(cl.fuente_administrativa_nombre, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4) AS fuente_administrativa_anio
	FROM split_acto_compensacion cl
	WHERE left(uab_areareserva, 4) = 'LEY2' 
)
SELECT 
	uab_identificador,
	fuente_administrativa_fecha_documento_fuente,
	CASE 
	    WHEN fuente_administrativa_tipo= 'Resolución' THEN 'Resolucion'
	    ELSE fuente_administrativa_tipo
	END AS fuente_administrativa_tipo,
	estructura_intermedia.homologar_numero(fuente_administrativa_numero::text)::int4 AS fuente_administrativa_numero,
	estructura_intermedia.homologar_numero(fuente_administrativa_anio::text)::int4 AS fuente_administrativa_anio
FROM compensacion_acto;

--========================================
-- Relacionar Compensaciones a las Sustracciones
--========================================
WITH fuente_s AS (
	SELECT *
	FROM estructura_intermedia.rl2_fuenteadministrativa
	WHERE left(uab_identificador, 1) = 'S'
), fuente_c AS (
	SELECT *
	FROM estructura_intermedia.rl2_fuenteadministrativa
	WHERE left(uab_identificador, 1) = 'C'
), fuente_sustraccion_compensacion as (
	SELECT DISTINCT 
		s.uab_identificador AS uab_sustraccion,
		c.uab_identificador AS uab_compensacion
	FROM fuente_c c  
	LEFT JOIN fuente_s s
		ON s.fuente_administrativa_tipo_formato = c.fuente_administrativa_tipo_formato
		AND s.fuente_administrativa_numero = c.fuente_administrativa_numero
		AND s.fuente_administrativa_anio = c.fuente_administrativa_anio
	WHERE s.uab_identificador IS NOT null
), asigna_expendiente_sustraccion as (
	select c.*,uc.uab_expediente
	from fuente_sustraccion_compensacion c
	left join estructura_intermedia.rl2_uab_compensacion uc
	on c.uab_compensacion=uc.uab_identificador
)
update estructura_intermedia.rl2_uab_sustraccion
set uab_compensacion=sc.uab_compensacion,
uab_expediente=sc.uab_expediente
from asigna_expendiente_sustraccion sc
where sc.uab_sustraccion=rl2_uab_sustraccion.uab_identificador;

--========================================
-- Relacionar Sustracciones a las Compensaciones
--========================================
WITH fuente_s AS (
	SELECT *
	FROM estructura_intermedia.rl2_fuenteadministrativa
	WHERE left(uab_identificador, 1) = 'S'
), fuente_c AS (
	SELECT *
	FROM estructura_intermedia.rl2_fuenteadministrativa
	WHERE left(uab_identificador, 1) = 'C'
), fuente_sustraccion_compensacion as (
	SELECT DISTINCT 
		s.uab_identificador AS uab_sustraccion,
		c.uab_identificador AS uab_compensacion
	FROM  fuente_s  s
	LEFT JOIN fuente_c c
		ON s.fuente_administrativa_tipo_formato = c.fuente_administrativa_tipo_formato
		AND s.fuente_administrativa_numero = c.fuente_administrativa_numero
		AND s.fuente_administrativa_anio = c.fuente_administrativa_anio
	WHERE s.uab_identificador IS NOT null
), asigna_solicitante_compensacion as (
	select c.*,s.interesado_nombre
	from fuente_sustraccion_compensacion c
	left join estructura_intermedia.rl2_uab_sustraccion s 
	on c.uab_sustraccion=s.uab_identificador
)
update estructura_intermedia.rl2_uab_compensacion
set uab_sustraccion=sc.uab_sustraccion,
interesado_nombre=sc.interesado_nombre
from asigna_solicitante_compensacion sc
where sc.uab_compensacion=rl2_uab_compensacion.uab_identificador;











