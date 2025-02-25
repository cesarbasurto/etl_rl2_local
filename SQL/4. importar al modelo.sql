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

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'arfw_etl_rl2' AND pid <> pg_backend_pid();

DROP database arfw_etl_rl2 ;

--========================================
--Fijar esquema
--========================================
set search_path to
	estructura_intermedia,	-- Esquema de estructura de datos intermedia
	ladm,		-- Esquema modelo LADM-LEY2
	public;


--================================================================================
-- 1.Define Basket si este no existe
--================================================================================
INSERT INTO ladm.t_ili2db_dataset
(t_id, datasetname)
VALUES(1, 'Baseset');

INSERT INTO ladm.t_ili2db_basket(
	t_id, 
	dataset, 
	topic, 
	t_ili_tid, 
	attachmentkey, 
	domains)
VALUES(
	1,
	1, 
	'LADM_COL_v_1_0_0_Ext_RL2.RL2',
	uuid_generate_v4(),
	'ETL de importación de datos',
	NULL );

--================================================================================
-- 2. Migración de rl2_interesado
--================================================================================
INSERT INTO ladm.rl2_interesado(
	t_basket, 
	t_ili_tid, 
	tipo, 
	observacion, 
	nombre, 
	tipo_interesado, 
	tipo_documento, 
	numero_documento, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)		
select 
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.rl2_interesadotipo where ilicode like interesado_tipo) as tipo,
	null as observacion,
	interesado_nombre, 
	(select t_id from ladm.col_interesadotipo where ilicode like interesado_tipo_interesado) as tipo_interesado,
	(select t_id from ladm.col_documentotipo where ilicode like interesado_tipo_documento) as interesado_tipo_documento,
	interesado_numero_documento,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_interesado' as espacio_de_nombres, 
	row_number() OVER (ORDER BY interesado_nombre)   local_id
from (
	select 
		interesado_tipo,
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rl2_uab_areareserva
	union
	select 
		interesado_tipo,
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rl2_uab_sustraccion
	union
	select 
		interesado_tipo,
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rl2_uab_zonificacion
	union
	select 
		interesado_tipo,
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rl2_uab_compensacion
) t;

--================================================================================
-- 3. Migración de  Area de Reserva
--================================================================================
--3.1 diligenciamiento de la tabla  rl2_uab_areareserva
INSERT INTO ladm.rl2_uab_areareserva(
	t_basket, 
	t_ili_tid, 
	nombre_reserva, 
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)	
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	uab_nombre_reserva,
	uab_nombre,
	(select t_id from ladm.col_unidadadministrativabasicatipo where ilicode like 'Ambiente_Desarrollo_Sostenible') as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_uab_areareserva' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rl2_uab_areareserva; 

--3.2 diligenciamiento de la tabla  rl2_ue_areareserva
INSERT INTO ladm.rl2_ue_areareserva(
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	uab_nombre_reserva as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_ue_areareserva' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rl2_uab_areareserva; 

--3.3 diligenciamiento de la tabla  rl2_derecho para rl2_uab_areareserva
INSERT INTO ladm.rl2_derecho(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rl2_interesado, 
	interesado_rl2_agrupacioninteresados, 
	unidad_rl2_uab_compensacion, 
	unidad_rl2_uab_sustraccion, 
	unidad_rl2_uab_zonificacion, 
	unidad_rl2_uab_areareserva, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)	
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.rl2_derechotipo where ilicode like ddr_tipo_derecho) as tipo,
	null as descripcion,
	(select t_id from ladm.rl2_interesado where nombre like interesado_nombre)  as interesado_rl2_interesado, 
	null as interesado_rl2_agrupacioninteresados, 
	null as unidad_rl2_uab_compensacion, 
	null as unidad_rl2_uab_sustraccion, 
	null as unidad_rl2_uab_zonificacion, 
	(select t_id from ladm.rl2_uab_areareserva where local_id like uab_identificador) as unidad_rl2_uab_areareserva,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_derecho' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rl2_uab_areareserva; 

--3.4 diligenciamiento de la tabla rl2_fuenteadministrativa para rl2_uab_areareserva
INSERT INTO ladm.rl2_fuenteadministrativa(
	t_basket, 
	t_ili_tid, 
	tipo, 
	fecha_fin, 
	estado_disponibilidad, 
	tipo_formato, 
	fecha_documento_fuente, 
	nombre, 
	descripcion, 
	url, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.col_fuenteadministrativatipo cf where ilicode like a.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato) as tipo,
	null as fecha_fin, 
	(select t_id from ladm.col_estadodisponibilidadtipo ce  where ilicode like a.fuente_administrativa_estado_disponibilidad) as estado_disponibilidad,
	(select t_id from ladm.col_formatotipo cf  where ilicode like a.fuente_administrativa_tipo_formato) as tipo_formato,
	a.fuente_administrativa_fecha_documento_fuente  fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rl2_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador  local_id
from estructura_intermedia.rl2_fuenteadministrativa f,estructura_intermedia.rl2_uab_areareserva a
where f.uab_identificador =a.uab_identificador;

--================================================================================
-- 4. Migración de  zonificacion
--================================================================================
INSERT INTO ladm.rl2_uab_zonificacion(
	t_basket, 
	t_ili_tid, 
	tipo_zona, 
	uab_areareserva, 
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.rl2_zonatipo where ilicode like z.uab_tipo_zona) as uab_tipo_zona,
	(select t_id from ladm.rl2_uab_areareserva where nombre_reserva in (select a.uab_nombre_reserva from estructura_intermedia.rl2_uab_areareserva a where a.uab_identificador=z.uab_areareserva)) as tipo,
	uab_nombre,
	(select t_id from ladm.col_unidadadministrativabasicatipo where ilicode like 'Ambiente_Desarrollo_Sostenible') as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_uab_zonificacion' as espacio_de_nombres, 
	z.uab_identificador as local_id
from  estructura_intermedia.rl2_uab_zonificacion z; 

--4.2 diligenciamiento de la tabla  rl2_ue_zonificacion
INSERT INTO ladm.rl2_ue_zonificacion (
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	null as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_ue_zonificacion' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rl2_uab_zonificacion; 

--4.3 diligenciamiento de la tabla  rl2_responsabilidad para rl2_uab_zonificacion
INSERT INTO ladm.rl2_responsabilidad(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rl2_interesado, 
	interesado_rl2_agrupacioninteresados, 
	unidad_rl2_uab_compensacion, 
	unidad_rl2_uab_sustraccion, 
	unidad_rl2_uab_zonificacion, 
	unidad_rl2_uab_areareserva, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.rl2_responsabilidadtipo  where ilicode like ddr_tipo_resposabilidad) as tipo,
	null as descripcion,
	(select t_id from ladm.rl2_interesado where nombre like interesado_nombre)  as interesado_rl2_interesado, 
	null as interesado_rl2_agrupacioninteresados, 
	null as unidad_rl2_uab_compensacion, 
	null as unidad_rl2_uab_sustraccion, 
	(select t_id from ladm.rl2_uab_zonificacion where local_id like uab_identificador) as unidad_rl2_uab_zonificacion, 
	null as unidad_rl2_uab_areareserva,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_responsabilidad' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rl2_uab_zonificacion; 

--4.4 diligenciamiento de la tabla rl2_fuenteadministrativa para rl2_uab_areareserva
INSERT INTO ladm.rl2_fuenteadministrativa(
	t_basket, 
	t_ili_tid, 
	tipo, 
	fecha_fin, 
	estado_disponibilidad, 
	tipo_formato, 
	fecha_documento_fuente, 
	nombre, 
	descripcion, 
	url, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.col_fuenteadministrativatipo cf where ilicode like z.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato) as tipo,
	null as fecha_fin, 
	(select t_id from ladm.col_estadodisponibilidadtipo ce  where ilicode like z.fuente_administrativa_estado_disponibilidad) as estado_disponibilidad,
	(select t_id from ladm.col_formatotipo cf  where ilicode like z.fuente_administrativa_tipo_formato) as tipo_formato,
	z.fuente_administrativa_fecha_documento_fuente  fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rl2_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador  local_id
from estructura_intermedia.rl2_fuenteadministrativa f,estructura_intermedia.rl2_uab_zonificacion z
where f.uab_identificador =z.uab_identificador;


--================================================================================
-- 5. Migración de  compensacion
--================================================================================
INSERT INTO ladm.rl2_uab_compensacion(
	t_basket, 
	t_ili_tid, 
	expediente, 
	observaciones, 
	uab_areareserva, 
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version,
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	c.uab_expediente,
	c.uab_observaciones,
	(select t_id from ladm.rl2_uab_areareserva where nombre_reserva in (select a.uab_nombre_reserva from estructura_intermedia.rl2_uab_areareserva a where a.uab_identificador=c.uab_areareserva)) as tipo,
	uab_nombre,
	(select t_id from ladm.col_unidadadministrativabasicatipo where ilicode like 'Ambiente_Desarrollo_Sostenible') as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_uab_compensacion' as espacio_de_nombres, 
	c.uab_identificador as local_id
from  estructura_intermedia.rl2_uab_compensacion c;	

--5.2 diligenciamiento de la tabla  rl2_uab_compensacion
INSERT INTO ladm.rl2_ue_compensacion (
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	null as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_ue_compensacion' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rl2_uab_compensacion; 

--5.3 diligenciamiento de la tabla  rl2_responsabilidad para rl2_uab_compensacion
INSERT INTO ladm.rl2_responsabilidad(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rl2_interesado, 
	interesado_rl2_agrupacioninteresados, 
	unidad_rl2_uab_compensacion, 
	unidad_rl2_uab_sustraccion, 
	unidad_rl2_uab_zonificacion, 
	unidad_rl2_uab_areareserva, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.rl2_responsabilidadtipo where ilicode like ddr_tipo_resposabilidad) as tipo,
	null as descripcion,
	(select t_id from ladm.rl2_interesado where nombre like interesado_nombre)  as interesado_rl2_interesado, 
	null as interesado_rl2_agrupacioninteresados, 
	(select t_id from ladm.rl2_uab_compensacion where local_id like uab_identificador) as unidad_rl2_uab_compensacion, 
	null as unidad_rl2_uab_sustraccion, 
	null as unidad_rl2_uab_zonificacion, 
	null as unidad_rl2_uab_areareserva,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_responsabilidad' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rl2_uab_compensacion; 

--5.4 diligenciamiento de la tabla rl2_fuenteadministrativa para rl2_uab_areareserva
INSERT INTO ladm.rl2_fuenteadministrativa(
	t_basket, 
	t_ili_tid, 
	tipo, 
	fecha_fin, 
	estado_disponibilidad, 
	tipo_formato, 
	fecha_documento_fuente, 
	nombre, 
	descripcion, 
	url, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.col_fuenteadministrativatipo cf where ilicode like c.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato) as tipo,
	null as fecha_fin, 
	(select t_id from ladm.col_estadodisponibilidadtipo ce  where ilicode like c.fuente_administrativa_estado_disponibilidad) as estado_disponibilidad,
	(select t_id from ladm.col_formatotipo cf  where ilicode like c.fuente_administrativa_tipo_formato) as tipo_formato,
	c.fuente_administrativa_fecha_documento_fuente  fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rl2_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador  local_id
from estructura_intermedia.rl2_fuenteadministrativa f,estructura_intermedia.rl2_uab_compensacion c
where f.uab_identificador =c.uab_identificador;

--================================================================================
-- 6. Migración de sustraccion
--================================================================================
INSERT INTO ladm.rl2_uab_sustraccion(
	t_basket, 
	t_ili_tid, 
	expediente,
	tipo_sustraccion, 
	tipo_causal, 
	sustrajo, 
	detalle_sustrajo, 
	fin_sustraccion, 
	tipo_sector, 
	detalle_sector, 
	observaciones, 
	uab_areareserva,
	uab_compensacion, 
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	s.uab_expediente,
	(select t_id from ladm.rl2_sustraccionreservatipo where ilicode like s.uab_tipo_sustraccion) as tipo_sustraccion,
	(select t_id from ladm.rl2_causalsustracciontipo where ilicode like s.uab_tipo_causal) as tipo_causal,
	(select t_id from ladm.rl2_sustrajotipo where ilicode like s.uab_sustrajo) as sustrajo,
	s.uab_detalle_sustrajo,
	s.uab_fin_sustraccion,
	(select t_id from ladm.rl2_sectortipo where ilicode like s.uab_tipo_sector) as tipo_sector,
	s.uab_detalle_sector,
	s.uab_observaciones,
	(select t_id from ladm.rl2_uab_areareserva where local_id like s.uab_areareserva ) as uab_areareserva,
	(select t_id from ladm.rl2_uab_compensacion where local_id like s.uab_compensacion ) as uab_compensacion,
	uab_nombre,
	(select t_id from ladm.col_unidadadministrativabasicatipo where ilicode like 'Ambiente_Desarrollo_Sostenible') as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_uab_sustraccion' as espacio_de_nombres, 
	s.uab_identificador as local_id
from  estructura_intermedia.rl2_uab_sustraccion s;	

--6.2 diligenciamiento de la tabla  rl2_ue_sustraccion
INSERT INTO ladm.rl2_ue_sustraccion (
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	null as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_ue_sustraccion' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rl2_uab_sustraccion ; 

--6.3 diligenciamiento de la tabla  rl2_responsabilidad para rl2_uab_sustraccion
INSERT INTO ladm.rl2_derecho(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rl2_interesado, 
	interesado_rl2_agrupacioninteresados, 
	unidad_rl2_uab_compensacion, 
	unidad_rl2_uab_sustraccion, 
	unidad_rl2_uab_zonificacion, 
	unidad_rl2_uab_areareserva, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,	
	(select t_id from ladm.rl2_derechotipo rd where ilicode like ddr_tipo_derecho) as tipo,
	null as descripcion,
	(select t_id from ladm.rl2_interesado where nombre like interesado_nombre)  as interesado_rl2_interesado,
	null as interesado_rl2_agrupacioninteresados, 
	null as unidad_rl2_uab_compensacion, 
	(select t_id from ladm.rl2_uab_sustraccion where local_id like uab_identificador) as unidad_rl2_uab_sustraccion, 
	null as unidad_rl2_uab_zonificacion, 
	null as unidad_rl2_uab_areareserva,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_derecho' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rl2_uab_sustraccion ; 

--6.4 diligenciamiento de la tabla rl2_fuenteadministrativa para rl2_uab_areareserva
INSERT INTO ladm.rl2_fuenteadministrativa(
	t_basket, 
	t_ili_tid, 
	tipo, 
	fecha_fin, 
	estado_disponibilidad, 
	tipo_formato, 
	fecha_documento_fuente, 
	nombre, 
	descripcion, 
	url, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.col_fuenteadministrativatipo cf where ilicode like s.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato) as tipo,
	null as fecha_fin, 
	(select t_id from ladm.col_estadodisponibilidadtipo ce  where ilicode like s.fuente_administrativa_estado_disponibilidad) as estado_disponibilidad,
	(select t_id from ladm.col_formatotipo cf  where ilicode like s.fuente_administrativa_tipo_formato) as tipo_formato,
	s.fuente_administrativa_fecha_documento_fuente  fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rl2_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador  local_id
from estructura_intermedia.rl2_fuenteadministrativa f,estructura_intermedia.rl2_uab_sustraccion s
where f.uab_identificador =s.uab_identificador;


--================================================================================
-- 7. Migración de col_rrrfuente
--================================================================================
INSERT INTO ladm.col_rrrfuente(
	t_basket, 
	fuente_administrativa, 
	rrr_rl2_derecho, 
	rrr_rl2_responsabilidad)
select
(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
*
from (
	select distinct 
	f.t_id as fuente_administrativa,
	null::int8 as rrr_rl2_derecho,
	r.t_id as rrr_rl2_responsabilidad
	from ladm.rl2_fuenteadministrativa f,
	ladm.rl2_responsabilidad r
	where f.local_id=r.local_id 
	union  
	select distinct 
	f.t_id as fuente_administrativa,
	d.t_id as rrr_rl2_derecho,
	null::int4 as rrr_rl2_responsabilidad
	from ladm.rl2_fuenteadministrativa f,
	ladm.rl2_derecho d
	where f.local_id=d.local_id
) t;

--================================================================================
-- 8. Migración de col_uebaunit
--================================================================================
INSERT INTO ladm.col_uebaunit(
	t_basket, 
	ue_rl2_ue_zonificacion, 
	ue_rl2_ue_compensacion, 
	ue_rl2_ue_sustraccion, 
	ue_rl2_ue_areareserva, 
	baunit_rl2_uab_zonificacion,
	baunit_rl2_uab_compensacion, 
	baunit_rl2_uab_sustraccion, 	 
	baunit_rl2_uab_areareserva)
select
(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1) as t_basket,
*
from (
	select uez.t_id::int8 as ue_rl2_ue_zonificacion,
	null::int8 as ue_rl2_ue_compensacion,
	null::int8 as ue_rl2_ue_sustraccion,
	null::int8 as ue_rl2_ue_areareserva,
	uabz.t_id::int8 as baunit_rl2_uab_zonificacion,
	null::int8 as baunit_rl2_uab_compensacion,
	null::int8 as baunit_rl2_uab_sustraccion,
	null::int8 as baunit_rl2_uab_areareserva
	from ladm.rl2_ue_zonificacion uez,
	ladm.rl2_uab_zonificacion uabz
	where uez.local_id=uabz.local_id
	union	
	select null as ue_rl2_ue_zonificacion,
	uez.t_id as ue_rl2_ue_compensacion,
	null as ue_rl2_ue_sustraccion,
	null as ue_rl2_ue_areareserva,
	null as baunit_rl2_uab_zonificacion,
	uabz.t_id as baunit_rl2_uab_compensacion,
	null as baunit_rl2_uab_sustraccion,
	null as baunit_rl2_uab_areareserva
	from ladm.rl2_ue_compensacion uez,
	ladm.rl2_uab_compensacion uabz
	where uez.local_id=uabz.local_id
	union  
	select null as ue_rl2_ue_zonificacion,
	null as ue_rl2_ue_compensacion,
	uez.t_id as ue_rl2_ue_sustraccion,
	null as ue_rl2_ue_areareserva,
	null as baunit_rl2_uab_zonificacion,
	null as baunit_rl2_uab_compensacion,
	uabz.t_id as baunit_rl2_uab_sustraccion,
	null as baunit_rl2_uab_areareserva
	from ladm.rl2_ue_sustraccion uez,
	ladm.rl2_uab_sustraccion uabz
	where uez.local_id=uabz.local_id
	union  
	select null as ue_rl2_ue_zonificacion,
	null as ue_rl2_ue_compensacion,
	null as ue_rl2_ue_sustraccion,
	uez.t_id as ue_rl2_ue_areareserva,
	null as baunit_rl2_uab_zonificacion,
	null as baunit_rl2_uab_compensacion,
	null as baunit_rl2_uab_sustraccion,
	uabz.t_id as baunit_rl2_uab_areareserva
	from ladm.rl2_ue_areareserva uez,
	ladm.rl2_uab_areareserva uabz
	where uez.local_id=uabz.local_id
) t;








