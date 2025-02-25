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

SELECT *
FROM estructura_intermedia.rl2_uab_areareserva;

SELECT count(*)
FROM estructura_intermedia.rl2_uab_areareserva;
--7 area de Reserva

select fuente_administrativa_tipo_formato,count(*)
from estructura_intermedia.rl2_fuenteadministrativa
where left(uab_identificador,1) ='L'
group by fuente_administrativa_tipo_formato;

--========================================
-- Zonificación Ley 2
--========================================
select * 
FROM estructura_intermedia.rl2_uab_zonificacion;

select count(*) 
FROM estructura_intermedia.rl2_uab_zonificacion;

select uab_areareserva,count(*) 
FROM estructura_intermedia.rl2_uab_zonificacion
group by uab_areareserva
order by uab_areareserva;

select uab_tipo_zona,count(*) 
FROM estructura_intermedia.rl2_uab_zonificacion
group by uab_tipo_zona
order by uab_tipo_zona;

select uab_areareserva,uab_tipo_zona,count(*) 
FROM estructura_intermedia.rl2_uab_zonificacion
group by uab_areareserva,uab_tipo_zona
order by uab_areareserva,uab_tipo_zona;

select fuente_administrativa_tipo_formato,count(*)
from estructura_intermedia.rl2_fuenteadministrativa
where left(uab_identificador,1) ='Z'
group by fuente_administrativa_tipo_formato;

--========================================
-- Compensación Ley 
--========================================
SELECT *		
FROM estructura_intermedia.rl2_uab_compensacion;

select count(*) 
FROM estructura_intermedia.rl2_uab_compensacion;

select uab_areareserva,count(*) 
FROM estructura_intermedia.rl2_uab_compensacion
group by uab_areareserva
order by uab_areareserva;


select uab_sustraccion,count(*)
FROM estructura_intermedia.rl2_uab_compensacion
group by uab_sustraccion
having count(*)>1;

select *
from estructura_intermedia.rl2_uab_compensacion
where uab_sustraccion is null;

select *
from estructura_intermedia.rl2_uab_compensacion
where uab_sustraccion ='SDL2_189';


select fuente_administrativa_tipo_formato,count(*)
from estructura_intermedia.rl2_fuenteadministrativa
where left(uab_identificador,1) ='C'
group by fuente_administrativa_tipo_formato;

--========================================
-- Se listan Compensaciones que no tienen relacionada sustraccion, se relacionan en la Bitacora
--========================================
select *
from estructura_intermedia.rl2_uab_compensacion
where uab_sustraccion is null;



--========================================
-- Sustracción Ley 2
--========================================
SELECT *		
FROM estructura_intermedia.rl2_uab_sustraccion;

select count(*) 
FROM estructura_intermedia.rl2_uab_sustraccion;

select uab_tipo_sustraccion,count(*) 
FROM estructura_intermedia.rl2_uab_sustraccion
group by uab_tipo_sustraccion;

select uab_areareserva,count(*) 
FROM estructura_intermedia.rl2_uab_sustraccion
group by uab_areareserva
order by uab_areareserva;
--3 sustraciones con dos areas de reserva




--========================================
-- Se eliminan sustraciones que tienen relacionada dos o mas compensaciones, se relacionan en la Bitacora
--========================================
select *
from estructura_intermedia.rl2_uab_sustraccion
where uab_compensacion in (
	select uab_compensacion
	from estructura_intermedia.rl2_uab_sustraccion
	where uab_compensacion is not null
	group by uab_compensacion
	having count(*)>1
);

delete
from estructura_intermedia.rl2_uab_sustraccion
where uab_compensacion in (
	select uab_compensacion
	from estructura_intermedia.rl2_uab_sustraccion
	where uab_compensacion is not null
	group by uab_compensacion
	having count(*)>1
);

--========================================
-- Eliminan  Compensaciones que no tienen relacionada sustraccion, se relacionan en la Bitacora
--========================================
select *
from estructura_intermedia.rl2_uab_compensacion
where uab_sustraccion not in(
	select uab_identificador
	from estructura_intermedia.rl2_uab_sustraccion 
) 
or uab_sustraccion is null 
or uab_sustraccion  in(
	select uab_sustraccion
	from estructura_intermedia.rl2_uab_compensacion
	group by uab_sustraccion
	having count(*)>1
);

delete 
from estructura_intermedia.rl2_uab_compensacion
where uab_sustraccion not in(
	select uab_identificador
	from estructura_intermedia.rl2_uab_sustraccion 
) 
or uab_sustraccion is null 
or uab_sustraccion  in(
	select uab_sustraccion
	from estructura_intermedia.rl2_uab_compensacion
	group by uab_sustraccion
	having count(*)>1
);












