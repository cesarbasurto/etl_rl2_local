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
-- Se eliminan sustraciones que tienen relacionada dos o mas compensaciones, se relacionan en la Bitacora
--========================================
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












