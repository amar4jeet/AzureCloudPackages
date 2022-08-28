/*
-------------------------------------------------------------------------------
-- Program TMP_DOMS_SPA_MV_DIST_SUBSTATION
-------------------------------------------------------------------------------
-- Purpose:  
--   Load data into TMP_DOMS_SPA_MV_DIST_SUBSTATION from rawvalut.
--
-- Various attributes SUBSTATION_ID
-------------------------------------------------------------------------------
-- Date       | Author                        | Description
-------------------------------------------------------------------------------
-- 2022-07-15 |  Amarjeet Kumar               | Initial Version
--            |                               |
--            |                               |
--            |                               |
-------------------------------------------------------------------------------
*/

select 
  SPA.EXTRACT_DATE_TIME,
  hub.SUBSTATION_ID, 
  SPA.ALIAS, 
  SPA.SUB_NAME, 
  SPA.CHANGE_TYPE, 
  SPA.HUB_SUBSTATION_HSH_KEY, 
  SPA.LOAD_DATE_TIME, 
  SPA.RECORD_SOURCE, 
  SPA.SAT_DOMS_SPA_MV_DIST_SUBSTATION_HSH_DIFF 
from 
  (
    select 
      sat.*, 
      ROW_NUMBER() OVER (
        partition BY sat.ALIAS
        ORDER BY 
          sat.extract_date_time DESC
      ) AS rec_seq 
    from 
      im${logicalenv}_rawvault.SAT_DOMS_SPA_MV_DIST_SUBSTATION sat 
  ) spa 
  join im${logicalenv}_rawvault.HUB_SUBSTATION hub on spa.HUB_SUBSTATION_HSH_KEY = hub.HUB_SUBSTATION_HSH_KEY 
where 
  spa.rec_seq = 1 
  and spa.CHANGE_TYPE <> 'D';