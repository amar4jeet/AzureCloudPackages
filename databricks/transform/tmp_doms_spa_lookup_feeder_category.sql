/*
-------------------------------------------------------------------------------
-- Program TMP_DOMS_SPA_LOOKUP_FEEDER_CATEGORY 
-------------------------------------------------------------------------------
-- Purpose:  
--   Load data into TMP_DOMS_SPA_LOOKUP_FEEDER_CATEGORY from rawvalut.
--
-- Various attributes FEEDER
-------------------------------------------------------------------------------
-- Date       | Author                        | Description
-------------------------------------------------------------------------------
-- 2022-07-15 |  Amarjeet Kumar               | Initial Version
--            |                               |
--            |                               |
--            |                               |
-------------------------------------------------------------------------------
*/

SELECT 
  SPA.EXTRACT_DATE_TIME, 
  hubf.FEEDER, 
  SPA.DATED, 
  SPA.FEEDER_CATEGORY, 
  SPA.FEEDER_NAME, 
  SPA.CHANGE_TYPE, 
  SPA.HUB_FEEDER_HSH_KEY, 
  SPA.LOAD_DATE_TIME, 
  SPA.RECORD_SOURCE, 
  SPA.SAT_DOMS_SPA_LOOKUP_FEEDER_CATEGORY_HSH_DIFF 
from 
  (
    select 
      sat.*, 
      ROW_NUMBER() OVER (
        partition BY sat.HUB_FEEDER_HSH_KEY, sat.DATED
        ORDER BY 
          sat.extract_date_time DESC
      ) AS rec_seq 
    from 
      im${logicalenv}_rawvault.SAT_DOMS_SPA_LOOKUP_FEEDER_CATEGORY sat 
  ) SPA 
  join im${logicalenv}_rawvault.HUB_FEEDER hubf on spa.HUB_FEEDER_HSH_KEY = hubf.HUB_FEEDER_HSH_KEY 
where 
  spa.rec_seq = 1 
  and spa.CHANGE_TYPE <> 'D';
