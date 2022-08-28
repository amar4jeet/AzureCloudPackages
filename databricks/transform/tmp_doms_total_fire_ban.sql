/*
-------------------------------------------------------------------------------
-- Program TMP_DOMS_TOTAL_FIRE_BAN
-------------------------------------------------------------------------------
-- Purpose: 
--   Load data into TMP_DOMS_TOTAL_FIRE_BAN from rawvalut. 
--
-- Various attributes CHILD_COMP_ID, DATE_TIME,ID, EQUIPMENT_ID
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
  hube.EQUIPMENT_ID,
  SPA.BATCHCMDNAME,
  SPA.CFA_REGION,
  SPA.CHILD_COMP_ID,
  SPA.DATE_TIME,
  SPA.ID,
  SPA.NO_OF_ATTEMPTS,
  SPA.SETTING_TYPE,
  SPA.STATUS,
  SPA.HUB_EQUIPMENT_HSH_KEY,
  SPA.SAT_DOMS_TOTAL_FIRE_BAN_HSH_DIFF,
  SPA.CHANGE_TYPE,
  SPA.RECORD_SOURCE,
  SPA.LOAD_DATE_TIME
from 
  (
	select 
      sat.*, 
      ROW_NUMBER() OVER (
        partition BY sat.HUB_EQUIPMENT_HSH_KEY, sat.CHILD_COMP_ID, sat.DATE_TIME,sat.ID
        ORDER BY 
          sat.extract_date_time DESC
      ) AS rec_seq 
    from 
      im${logicalenv}_rawvault.SAT_DOMS_TOTAL_FIRE_BAN sat 
  ) SPA 
  join im${logicalenv}_rawvault.HUB_EQUIPMENT hube on hube.HUB_EQUIPMENT_HSH_KEY = spa.HUB_EQUIPMENT_HSH_KEY
  where 
	  spa.rec_seq = 1 
	  and spa.CHANGE_TYPE <> 'D';