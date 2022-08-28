/*
-------------------------------------------------------------------------------
-- Program TMP_DOMS_COMPONENT_ATTRIBUTES_DETAILS
-------------------------------------------------------------------------------
-- Purpose:  
--   Load data into TMP_DOMS_COMPONENT_ATTRIBUTES_DETAILS from rawvalut.
--
-- Various attributes COMPONENT_ID
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
  hubc.COMPONENT_ID, 
  SPA.ATTRIBUTE_ID,
  SPA.ATTRIBUTE_NAME, 
  SPA.ATTRIBUTE_INDEX, 
  SPA.ATTRIBUTE_DEFINITION, 
  SPA.ATTRIBUTE_LOCATION, 
  SPA.ATTRIBUTE_VALUE, 
  SPA.ATTRIBUTE_TYPE, 
  SPA.ATTRIBUTE_TABLE_SIZE, 
  SPA.ATTRIBUTE_VECTOR_SIZE, 
  SPA.ATTRIBUTE_DE_TYPE, 
  SPA.ATTRIBUTE_ALARM_REF, 
  SPA.ATTRIBUTE_WRITE_GROUP, 
  SPA.ATTRIBUTE_READ_GROUP, 
  SPA.ATTRIBUTE_STATUS, 
  SPA.ATTRIBUTE_CLONE_ID, 
  SPA.ATTRIBUTE_GRTV_LOGGING, 
  SPA.ATTRIBUTE_RDBMS_ARCHIVING, 
  SPA.ATTRIBUTE_EVENT_PRIORITY, 
  SPA.ATTRIBUTE_LOGGING_CLASS, 
  SPA.PROTECTION_LEVEL, 
  SPA.CE_EVAL_MODE, 
  SPA.ATTRIBUTE_ALARM_INDEX, 
  SPA.ATTRIBUTE_ALARM_FILTER, 
  SPA.SOURCE, 
  SPA.IDENTITY, 
  SPA.STATISTICS_PROFILE, 
  SPA.LAST_GOOD_VALUE, 
  SPA.RT_CALC_PERIODICITY, 
  SPA.VALIDATION_GROUP, 
  SPA.ChangeType, 
  SPA.HUB_DOMS_COMPONENT_HSH_KEY, 
  SPA.LOAD_DATE_TIME, 
  SPA.RECORD_SOURCE, 
  SPA.SAT_DOMS_COMPONENT_ATTRIBUTES_DETAILS_HSH_DIFF 
from 
  (
    select 
      sat.*, 
      ROW_NUMBER() OVER (
        partition BY sat.ATTRIBUTE_ID

        ORDER BY 
          sat.extract_date_time DESC
      ) AS rec_seq 
    from 
      im${logicalenv}_rawvault.SAT_DOMS_COMPONENT_ATTRIBUTES_DETAILS sat 
  ) SPA
  join im${logicalenv}_rawvault.HUB_DOMS_COMPONENT hubc on hubc.HUB_DOMS_COMPONENT_HSH_KEY = spa.HUB_DOMS_COMPONENT_HSH_KEY 
where 
  spa.rec_seq = 1 
  and spa.ChangeType <> 'D'
  
