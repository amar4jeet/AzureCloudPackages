/*
-------------------------------------------------------------------------------
-- Program TMP_DOMS_RPT_DETAILS
-------------------------------------------------------------------------------
-- Purpose:  
--   Load data into TMP_DOMS_RPT_DETAILS from rawvalut.
--
-- Various attributes REPORT_ID
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
	hubr.REPORT_ID,
	SPA.REPORT_REF,
	SPA.REPORT_OP_COMPLETE,
	SPA.REPORT_REG_COMPLETE,
	from_utc_timestamp(SPA.REPORT_USER_COMPLETED_DATE,'Australia/Melbourne') as REPORT_USER_COMPLETED_DATE,
	from_utc_timestamp(SPA.REPORT_REG_COMPLETED_DATE,'Australia/Melbourne') as REPORT_REG_COMPLETED_DATE ,
	from_utc_timestamp(SPA.REPORT_CREATION_DATE,'Australia/Melbourne') as REPORT_CREATION_DATE,
	from_utc_timestamp(SPA.REPORT_RECEIVED_DATE,'Australia/Melbourne') as REPORT_RECEIVED_DATE,
	SPA.REPORT_TYPE_ID,
	SPA.CONTROL_REFERENCE,
	SPA.CONTROL_ENGINEER,
	SPA.COMPLETION_ENGINEER,
	SPA.READING_TYPE,
	SPA.SOP_REF,
	SPA.VOLTAGE_GROUP,
	SPA.SI_VOLTAGE,
	SPA.REPORT_CML,
	SPA.REPORT_CI,
	SPA.REPORT_RI,
	SPA.REPORT_FILLED,
	SPA.STATUS,
	SPA.JOB_NUMBER,
	from_utc_timestamp(SPA.REPORT_START_DATE,'Australia/Melbourne') as REPORT_START_DATE ,
	SPA.CONFIRMED_OUTAGE_ID,
	SPA.SI_FLAG,
	from_utc_timestamp(SPA.REPORT_PLANNED_START_DATE,'Australia/Melbourne') as REPORT_PLANNED_START_DATE,
	from_utc_timestamp(SPA.REPORT_PLANNED_END_DATE,'Australia/Melbourne') as REPORT_PLANNED_END_DATE,
	SPA.SYSTEM_REPORT_TYPE,
	SPA.CHANGE_TYPE,
	SPA.HUB_REPORT_HSH_KEY,
	SPA.LOAD_DATE_TIME ,
	SPA.RECORD_SOURCE,
	SPA.SAT_DOMS_RPT_DETAILS_HSH_DIFF
from 
  (
    select 
      sat.*, 
      ROW_NUMBER() OVER (
        partition BY sat.HUB_REPORT_HSH_KEY 
        ORDER BY 
          sat.extract_date_time DESC
      ) AS rec_seq 
    from 
      im${logicalenv}_rawvault.SAT_DOMS_RPT_DETAILS sat 
  ) SPA 
    join im${logicalenv}_rawvault.HUB_REPORT hubr on hubr.HUB_REPORT_HSH_KEY = spa.HUB_REPORT_HSH_KEY
where 
  spa.rec_seq = 1 
  and spa.CHANGE_TYPE <> 'D';