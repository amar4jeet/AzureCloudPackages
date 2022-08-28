/*
-------------------------------------------------------------------------------
-- Program TMP_DOMS_SPA_MV_CALL
-------------------------------------------------------------------------------
-- Purpose:  
--   Load data into TMP_DOMS_SPA_MV_CALL from rawvalut.
--
-- Various attributes CALL_ID, OPERATING_ZONE_ID, CUSTOMER_CONNECTION_POINT
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
  EXTRACT_DATE_TIME, 
  CALL_ID, 
  OPERATING_ZONE_ID, 
  CUSTOMER_CONNECTION_POINT, 
  CALL_CATEGORY_DESCRIPTION, 
  CALL_CATEGORY_ID, 
  CALL_CHARGEABLE, 
  CALL_REFERENCE, 
  CALL_STATUS, 
  CALL_TAKER, 
  CALLER_FIRST_NAME, 
  CALLER_INITIALS, 
  CALLER_NAME, 
  CALLER_TELEPHONE, 
  CALLER_TITLE, 
  COMMENTS, 
  CRITICAL_INFO, 
  CUSTOMER_WARNED, 
  DANGEROUS_SITUATION, 
  LOGGED_TIME, 
  POSTCODE, 
  PREMISE_NAME, 
  PREMISE_NUMBER, 
  PRIORITY, 
  STREET, 
  TOWN_NAME, 
  TPD_CUSTOMER_AGREEMENT_NUMBER, 
  TPD_POLICE_FIRE_EVENT_NUMBER, 
  TPD_STATEMENT_AT_SITE_NUMBER, 
  TPD_VEHICLE_DETAILS, 
  CHANGE_TYPE, 
  LINK_CUSTOMER_CONNECTION_CALL_OPERATING_ZONE_HSH_KEY, 
  LOAD_DATE_TIME, 
  LSAT_DOMS_SPA_MV_CALL_HSH_DIFF, 
  RECORD_SOURCE 
FROM 
  (
    select 
      SPA.EXTRACT_DATE_TIME, 
      hubc.CALL_ID, 
      huboz.OPERATING_ZONE_ID, 
      hubcc.CUSTOMER_CONNECTION_POINT, 
      SPA.CALL_CATEGORY_DESCRIPTION, 
      SPA.CALL_CATEGORY_ID, 
      SPA.CALL_CHARGEABLE, 
      SPA.CALL_REFERENCE, 
      SPA.CALL_STATUS, 
      SPA.CALL_TAKER, 
      SPA.CALLER_FIRST_NAME, 
      SPA.CALLER_INITIALS, 
      SPA.CALLER_NAME, 
      SPA.CALLER_TELEPHONE, 
      SPA.CALLER_TITLE, 
      SPA.COMMENTS, 
      SPA.CRITICAL_INFO, 
      SPA.CUSTOMER_WARNED, 
      SPA.DANGEROUS_SITUATION, 
      SPA.LOGGED_TIME, 
      SPA.POSTCODE, 
      SPA.PREMISE_NAME, 
      SPA.PREMISE_NUMBER, 
      SPA.PRIORITY, 
      SPA.STREET, 
      SPA.TOWN_NAME, 
      SPA.TPD_CUSTOMER_AGREEMENT_NUMBER, 
      SPA.TPD_POLICE_FIRE_EVENT_NUMBER, 
      SPA.TPD_STATEMENT_AT_SITE_NUMBER, 
      SPA.TPD_VEHICLE_DETAILS, 
      SPA.CHANGE_TYPE, 
      SPA.LINK_CUSTOMER_CONNECTION_CALL_OPERATING_ZONE_HSH_KEY, 
      SPA.LOAD_DATE_TIME, 
      SPA.LSAT_DOMS_SPA_MV_CALL_HSH_DIFF, 
      SPA.RECORD_SOURCE, 
      ROW_NUMBER() OVER (
        partition BY hubc.CALL_ID 
        ORDER BY 
          SPA.extract_date_time DESC
      ) AS rec_seq 
    FROM 
      im${logicalenv}_rawvault.LSAT_DOMS_SPA_MV_CALL SPA 
      join im${logicalenv}_rawvault.LINK_CUSTOMER_CONNECTION_CALL_OPERATING_ZONE lnk on spa.LINK_CUSTOMER_CONNECTION_CALL_OPERATING_ZONE_HSH_KEY = lnk.LINK_CUSTOMER_CONNECTION_CALL_OPERATING_ZONE_HSH_KEY 
      join im${logicalenv}_rawvault.HUB_OPERATING_ZONE huboz on huboz.HUB_OPERATING_ZONE_HSH_KEY = lnk.HUB_OPERATING_ZONE_HSH_KEY 
      join im${logicalenv}_rawvault.HUB_CUSTOMER_CONNECTION hubcc on hubcc.HUB_CUSTOMER_CONNECTION_HSH_KEY = lnk.HUB_CUSTOMER_CONNECTION_HSH_KEY 
      join im${logicalenv}_rawvault.HUB_CALL hubc on hubc.HUB_CALL_HSH_KEY = lnk.HUB_CALL_HSH_KEY
  ) as FIN 
where 
  FIN.rec_seq = 1 
  and FIN.CHANGE_TYPE <> 'D';