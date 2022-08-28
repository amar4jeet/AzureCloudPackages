/*
-------------------------------------------------------------------------------
-- Program TMP_DOMS_SPA_MV_PROP_CON_POINT
-------------------------------------------------------------------------------
-- Purpose:  
--   Load data into TMP_DOMS_SPA_MV_PROP_CON_POINT from rawvalut.
--
-- Various attributes EQUIPMENT_ID, FEEDER, NMI, PROPERTY_NUMBER, CUSTOMER_CONNECTION_POINT
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
  EXTRACT_DATE_TIME, 
  EQUIPMENT_ID, 
  CUSTOMER_CONNECTION_POINT, 
  FEEDER, 
  NMI, 
  PROPERTY_NUMBER, 
  FEEDER_COMPONENT_ID, 
  SUPPLY_STATE, 
  UPDATE_TIME,
  CHANGE_TYPE, 
  LINK_NMI_PROPERTY_CUSTOMER_CONNECTION_EQUIPMENT_FEEDER_HSH_KEY, 
  LOAD_DATE_TIME, 
  RECORD_SOURCE, 
  LSAT_DOMS_SPA_MV_PROP_CON_POINT_HSH_DIFF
from (
select 
  SPA.EXTRACT_DATE_TIME, 
  hube.EQUIPMENT_ID, 
  hubcc.CUSTOMER_CONNECTION_POINT, 
  hubf.FEEDER, 
  hubn.NMI, 
  hubp.PROPERTY_NUMBER, 
  SPA.FEEDER_COMPONENT_ID, 
  SPA.SUPPLY_STATE, 
  from_utc_timestamp(SPA.UPDATE_TIME, 'Australia/Melbourne') as UPDATE_TIME,
  SPA.CHANGE_TYPE, 
  SPA.LINK_NMI_PROPERTY_CUSTOMER_CONNECTION_EQUIPMENT_FEEDER_HSH_KEY, 
  SPA.LOAD_DATE_TIME, 
  SPA.RECORD_SOURCE, 
  SPA.LSAT_DOMS_SPA_MV_PROP_CON_POINT_HSH_DIFF,
  ROW_NUMBER() OVER (
        partition BY hube.HUB_EQUIPMENT_HSH_KEY, hubn.HUB_NMI_HSH_KEY, SPA.SUPPLY_STATE
        ORDER BY 
          SPA.extract_date_time DESC
      ) AS rec_seq 
  from im${logicalenv}_rawvault.LSAT_DOMS_SPA_MV_PROP_CON_POINT SPA 
  join im${logicalenv}_rawvault.LINK_NMI_PROPERTY_CUSTOMER_CONNECTION_EQUIPMENT_FEEDER lnk on spa.LINK_NMI_PROPERTY_CUSTOMER_CONNECTION_EQUIPMENT_FEEDER_HSH_KEY = lnk.LINK_NMI_PROPERTY_CUSTOMER_CONNECTION_EQUIPMENT_FEEDER_HSH_KEY 
  join im${logicalenv}_rawvault.HUB_EQUIPMENT hube on hube.HUB_EQUIPMENT_HSH_KEY = lnk.HUB_EQUIPMENT_HSH_KEY 
  join im${logicalenv}_rawvault.HUB_CUSTOMER_CONNECTION hubcc on hubcc.HUB_CUSTOMER_CONNECTION_HSH_KEY = lnk.HUB_CUSTOMER_CONNECTION_HSH_KEY 
  join im${logicalenv}_rawvault.HUB_FEEDER hubf on hubf.HUB_FEEDER_HSH_KEY = lnk.HUB_FEEDER_HSH_KEY 
  join im${logicalenv}_rawvault.HUB_NMI hubn on hubn.HUB_NMI_HSH_KEY = lnk.HUB_NMI_HSH_KEY 
  join im${logicalenv}_rawvault.HUB_PROPERTY hubp on hubp.HUB_PROPERTY_HSH_KEY = lnk.HUB_PROPERTY_HSH_KEY 
)  as FIN
where 
  FIN.rec_seq = 1 
  and FIN.CHANGE_TYPE <> 'D';