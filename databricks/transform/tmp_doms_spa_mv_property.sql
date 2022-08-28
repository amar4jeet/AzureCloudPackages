/*
-------------------------------------------------------------------------------
-- Program TMP_DOMS_SPA_MV_PROPERTY
-------------------------------------------------------------------------------
-- Purpose:  
--   Load data into TMP_DOMS_SPA_MV_PROPERTY from rawvalut.
--
-------------------------------------------------------------------------------
-- Date       | Author                        | Description
-------------------------------------------------------------------------------
-- 2022-07-28 |  Santhosh Kumar               | Initial Version
--            |                               |
--            |                               |
--            |                               |
-------------------------------------------------------------------------------
*/


SELECT 

  EXTRACT_DATE_TIME ,
  CUSTOMER_NO  ,
  PROPERTY_NUMBER ,
  CHANGE_TYPE ,
  CUSTOMER_NAME ,
  ADDRESS_NUMBER ,
  ENERGISED_FLAG ,
  ENERGISED_DESC ,
  PROPERTY_TYPE ,
  FRMP ,
  LOAD_DATE_TIME ,
  RECORD_SOURCE ,
  LSAT_DOMS_SPA_MV_PROPERTY_HSH_DIFF ,
  LINK_CUSTOMER_PROPERTY_HSH_KEY 

FROM
(

SELECT 

 LSDSMP.EXTRACT_DATE_TIME ,
 HC.CUSTOMER_NO  ,
 HP.PROPERTY_NUMBER ,
 LSDSMP.CHANGE_TYPE ,
 LSDSMP.CUSTOMER_NAME ,
 LSDSMP.ADDRESS_NUMBER ,
 LSDSMP.ENERGISED_FLAG ,
 LSDSMP.ENERGISED_DESC ,
 LSDSMP.PROPERTY_TYPE ,
 LSDSMP.FRMP ,
 LSDSMP.LOAD_DATE_TIME ,
 LSDSMP.RECORD_SOURCE ,
 LSDSMP.LSAT_DOMS_SPA_MV_PROPERTY_HSH_DIFF ,
 LSDSMP.LINK_CUSTOMER_PROPERTY_HSH_KEY,
 ROW_NUMBER() OVER (
        partition BY  HC.CUSTOMER_NO ,HP.PROPERTY_NUMBER ,LSDSMP.LINK_CUSTOMER_PROPERTY_HSH_KEY
        ORDER BY 
          LSDSMP.EXTRACT_DATE_TIME  DESC
      ) AS rec_seq 
      
FROM im${logicalenv}_RAWVAULT.LSAT_DOMS_SPA_MV_PROPERTY LSDSMP      
JOIN im${logicalenv}_RAWVAULT.LINK_CUSTOMER_PROPERTY LCP ON LCP.LINK_CUSTOMER_PROPERTY_HSH_KEY = LSDSMP.LINK_CUSTOMER_PROPERTY_HSH_KEY
JOIN im${logicalenv}_RAWVAULT.HUB_PROPERTY HP ON HP.HUB_PROPERTY_HSH_KEY= LCP.HUB_PROPERTY_HSH_KEY
JOIN im${logicalenv}_RAWVAULT.HUB_CUSTOMER HC ON HC.HUB_CUSTOMER_HSH_KEY= LCP.HUB_CUSTOMER_HSH_KEY    
)
as SMP  where 
  SMP.rec_seq = 1 
  and SMP.CHANGE_TYPE <> 'D';