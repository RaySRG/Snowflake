/*****************************************************************
Name:
    EDW.API_ORDER_HISTORY_DYNTBL

Description:
    A dynamic table which materialize data for order history API.
    The table is set to refresh on half house bases

Sample syntax:
SELECT top 100 *
FROM EDW.API_ORDER_HISTORY_DYNTBL;

Version History
2024-01-10  Raymond Zhao    copy from prod
*****************************************************************/
create or replace dynamic table SRG_PRD_EDW.EDW.API_ORDER_HISTORY_DYNTBL(
	CALENDARDAY,
	TRANSACTIONDATE,
	POSDOCKETNO,
	TRANSACTIONINDICATOR,
	CUSTOMER,
	SALESORG,
	NETAMOUNT,
	QUANTITY
) lag = '30 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = ORDERHISTORYAPI_WH
 as
SELECT MAX(CALENDARDAY) AS CALENDARDAY
      ,MAX(TRANSACTIONDATE) AS TRANSACTIONDATE
      ,POSDOCKETNO
      ,TRANSACTIONINDICATOR
      ,CUSTOMER
      ,SALESORG
      ,SUM(IFNULL(NETAMOUNT,0)) AS NETAMOUNT
      ,SUM(QUANTITY) AS QUANTITY 
FROM (
       SELECT TRY_to_timestamp(vk.fkdat || v.ERZET,'YYYYMMDDHH24MISS') AS calendarday
             ,TRY_to_DATE(vk.fkdat,'YYYYMMDD') AS TRANSACTIONDATE
             ,ifnull(vk.kunag,NULL) AS CUSTOMER
             ,ifnull(v.vkorg_auft,NULL) AS SALESORG
             ,CASE WHEN v.shkzg = 'X' THEN -1
                   WHEN v.MATKL = 'FREIGHT' THEN 0
                   WHEN v.matnr in ('900441','FRT-ST', 'LOY001', 'LOY002', 'LOY003', 'LOY004', 'LOY005', 'LOY006') THEN 0
                   --WHEN v.matnr not in ('900441','FRT-ST', 'LOY001', 'LOY002', 'LOY003', 'LOY004', 'LOY005', 'LOY006') THEN 1
                   WHEN vk.fktyp = 'A' THEN 0
              ELSE 1 END * to_number(v.fkimg,'MI9999999999.9999|9999999999.9999MI', 12, 2) AS QUANTITY
             ,CASE WHEN VBTYP IN ('N', 'O') THEN ifnull(to_number(v.netwr,'MI9999999999.9999|9999999999.9999MI', 12, 2),0) *-1
              ELSE ifnull(to_number(v.netwr,'MI9999999999.9999|9999999999.9999MI', 12, 2),0) END AS SALES
             ,CASE WHEN SALES < 0 AND TO_NUMBER(V.MWSBP,'MI9999999999.9999|9999999999.9999MI', 12, 2) < 0 THEN 1
                   WHEN SALES > 0 AND TO_NUMBER(V.MWSBP,'MI9999999999.9999|9999999999.9999MI', 12, 2) > 0 THEN 1
                   WHEN SALES > 0 AND TO_NUMBER(V.MWSBP,'MI9999999999.9999|9999999999.9999MI', 12, 2) < 0 THEN -1
                   WHEN SALES < 0 AND TO_NUMBER(V.MWSBP,'MI9999999999.9999|9999999999.9999MI', 12, 2) > 0 THEN -1
              END * (TO_NUMBER(V.MWSBP,'MI9999999999.9999|9999999999.9999MI', 12, 2)) AS TAXAMOUNT
             ,iff(IFNULL(V.MATNR,NULL) NOT in ('900441', 'FRT-ST', 'LOY001', 'LOY002', 'LOY003', 'LOY004', 'LOY005', 'LOY006') , ifnull(TAXAMOUNT,0) + ifnull(SALES,0) , 0) AS netamount
             ,CASE WHEN vk.fkart = 'L2' THEN 'Debit Memo' --7
                   WHEN vk.fkart = 'S2' THEN 'Cancel of Credit Memo' --6
                   WHEN vk.fkart = 'G2' THEN 'Credit Memo' --5
                   WHEN vk.fkart = 'RE' THEN 'Returns' --3
                   WHEN ifnull(v.shkzg,'') = '' AND to_number(v.netwr,'MI9999999999.9999|9999999999.9999MI', 12, 2) >= 0 THEN 'Sales' --1
                   WHEN ifnull(v.shkzg,'') = '' THEN 'Sales' --'Cancellation'   business wants TO see cancelled items IN the resultset
                   WHEN to_number(v.netwr,'MI9999999999.9999|9999999999.9999MI', 12, 2) < 0 AND v.shkzg = 'X' THEN 'Returns' --3
                   WHEN to_number(v.netwr,'MI9999999999.9999|9999999999.9999MI', 12, 2) > 0 AND v.shkzg = 'X' THEN 'Return Cancelation' --4
               END AS transactionindicator
             ,ifnull(vk.xblnr,NULL) AS POSDOCKETNO
        FROM sapecc.vbrp_0 v
             INNER JOIN sapecc.vbrk_0 vk ON vk.vbeln = v.vbeln AND v.VTWEG_AUFT = 'S3' AND vk.VTWEG = v.VTWEG_AUFT AND vk.VKORG = v.VKORG_AUFT AND to_date(vk.fkdat,'YYYYMMDD') >= DATEADD(YEAR, -11,CURRENT_DATE)
        WHERE transactionindicator IN ('Sales', 'Returns')
     )
GROUP BY POSDOCKETNO
        ,TRANSACTIONINDICATOR
        ,CUSTOMER
        ,SALESORG;