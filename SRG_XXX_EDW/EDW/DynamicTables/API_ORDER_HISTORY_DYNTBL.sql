/*****************************************************************
Name:
    EDW.API_ORDER_HISTORY_DYNTBL

Description:
    A dynamic table which materialize data for order history API.
    The table is set to refresh on half hour bases.

Sample syntax:
SELECT top 100 *
FROM EDW.API_ORDER_HISTORY_DYNTBL;

Version History
2024-01-10  Raymond Zhao    copy from prod
2024-01-11  Raymond Zhao    rename columns as per API
*****************************************************************/
create or replace dynamic table EDW.API_ORDER_HISTORY_DYNTBL_DEV
(
	CUSTOMER,
	SALES_ORG,
      ORDER_DATE, -- can we rename it to ORDER_DATETIME
	ORDER_NO,
      TRANSACTIONDATE,
	ORDER_TYPE,
	ORDER_TOTAL,
	ITEM_COUNT
) lag = '30 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = DEV_WH
as
SELECT
      CUSTOMER,
      SALES_ORG,
      MAX(ORDER_DATE) AS ORDER_DATE,
      ORDER_NO,
      MAX(TRANSACTIONDATE) AS TRANSACTIONDATE,
      ORDER_TYPE,
      SUM(IFNULL(ORDER_TOTAL,0)) AS ORDER_TOTAL,
      SUM(ITEM_COUNT) AS ITEM_COUNT
FROM
(
      SELECT
            TRY_TO_TIMESTAMP(vk.fkdat || v.ERZET,'YYYYMMDDHH24MISS') AS ORDER_DATE,
            TRY_to_DATE(vk.fkdat,'YYYYMMDD') AS TRANSACTIONDATE,
            IFNULL(vk.kunag,NULL) AS CUSTOMER,
            IFNULL(v.vkorg_auft,NULL) AS SALES_ORG,
            CASE
                  WHEN v.shkzg = 'X' THEN -1
                  WHEN v.MATKL = 'FREIGHT' THEN 0
                  WHEN v.matnr in ('900441','FRT-ST', 'LOY001', 'LOY002', 'LOY003', 'LOY004', 'LOY005', 'LOY006') THEN 0
                  --WHEN v.matnr not in ('900441','FRT-ST', 'LOY001', 'LOY002', 'LOY003', 'LOY004', 'LOY005', 'LOY006') THEN 1
                  WHEN vk.fktyp = 'A' THEN 0
                  ELSE 1
            END * to_number(v.fkimg,'MI9999999999.9999|9999999999.9999MI', 12, 2) AS ITEM_COUNT,
            CASE
                  WHEN VBTYP IN ('N', 'O') THEN IFNULL(to_number(v.netwr,'MI9999999999.9999|9999999999.9999MI', 12, 2),0) * -1
                  ELSE IFNULL(to_number(v.netwr,'MI9999999999.9999|9999999999.9999MI', 12, 2),0) 
            END AS SALES,
            CASE
                  WHEN SALES < 0 AND TO_NUMBER(V.MWSBP,'MI9999999999.9999|9999999999.9999MI', 12, 2) < 0 THEN 1
                  WHEN SALES > 0 AND TO_NUMBER(V.MWSBP,'MI9999999999.9999|9999999999.9999MI', 12, 2) > 0 THEN 1
                  WHEN SALES > 0 AND TO_NUMBER(V.MWSBP,'MI9999999999.9999|9999999999.9999MI', 12, 2) < 0 THEN -1
                  WHEN SALES < 0 AND TO_NUMBER(V.MWSBP,'MI9999999999.9999|9999999999.9999MI', 12, 2) > 0 THEN -1
            END * (TO_NUMBER(V.MWSBP,'MI9999999999.9999|9999999999.9999MI', 12, 2)) AS TAXAMOUNT,
            IFF(IFNULL(V.MATNR,NULL) NOT in ('900441', 'FRT-ST', 'LOY001', 'LOY002', 'LOY003', 'LOY004', 'LOY005', 'LOY006') , IFNULL(TAXAMOUNT,0) + IFNULL(SALES,0) , 0) AS ORDER_TOTAL,
            CASE
                  WHEN vk.fkart = 'L2' THEN 'Debit Memo' --7
                  WHEN vk.fkart = 'S2' THEN 'Cancel of Credit Memo' --6
                  WHEN vk.fkart = 'G2' THEN 'Credit Memo' --5
                  WHEN vk.fkart = 'RE' THEN 'Returns' --3
                  WHEN IFNULL(v.shkzg,'') = '' AND to_number(v.netwr,'MI9999999999.9999|9999999999.9999MI', 12, 2) >= 0 THEN 'Sales' --1
                  WHEN IFNULL(v.shkzg,'') = '' THEN 'Sales' --'Cancellation'   business wants TO see cancelled items IN the resultset
                  WHEN to_number(v.netwr,'MI9999999999.9999|9999999999.9999MI', 12, 2) < 0 AND v.shkzg = 'X' THEN 'Returns' --3
                  WHEN to_number(v.netwr,'MI9999999999.9999|9999999999.9999MI', 12, 2) > 0 AND v.shkzg = 'X' THEN 'Return Cancelation' --4
            END AS ORDER_TYPE,
            IFNULL(vk.xblnr,NULL) AS ORDER_NO
        FROM sapecc.vbrp_0 v
            INNER JOIN sapecc.vbrk_0 vk ON vk.vbeln = v.vbeln AND v.VTWEG_AUFT = 'S3' AND vk.VTWEG = v.VTWEG_AUFT AND vk.VKORG = v.VKORG_AUFT AND to_date(vk.fkdat,'YYYYMMDD') >= DATEADD(YEAR, -11,CURRENT_DATE)
        WHERE ORDER_TYPE IN ('Sales', 'Returns')
     )
GROUP BY
      CUSTOMER,
      SALES_ORG,
      ORDER_NO,
      ORDER_TYPE
;