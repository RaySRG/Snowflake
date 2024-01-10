/*****************************************************************
Name:
    SRG_DEV_EDW.EDW.API_ORDER_HISTORY_DYNTBL

Description:
    A dynamic table which materialize data for order history API needed.
    The table is set to refresh on half house bases


Sample syntax:
SELECT top 100 *
FROM EDW.API_ORDER_HISTORY_DYNTBL;

Version History
2024-01-10  John Gauci      initial draft
*****************************************************************/
create or replace dynamic table EDW.API_ORDER_HISTORY_DYNTBL
(
	CUSTOMER,
	ORDER_NO,
	ORDER_TYPE,
	AMOUNT,
	CURRENCY,
	DATE_P,
	DATE_F,
	ITEMS,
	STATUS,
	CHANNEL,
	FIRST_ITEM,
	SALES_ORG
) lag = '60 days' refresh_mode = AUTO initialize = ON_CREATE warehouse = ORDERHISTORYAPI_WH
as
select CUSTOMER,
    ORDER_NO,
    TransactionIndicator as ORDER_TYPE,
    sum(NetAmount) as AMOUNT,
    CURRENCY,
    min(DATE_P) as DATE_P,
    max(DATE_F) as DATE_F,
    sum(Quantity) as ITEMS,
    max(Status) as STATUS,
    CHANNEL,
    max(FIRST_ITEM) as FIRST_ITEM,
    SALES_ORG
from
( 
    select vbrp.vkorg_auft as SALES_ORG,
        vbrk.kunag as CUSTOMER,
        vbrk.xblnr as ORDER_NO,
        vbrk.waerk as CURRENCY,
        vbrk.vtweg as CHANNEL,
        try_to_number(vbrp.netwr,'MI99999999999999999999999.9999|99999999999999999999999.9999MI', 38, 2) as SalesNum
        case when vbrk.vbtyp = 'N' then -1
              when vbrk.vbtyp = 'O' then -1
         else 1 end as SalesMultiplier,
        zeroifnull(SalesMultiplier * SalesNum) as Sales,
        try_to_number(vbrp.fkimg,'MI99999999999999999999999.9999|99999999999999999999999.9999MI', 38, 2) as QuantityNum,
        case when vbrp.shkzg = 'X' then -1
              when vbrp.matnr = 'FREIGHT' then 0
              when vbrp.matnr in ('900441','FRT-ST', 'LOY001', 'LOY002', 'LOY003', 'LOY004', 'LOY005', 'LOY006') THEN 0
              when vbrk.fktyp = 'A' then 0
         else 1 end as QuantityMultiplier,
        zeroifnull(QuantityMultiplier * QuantityNum) as Quantity,
        try_to_number(vbrp.mwsbp,'MI99999999999999999999999.9999|99999999999999999999999.9999MI', 38, 2) as TaxAmountNum,
        case when Sales < 0 and TaxAmountNum < 0 then 1
              when Sales > 0 and TaxAmountNum > 0 then 1
              when Sales > 0 and TaxAmountNum < 0 then -1
              when Sales < 0 and TaxAmountNum > 0 then -1
          end as TaxMultiplier,
        zeroifnull(TaxMultiplier*TaxAmountNum) as TaxAmount,
        iff(vbrp.matnr in ('900441','FRT-ST', 'LOY001', 'LOY002', 'LOY003', 'LOY004', 'LOY005', 'LOY006'),0,zeroifnull(TaxAmount+Sales)) as NetAmount,
        case when charindex('CS',vbrk.xblnr,1)>0 then 'CSR'
              when vbrk.fkart = 'L2' then 'Debit Memo'
              when vbrk.fkart = 'S2' then 'Cancel of Credit Memo'
              when vbrk.fkart = 'G2' then 'Credit Memo'
              when vbrk.fkart = 'RE' then 'Returns'
              when nullif(vbrp.shkzg,'') is null and SalesNum >= 0 then 'Sales'
              when nullif(vbrp.shkzg,'') is null then 'Sales' /*business wants TO see cancelled items IN the resultset*/
              when SalesNum < 0 and vbrp.shkzg = 'X' then 'Returns'
              when SalesNum > 0 and vbrp.shkzg = 'X' then 'Return Cancellation'
         end as TransactionIndicator,
        case when vbrk.vtweg <>'S3'THEN 'C'
              when COALESCE(vbuk.gbstk,vbuk.lfgsk) is not null then COALESCE(vbuk.gbstk,vbuk.lfgsk)
              when likp.wadat_ist is not null then  'C'
              else 'U'
         end as Status,
        REPLACE(COALESCE(NULLIF(vbak.zzcust_po_rel_date,'00000000'),vbrk.fkdat),'-') as DATE_P,
        REPLACE(iff(Status='C',COALESCE(likp.wadat_ist,vbrk.fkdat),''),'-') as DATE_F,
        iff(vbrp.posnr=(MIN(vbrp.posnr) OVER(PARTITION BY vbrp.vbeln)),vbrp.arktx,'') as FIRST_ITEM,
    from sapecc.vbrp_0 vbrp 
        inner join sapecc.vbrk_0 vbrk on vbrp.vbeln=vbrk.vbeln and vbrp.VTWEG_AUFT = vbrk.VTWEG and vbrp.vkorg_auft=vbrk.VKORG
        left join sapecc.likp_0 likp on vbrp.vgbel=likp.vbeln
        left join sapecc.vbak_0 vbak on vbrp.aubel=vbak.vbeln
        left join sapecc.vbuk_0 vbuk on vbrp.vbeln=vbuk.vbeln and vbrk.vtweg = 'S3'
    where to_date(nullif(vbrk.fkdat,'00000000'),'YYYYMMDD') >= '2014-01-01'::date
        and TransactionIndicator IN ('Sales', 'Returns')
)
group by CUSTOMER,
    ORDER_NO,
    TransactionIndicator,
    CURRENCY,
    CHANNEL,
    SALES_ORG
;