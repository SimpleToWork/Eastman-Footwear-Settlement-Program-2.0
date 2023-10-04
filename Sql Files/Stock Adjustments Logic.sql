
Create Table if not exists rlm_inventory_event_detail_data_table (primary key(COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION, RANKING))
       SELECT *, "Not Imported" as Status FROM
       (Select COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION,
       ROW_NUMBER() OVER (PARTITION BY COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION 
       ORDER BY COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION) AS RANKING
       from fba_inventory_event_detail
		where date >= "2020-12-01"
		and TRANSACTION_TYPE != "Shipments"
		and TRANSACTION_TYPE != "Receipts"
		and TRANSACTION_TYPE != "WhseTransfers"
		and TRANSACTION_TYPE != "CustomerReturns") A
        GROUP BY  COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION, RANKING;

       

drop table if exists program_inventory_adjustment_conversion;
create table if not exists program_inventory_adjustment_conversion
select A.*, B.asin as Product_asin, ifnull(C.upc, d.upc) as RLM_UPC
from
(select a.*,ifnull(b.asin,c.asin) as Asin from 
rlm_inventory_event_detail_data_table a
left join
(select sku, asin from product_data where asin != ""  group by sku ) b using(sku)
left join
(select PRODUCT_ID, asin from product_data where asin != ""  group by PRODUCT_ID ) c on a.sku = c.PRODUCT_ID) A
left join 
(select distinct asin, PRODUCT_ID AS UPC from product_data where  PRODUCT_ID not like 'B%') B using(asin)
left join (select * from rlm_inventory where upc != 0 group by upc) C using (UPC)
left join (select * from rlm_inventory where upc != 0 group by upc) D on right(b.upc,length(b.upc)-1) = d.upc
where status = "Not Imported"
GROUP BY COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION;
-- where ifnull(C.upc, d.upc) is not null;

drop  table if exists program_inventory_adjustment_conversion_merged;
create  table if not exists program_inventory_adjustment_conversion_merged(primary key(COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION))
select
a.*,
ifnull(c.DIVISION,d.DIVISION) as DIVISION,
ifnull(c.`DIV NAME`,d.`DIV NAME`) as `DIV NAME`, 
ifnull(c.`SUB DIV`,d.`SUB DIV`) as `SUB DIV`,
ifnull(c.`SUB DIV DESC`,d.`SUB DIV DESC`) as `SUB DIV DESC`,
ifnull(c.`WAREHOUSE`,d.`WAREHOUSE`) as `WAREHOUSE`,
ifnull(c.`WAREHOUSE NAME`,d.`WAREHOUSE NAME`) as `WAREHOUSE NAME`,
ifnull(c.`SEASON`,d.`SEASON`) as `SEASON`,
ifnull(c.`STYLE`,d.`STYLE`) as `STYLE`,
ifnull(c.`COLOR`,d.`COLOR`) as `COLOR`,
ifnull(c.`PACK/DIM`,d.`PACK/DIM`) as `PACK/DIM`,
ifnull(c.`SIZE`,d.`SIZE`) as `SIZE`,
ifnull(c.`CARTON QTY`,d.`CARTON QTY`) as `CARTON QTY`,
ifnull(c.`AVAIL IN STOCK`,d.`AVAIL IN STOCK`) as `AVAIL IN STOCK`,
ifnull(c.`WIP`,d.`WIP`) as `WIP`,
ifnull(c.`IN TRANSIT`,d.`IN TRANSIT`) as `IN TRANSIT`,
ifnull(c.`COST`,d.`COST`) as `COST`,
ifnull(c.`SKU200`,d.`SKU200`) as `SKU200`
from 
program_inventory_adjustment_conversion A
left join (select * from rlm_inventory where upc != 0 group by upc) C on a.RLM_UPC = c.upc
left join (select * from rlm_inventory where upc != 0 group by upc) D on right(a.RLM_UPC,length(a.RLM_UPC)-1) = d.upc
where (IFNULL(c.UPC,d.UPC) is not null and sku !="")
or sku = ""
;



Drop table if exists RLM_inventory_adjustments_extract;
Create table if not exists RLM_inventory_adjustments_extract 
select 
DIVISION,
left(SEASON,1) as SEASON,
right(SEASON,2) as SEASON_YEAR,
Style,
SKU200,
"" AS BLANK_1,
COLOR,
`PACK/DIM` AS SIZE_SCALE,
SIZE,
QUANTITY,
case when TRANSACTION_TYPE = "VendorReturns" then 297 else 297 end as WAREHOUSE_IN,
case when TRANSACTION_TYPE = "VendorReturns" then 297 else 297 end as WAREHOUSE_OUT
from program_inventory_adjustment_conversion_merged
order by division, Style, color;



update rlm_inventory_event_detail_data_table A inner join 
program_inventory_adjustment_conversion using(COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION)
set a.status ="Complete"
where RLM_UPC is not null;


select * from rlm_inventory_event_detail_data_table;
