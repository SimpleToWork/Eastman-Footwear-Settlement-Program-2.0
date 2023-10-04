
select * from rlm_settlements_data_table
where TRANSACTION_TYPE = "refund"
and FEE_TYPE = "Principal"
and QUANTITY_PURCHASED != 0;

select *, 
row_number() over (partition by POSTED_DATE_1, ORDER_ID, sku, FEE_CATEGORY, FEE_CATEGORY,FEE_TYPE, AMOUNT) as ranking
 from settlements_statements
where POSTED_DATE_1 > curdate() - interval 20 day
and TRANSACTION_TYPE = "refund"
and STATUS = "open"
and QUANTITY_PURCHASED != 0
and FEE_TYPE = "Principal";

select * from settlements_statements
where order_id = "111-3908330-4788220";
select * from rlm_settlements_data_table
where  order_id = "111-3908330-4788220";
select * from rlm_settlements_data_table
where status = "Not Imported"
and (TRANSACTION_TYPE = "refund" and FEE_CATEGORY = "PRICE_TYPE" and FEE_TYPE = "Principal")
or (TRANSACTION_TYPE = "refund" and FEE_CATEGORY = "PRICE_TYPE" and FEE_TYPE like "%Tax%");

drop temporary table if exists program_settlement_refund_conversion;
create temporary table if not exists program_settlement_refund_conversion(primary key(order_id, sku))
select 
ifnull(ORDER_ID,"") as Order_ID, 
SKU, 
POSTED_DATE,
sum(case when FEE_TYPE="Principal" then QUANTITY_PURCHASED else 0 end) as quantity, 
sum(AMOUNT) as total_amount,
sum(case when FEE_CATEGORY = "PRICE_TYPE" AND FEE_TYPE ="Principal" then AMOUNT else 0 end) as Principal,
sum(case when FEE_TYPE like '%tax%' then amount else 0 end) as Tax
from rlm_settlements_data_table
where status = "Not Imported"
and ((TRANSACTION_TYPE = "refund" and FEE_CATEGORY = "PRICE_TYPE" and FEE_TYPE = "Principal")
or (TRANSACTION_TYPE = "refund" and FEE_CATEGORY = "PRICE_TYPE" and FEE_TYPE like "%Tax%"))
group by ORDER_ID, SKU;



drop temporary table if exists program_settlement_refund_conversion_merged;
create temporary table if not exists program_settlement_refund_conversion_merged(primary key(order_id, sku))
select b.upc, 
IFNULL(c.UPC,d.UPC) AS RLM_UPC,
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
(select a.*, b.Asin from program_settlement_refund_conversion a left join product_data b using(sku)) A
left join 
(select distinct asin, PRODUCT_ID AS UPC from product_data where Fulfillment_Channel = "AMAZON_NA" and PRODUCT_ID not like 'B%') B
using(asin)
left join (select * from rlm_inventory where upc != 0 group by upc) C using (UPC)
left join (select * from rlm_inventory where upc != 0 group by upc) D on right(b.upc,length(b.upc)-1) = d.upc
where IFNULL(c.UPC,d.UPC) is not null;


Drop table if exists RLM_Settlement_refunds_extract;
Create table if not exists RLM_Settlement_refunds_extract 
select 
"01" as Company,
DIVISION,
297 as Warehouse,
"EASF02" as `Customer_Number`,
"0001" as Store_Number,
"C" as Col_1,
"R" as Col_2,
left(SEASON,1) as Style_SEASON,
"0" as Col_3,
UPC,
quantity,
Principal + tax as Amount,
Principal + tax as Amount,
"0"  as Col_4,
"0"  as Col_5,
"" as Blank_1,
ORDER_ID as External_Order_Number,
"RBM" as Col_6,
"S" as Col_7,
POSTED_DATE,
"$" as Col_8
from program_settlement_refund_conversion_merged;



update rlm_settlements_data_table A inner join 
(select POSTED_DATE, b.UPC, asin, order_id, sku, IFNULL(C.UPC,D.UPC) AS RLM_UPC  
from 
(select a.*, b.Asin from program_settlement_refund_conversion a left join product_data b using(sku)) A
left join 
(select distinct asin, PRODUCT_ID AS UPC from product_data where Fulfillment_Channel = "AMAZON_NA" and PRODUCT_ID not like 'B%') B using(asin)
left join 
(select * from rlm_inventory where upc != 0 group by upc) C on b.upc = c.upc
left join 
(select * from rlm_inventory where upc != 0 group by upc) D on right(b.upc,length(b.upc)-1) = d.upc) B
using(ORDER_ID, sku)
set a.Status = "Complete"
where RLM_UPC is not null
and ((TRANSACTION_TYPE = "refund" and FEE_CATEGORY = "PRICE_TYPE" and FEE_TYPE = "Principal")
or (TRANSACTION_TYPE = "refund" and FEE_CATEGORY = "PRICE_TYPE" and FEE_TYPE like "%Tax%"));

