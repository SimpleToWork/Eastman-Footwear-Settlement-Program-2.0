use  eastman_footwear_amazon;

select sum(AMOUNT) from settlements_statements
where POSTED_DATE_1 = curdate() - interval 1 day
and TRANSACTION_TYPE = "Order"
and FEE_TYPE = "Principal";

select *, 
row_number() over (partition by POSTED_DATE_1, ORDER_ID, sku, FEE_CATEGORY, FEE_CATEGORY,FEE_TYPE, AMOUNT) as ranking
 from settlements_statements
where POSTED_DATE_1 > curdate() - interval 20 day
and TRANSACTION_TYPE = "Order"
and FEE_TYPE = "Principal";



select POSTED_DATE_1, ORDER_ID, sku,FEE_CATEGORY, FEE_CATEGORY,FEE_TYPE,AMOUNT,ranking,  count(*), "" as Active from 
(select *, 
row_number() over (partition by POSTED_DATE_1, ORDER_ID, sku, FEE_CATEGORY, FEE_CATEGORY,FEE_TYPE, AMOUNT) as ranking
 from settlements_statements
where POSTED_DATE_1 > curdate() - interval 60 day) A
where POSTED_DATE_1 > curdate() - interval 60 day
-- and TRANSACTION_TYPE = "Order"
-- and FEE_TYPE = "Principal"
group by POSTED_DATE_1, ORDER_ID, sku, FEE_CATEGORY, FEE_CATEGORY,FEE_TYPE, AMOUNT, ranking;


drop temporary table if exists program_settlement_order_conversion;
create temporary table if not exists program_settlement_order_conversion(primary key(order_id, sku))
select ifnull(ORDER_ID,"") as Order_ID, SKU, POSTED_DATE,
sum(QUANTITY_PURCHASED) as quantity, 
sum(AMOUNT) as total_amount,
sum(case when FEE_CATEGORY = "PRICE_TYPE" AND FEE_TYPE ="Principal" then AMOUNT else 0 end) as Principal,
sum(case when FEE_CATEGORY ="ITEM_RELATED_FEE_TYPE" 
				and (FEE_TYPE="FBAPerUnitFulfillmentFee" 
						or FEE_TYPE = "FBAPerOrderFulfillmentFee" 
                        or FEE_TYPE = "FBAWeightBasedFee") then AMOUNT else 0 end) as Freight,
sum(case when FEE_TYPE like '%tax%' then amount else 0 end) as Tax,
sum(case when NOT (FEE_CATEGORY = "PRICE_TYPE" AND FEE_TYPE ="Principal")
				and not (FEE_CATEGORY ="ITEM_RELATED_FEE_TYPE" 
					and (FEE_TYPE="FBAPerUnitFulfillmentFee" 
						or FEE_TYPE = "FBAPerOrderFulfillmentFee" 
						or FEE_TYPE = "FBAWeightBasedFee")) 
				and  FEE_TYPE not like '%tax%'
                then amount else 0 end) as Misc
from rlm_settlements_data_table
where status = "Not Imported"
and TRANSACTION_TYPE = "Order"
group by ORDER_ID, SKU 
;

SELECT * 
from settlements_statements
where POSTED_DATE_1 = curdate() - interval 2 day
and TRANSACTION_TYPE = "Order"
group by ORDER_ID, SKU 
;


select a.* from product_data a  inner join (select a.*, b.Asin from program_settlement_order_conversion a left join product_data b using(sku)) b using(asin);

select * from 
        (select sku,asin,  product_id, c.UPC from settlements_statements A
        left join product_data b  using(sku)
        left join (select distinct asin, PRODUCT_ID AS UPC from product_data where PRODUCT_ID not like 'B%') C using(asin)
        where POSTED_DATE_1 >=  "2020-12-01"
        and (sku != "" and sku is not null)
        group by sku) A
        where upc is null
        order by sku;


drop temporary table if exists program_settlement_order_conversion_merged;
create temporary table if not exists program_settlement_order_conversion_merged(primary key(order_id, sku))
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
(select a.*, b.Asin from program_settlement_order_conversion a left join product_data b using(sku)) A
left join 
(select distinct asin, PRODUCT_ID AS UPC from product_data where Fulfillment_Channel = "AMAZON_NA" and PRODUCT_ID not like 'B%') B
using(asin)
left join (select * from rlm_inventory where upc != 0 group by upc) C using (UPC)
left join (select * from rlm_inventory where upc != 0 group by upc) D on right(b.upc,length(b.upc)-1) = d.upc
where IFNULL(c.UPC,d.UPC) is not null
;


select * from RLM_Settlement_Orders_extract;

select * from 
        (select sku,asin,  product_id, c.UPC from settlements_statements A
        left join product_data b  using(sku)
        left join (select distinct asin, PRODUCT_ID AS UPC from product_data where PRODUCT_ID not like 'B%') C using(asin)
        where POSTED_DATE_1 >=  "2020-12-01"
        and (sku != "" and sku is not null)
        group by sku) A
        where upc is not null;


Drop table if exists RLM_Settlement_Orders_extract;
Create table if not exists RLM_Settlement_Orders_extract 
select 
"01" as Company,
DIVISION,
"EASF02" as `Customer_Number`,
1 as Store_Number,
ORDER_ID as External_Order_Number,
POSTED_DATE as Order_Date,
POSTED_DATE as Start_Date,
POSTED_DATE as Cancel_Date,
82 as Terms,
"RTG" as Routing,
297 as Warehouse,
"RBM" as Sales_Rep,
ORDER_ID as Customer_PO_Number,
"" as Blank_1,
"" as Blank_2,
"" as Blank_3,
"" as Blank_4,
"" as Blank_5,
"" as Blank_6,
0 as Merch_Discount,
"S" as Order_Type,
left(SEASON,1) as Style_SEASON,
right(SEASON,2) as Style_Year,
STYLE,
SKU200,
COLOR as SKU300,
`PACK/DIM` as SKU400,
SIZE as Size_Code,
quantity as Units,
Principal as Customer_Price,
Principal as Cop_Price,
0 as Dollar_Discount,
"" as Blank_7,
"" as Blank_8,
"" as Blank_9,
"" as Blank_10,
"" as Blank_11,
"" as Blank_12,
"" as Blank_13,
"" as Blank_14,
Misc,
Freight,
Tax,
"" as Blank_15,
"" as Blank_16,
"" as Blank_17,
"A" as Order_Origin
from program_settlement_order_conversion_merged;

select *  from 
        (select sku,asin,  product_id, c.UPC, IFNULL(D.UPC,E.UPC) AS RLM_UPC from settlements_statements A
        left join product_data b  using(sku)
        left join (select distinct asin, PRODUCT_ID AS UPC from product_data where PRODUCT_ID not like 'B%') C using(asin)
        left join (select * from rlm_inventory where upc != 0 group by upc) D using (UPC)
		left join (select * from rlm_inventory where upc != 0 group by upc) E on right(C.upc,length(C.upc)-1) = E.upc
        where POSTED_DATE_1 >=  "2020-12-01"
        and (sku != "" and sku is not null)
        group by sku) A
        where RLM_UPC is null
        order by sku;

update rlm_settlements_data_table A inner join 
(select POSTED_DATE, b.UPC, asin, order_id, sku, IFNULL(C.UPC,D.UPC) AS RLM_UPC  
from 
(select a.*, b.Asin from program_settlement_order_conversion a left join product_data b using(sku)) A
left join 
(select distinct asin, PRODUCT_ID AS UPC from product_data where Fulfillment_Channel = "AMAZON_NA" and PRODUCT_ID not like 'B%') B using(asin)
left join 
(select * from rlm_inventory where upc != 0 group by upc) C on b.upc = c.upc
left join 
(select * from rlm_inventory where upc != 0 group by upc) D on right(b.upc,length(b.upc)-1) = d.upc) B
using(ORDER_ID, sku)
set a.Status = "Complete"
where RLM_UPC is not null
and Transaction_type = "Order";
