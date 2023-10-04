


drop temporary table if exists program_settlement_credit_conversion;
create temporary table if not exists program_settlement_credit_conversion(primary key(posted_date, ORDER_ID, SKU, Transaction_type, FEE_CATEGORY, FEE_TYPE, ranking))
select 
ifnull(ORDER_ID,"") as Order_ID, 
SKU, 
POSTED_DATE,
Transaction_type, FEE_CATEGORY, FEE_TYPE,
ranking,
sum(QUANTITY_PURCHASED) as quantity, 
sum(AMOUNT) as total_amount
-- sum(case when FEE_CATEGORY = "PRICE_TYPE" AND FEE_TYPE ="Principal" then AMOUNT else 0 end) as Principal,
-- sum(case when FEE_TYPE like '%tax%' then amount else 0 end) as Tax
from rlm_settlements_data_table
where status = "Not Imported"
and TRANSACTION_TYPE != "order"
and not ((TRANSACTION_TYPE = "refund" and FEE_CATEGORY = "PRICE_TYPE" and FEE_TYPE = "Principal")
or (TRANSACTION_TYPE = "refund" and FEE_CATEGORY = "PRICE_TYPE" and FEE_TYPE like "%Tax%"))
group by posted_date, ORDER_ID, SKU, Transaction_type, FEE_CATEGORY, FEE_TYPE, ranking;




drop temporary table if exists program_settlement_credit_conversion_merged;
create temporary table if not exists program_settlement_credit_conversion_merged(primary key(posted_date, order_id, sku,Transaction_type, FEE_CATEGORY, FEE_TYPE, ranking))
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
(select a.*, b.Asin from program_settlement_credit_conversion a left join product_data b using(sku)) A
left join 
(select distinct asin, PRODUCT_ID AS UPC from product_data where Fulfillment_Channel = "AMAZON_NA" and PRODUCT_ID not like 'B%') B
using(asin)
left join (select * from rlm_inventory where upc != 0 group by upc) C using (UPC)
left join (select * from rlm_inventory where upc != 0 group by upc) D on right(b.upc,length(b.upc)-1) = d.upc
where (IFNULL(c.UPC,d.UPC) is not null and sku !="")
or sku = ""
;




select * from program_settlement_credit_conversion_merged;


Drop table if exists RLM_Settlement_credits_extract;
Create table if not exists RLM_Settlement_credits_extract 
select 
"01" as Company,
ifnull(DIVISION,0) as DIVISION,
"" as Invoice_Number,
POSTED_DATE as Invoice_Date,
left(SEASON,1) as SEASON,
"EASF02" as `Customer_Number`,
"0001" as Store_Number,
"" as Order_type,
concat(ORDER_ID,"_",Division) as Customer_Po_Number,
297 as Warehouse,
ifnull(`SUB DIV`,"") as Sub_Division,
"" as Factor_Code,
"RBM" as Sales_Rea,
"RBM" as Sales_Rep,
"" as Terms_Code,
total_amount as amount,
Transaction_type as Misc_Description,
FEE_CATEGORY as Credit_Memo_Class,
FEE_TYPE as Credit_Memo_Sub_Class,
"AMAZON" as Business_Unit,
"" as Y_Tax,
"" as TAX_Amount,
"RBM" as Sales_Manager,
ifnull(Style,"") as Style,
"" as Fabric,
ifnull(COLOR,"") as Color
from program_settlement_credit_conversion_merged;


-- select * from rlm_settlements_data_table
-- where 
-- ORDER_ID = ""
-- and sku = "FLYSIDEP_WHTPK_4M_FBA"
-- and Transaction_type = "WAREHOUSE_DAMAGE"
-- and FEE_CATEGORY = ""
-- and FEE_TYPE= ""

update rlm_settlements_data_table A inner join 
(select POSTED_DATE, b.UPC, asin, order_id, sku, Transaction_type, FEE_CATEGORY, FEE_TYPE,ranking, IFNULL(C.UPC,D.UPC) AS RLM_UPC  
from 
(select a.*, b.Asin from program_settlement_credit_conversion a left join product_data b using(sku)) A
left join 
(select distinct asin, PRODUCT_ID AS UPC from product_data where Fulfillment_Channel = "AMAZON_NA" and PRODUCT_ID not like 'B%') B using(asin)
left join 
(select * from rlm_inventory where upc != 0 group by upc) C on b.upc = c.upc
left join 
(select * from rlm_inventory where upc != 0 group by upc) D on right(b.upc,length(b.upc)-1) = d.upc) B
using(posted_date, ORDER_ID, sku,Transaction_type, FEE_CATEGORY, FEE_TYPE,ranking)
set a.Status = "Complete"
where ((RLM_UPC is not null and sku !="")
or sku = "")
and TRANSACTION_TYPE != "order"
and not ((TRANSACTION_TYPE = "refund" and FEE_CATEGORY = "PRICE_TYPE" and FEE_TYPE = "Principal")
or (TRANSACTION_TYPE = "refund" and FEE_CATEGORY = "PRICE_TYPE" and FEE_TYPE like "%Tax%"))





select * from settlements_statements
where transaction_type  = "ITEM_RELATED_FEE_TYPE"