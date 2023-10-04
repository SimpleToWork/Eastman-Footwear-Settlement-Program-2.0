drop table if exists program_settlement_order_conversion;
  create table if not exists program_settlement_order_conversion(primary key(company_name, order_id, sku))
        select company_name, ifnull(ORDER_ID,"") as Order_ID, SKU, POSTED_DATE,
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
                        then amount else 0 end) as Misc,
                        b.FULFILLMENT_CHANNEL
        from rlm_settlements_data_table a
        left join (select distinct amazon_order_id as  order_id, sku, FULFILLMENT_CHANNEL from all_orders) b using(order_id, sku)
        where status = "Not Imported"
        and TRANSACTION_TYPE = "Order"
        group by company_name, ORDER_ID, SKU;
        
        
        
        
        
        select * from program_settlement_order_conversion where FULFILLMENT_CHANNEL is null;
        select a.order_id, a.sku,B.FULFILLMENT_CHANNEL from program_settlement_order_conversion a left join
        (select distinct amazon_order_id as  order_id, sku, FULFILLMENT_CHANNEL from all_orders) b using(order_id, sku);
        


select *  from imported_file_log
where report_name = "all orders";

select * from all_orders where amazon_order_id = "111-1904994-2374660";

drop table if exists program_settlement_order_conversion_merged;
create table if not exists program_settlement_order_conversion_merged(primary key(company_name, order_id, sku))
select
b.product_id,
ifnull(ifnull(ifnull(ifnull(ifnull(E.upc, F.upc), G.upc),H.upc),I.upc),J.upc) as RLM_UPC ,
a.*,
ifnull(ifnull(ifnull(ifnull(ifnull(E.DIVISION, F.DIVISION), G.DIVISION),H.DIVISION),I.DIVISION),J.DIVISION) as DIVISION ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.`DIV NAME`, F.`DIV NAME`), G.`DIV NAME`),H.`DIV NAME`),I.`DIV NAME`),J.`DIV NAME`) as `DIV NAME` ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.`SUB DIV`, F.`SUB DIV`), G.`SUB DIV`),H.`SUB DIV`),I.`SUB DIV`),J.`SUB DIV`) as `SUB DIV` ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.`SUB DIV DESC`, F.`SUB DIV DESC`), G.`SUB DIV DESC`),H.`SUB DIV DESC`),I.`SUB DIV DESC`),J.`SUB DIV DESC`) as `SUB DIV DESC` ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.WAREHOUSE, F.WAREHOUSE), G.WAREHOUSE),H.WAREHOUSE),I.WAREHOUSE),J.WAREHOUSE) as WAREHOUSE ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.`WAREHOUSE NAME`, F.`WAREHOUSE NAME`), G.`WAREHOUSE NAME`),H.`WAREHOUSE NAME`),I.`WAREHOUSE NAME`),J.`WAREHOUSE NAME`) as `WAREHOUSE NAME` ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.SEASON, F.SEASON), G.SEASON),H.SEASON),I.SEASON),J.SEASON) as SEASON ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.STYLE, F.STYLE), G.STYLE),H.STYLE),I.STYLE),J.STYLE) as STYLE ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.COLOR, F.COLOR), G.COLOR),H.COLOR),I.COLOR),J.COLOR) as COLOR ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.`PACK/DIM`, F.`PACK/DIM`), G.`PACK/DIM`),H.`PACK/DIM`),I.`PACK/DIM`),J.`PACK/DIM`) as `PACK/DIM` ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.SIZE, F.SIZE), G.SIZE),H.SIZE),I.SIZE),J.SIZE) as SIZE ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.`CARTON QTY`, F.`CARTON QTY`), G.`CARTON QTY`),H.`CARTON QTY`),I.`CARTON QTY`),J.`CARTON QTY`) as `CARTON QTY` ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.`AVAIL IN STOCK`, F.`AVAIL IN STOCK`), G.`AVAIL IN STOCK`),H.`AVAIL IN STOCK`),I.`AVAIL IN STOCK`),J.`AVAIL IN STOCK`) as `AVAIL IN STOCK` ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.WIP, F.WIP), G.WIP),H.WIP),I.WIP),J.WIP) as WIP ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.`IN TRANSIT`, F.`IN TRANSIT`), G.`IN TRANSIT`),H.`IN TRANSIT`),I.`IN TRANSIT`),J.`IN TRANSIT`) as `IN TRANSIT` ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.COST, F.COST), G.COST),H.COST),I.COST),J.COST) as COST ,
ifnull(ifnull(ifnull(ifnull(ifnull(E.SKU200, F.SKU200), G.SKU200),H.SKU200),I.SKU200),J.SKU200) as SKU200 
from program_settlement_order_conversion A 
left join product_data b using(sku)
left join (select distinct asin, PRODUCT_ID AS UPC from product_data where PRODUCT_ID not like 'B%') C using(asin)
left join (select distinct sku, fnsku as Asin from fba_inventory_event_detail) D using(sku)
left join (select * from rlm_inventory where upc != 0 group by upc) E using (UPC)
left join (select * from rlm_inventory where upc != 0 group by upc) F on ifnull(C.asin, D.asin)= F.ASin
left join (select * from rlm_inventory where upc != 0 group by upc) G on right(C.upc,length(C.upc)-1) = G.upc
left join (select * from rlm_inventory where upc != 0 group by upc) H on case when b.product_id = "" then c.Asin else b.product_id end = H.asin
left join (select * from rlm_inventory where upc != 0 group by upc) I on REGEXP_SUBSTR(SKU,"[0-9]+")= I.UPC
left join (select * from rlm_inventory where upc != 0 group by upc) J on right(REGEXP_SUBSTR(SKU,"[0-9]+"),length(REGEXP_SUBSTR(SKU,"[0-9]+"))-1) = J.upc
group by order_id, sku;

				



drop temporary table rlm_upc_connections;
create temporary table rlm_upc_connections
Select * from 
(select A.*,REGEXP_SUBSTR(SKU,"[0-9]+") as clena_up_sku,
ifnull(C.asin, D.asin) as asin, C.upc, 
right(C.upc,length(C.upc)-1),length(C.upc)-1,
ifnull(ifnull(ifnull(ifnull(ifnull(E.upc, F.upc), G.upc),H.upc),I.upc),J.upc) as RLM_UPC 
from (Select distinct sku from program_settlement_order_conversion) A 
left join product_data b using(sku)
left join (select distinct asin, PRODUCT_ID AS UPC from product_data where PRODUCT_ID not like 'B%') C using(asin)
left join (select distinct sku, fnsku as Asin from fba_inventory_event_detail) D using(sku)
left join (select * from rlm_inventory where upc != 0 group by upc) E using (UPC)
left join (select * from rlm_inventory where upc != 0 group by upc) F on ifnull(C.asin, D.asin)= F.ASin
left join (select * from rlm_inventory where upc != 0 group by upc) G on right(C.upc,length(C.upc)-1) = G.upc
left join (select * from rlm_inventory where upc != 0 group by upc) H on case when b.product_id = "" then c.Asin else b.product_id end = H.asin
left join (select * from rlm_inventory where upc != 0 group by upc) I on REGEXP_SUBSTR(SKU,"[0-9]+")= I.UPC
left join (select * from rlm_inventory where upc != 0 group by upc) J on right(REGEXP_SUBSTR(SKU,"[0-9]+"),length(REGEXP_SUBSTR(SKU,"[0-9]+"))-1) = J.upc
) A;



  Drop table if exists RLM_Settlement_Orders_extract;
  Create table if not exists RLM_Settlement_Orders_extract 
        select 
        "01" as Company,
        DIVISION,
        "EASF02" as `Customer_Number`,
        1 as Store_Number,
        concat(ORDER_ID,"_",ifnull(DIVISION,"")) as External_Order_Number,
        POSTED_DATE as Order_Date,
        POSTED_DATE as Start_Date,
        POSTED_DATE as Cancel_Date,
        82 as Terms,
        "RTG" as Routing,
        297 as Warehouse,
        "RBM" as Sales_Rep,
        concat(ORDER_ID,"_",ifnull(DIVISION,""))  as Customer_PO_Number,
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

  drop temporary table if exists settlement_lookup_orders
  create temporary table settlement_lookup_orders (primary key(id))
	select A.* from rlm_settlements_data_table A inner join
        (select company_name, POSTED_DATE, b.UPC, A.asin, order_id, sku, IFNULL(IFNULL(C.UPC,D.UPC),E.upc) AS RLM_UPC
        from
        (select a.*, b.Asin from program_settlement_order_conversion a left join product_data b using(company_name, sku)) A
        left join
        (select distinct asin, PRODUCT_ID AS UPC from product_data) B using(asin)
        left join
        (select * from rlm_inventory where upc != 0 group by upc) C on b.upc = c.upc
        left join
        (select * from rlm_inventory where upc != 0 group by upc) D on right(b.upc,length(b.upc)-1) = d.upc
		left join 
		(select * from rlm_inventory where upc != 0 group by upc) E on A.asin = E.asin
		group by Order_ID, sku
        ) B
        using(ORDER_ID, sku)
        where RLM_UPC is not null
        and Transaction_type = "Order";


  update rlm_settlements_data_table A inner join
        settlement_lookup_orders b using(id)
        set a.Status = "Complete";
    