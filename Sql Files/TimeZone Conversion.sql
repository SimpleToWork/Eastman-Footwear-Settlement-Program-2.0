 select row_number() over (partition by"" order by POSTED_DATE asc, order_id, sku) as ID,
            COMPANY_NAME,POSTED_DATE, 
            CONVERT_TZ(POSTED_DATE,'+03:00','+00:00'), 
            DATE(CONVERT_TZ(POSTED_DATE,@@session.time_zone,'+00:00')),  
            POSTED_DATE_1, ifnull(ORDER_ID,"") as ORDER_ID, ifnull(sku,"") as sku ,Transaction_type, FEE_CATEGORY,FEE_TYPE,AMOUNT,QUANTITY_PURCHASED, ranking,  
            count(*), "Not Imported" as `Status` from 
            (select *, 
            row_number() over (partition by COMPANY_NAME, POSTED_DATE_1, ORDER_ID, sku, TRANSACTION_TYPE, FEE_CATEGORY,FEE_TYPE, AMOUNT, QUANTITY_PURCHASED) as ranking
             from settlements_statements
            where DATE(CONVERT_TZ(POSTED_DATE,'+03:00','+00:00')) >= "2020-10-31" and DATE(CONVERT_TZ(POSTED_DATE,'+03:00','+00:00')) <="2020-10-31" and COMPANY_NAME ="Brilliant_Footwear") A
            group by POSTED_DATE_1, ORDER_ID, sku, Transaction_type, FEE_CATEGORY,FEE_TYPE, AMOUNT,QUANTITY_PURCHASED, ranking;
        
        select @@session.time_zone;
        
			select TRANSACTION_TYPE ,FEE_CATEGORY,FEE_TYPE,  sum(amount) from
            (select POSTED_DATE as PD1,CONVERT_TZ(POSTED_DATE,'+03:00','+00:00'),  A.*  from settlements_statements A 
            where DATE(CONVERT_TZ(POSTED_DATE,'+03:00','+00:00')) >= "2020-10-31" AND DATE(CONVERT_TZ(POSTED_DATE,'+03:00','+00:00')) <= "2020-10-31" 
            AND COMPANY_NAME = "Brilliant_Footwear") A
            group by TRANSACTION_TYPE ,FEE_CATEGORY,FEE_TYPE;
            
            select SETTLEMENT_ID, SETTLEMENT_batch, sum(amount) from settlements_statements where posted_date_1 = "2020-10-31" and COMPANY_NAME = "Brilliant_Footwear" group by SETTLEMENT_ID
            ;
            
    select CONVERT_TZ(POSTED_DATE,'+00:00','+04:00'),POSTED_DATE, A.*  from settlements A
    where SETTLEMENT_ID = "13498447311"
	and left(CONVERT_TZ(POSTED_DATE,'+00:00','+04:00'),10) = "2020-10-31";
    SELECT * from mysql.time_zone_name;
    Select  SETTLEMENT_ID, SETTLEMENT_batch, PD_1, PD,PD_2, ORDER_ID, TRANSACTION_TYPE, sum(amount)  from
	(select 
    CONVERT_TZ(POSTED_DATE,'US/Eastern','UTC') as PD_1,
    CONVERT_TZ(POSTED_DATE,'+00:00','+05:00') as PD_2, 
    POSTED_DATE as PD, A.*  from settlements_statements A
    where SETTLEMENT_ID = "13498447311"
    and (order_id = '111-2107498-2429853' or
			order_id ='113-1862335-0592225')
	and left(CONVERT_TZ(POSTED_DATE,'+00:00','-03:00'),10) = "2020-10-31") A
    group by   PD_1, PD, ORDER_ID, TRANSACTION_TYPE;
    
    select * from settlements  where SETTLEMENT_ID = "13498447311"
    and (order_id = '111-2107498-2429853' or
			order_id = '113-1862335-0592225');
    
    
	SHOW CREATE TABLE settlements_statements


select SETTLEMENT_START_DATE, SETTLEMENT_END_DATE, SETTLEMENT_ID, SETTLEMENT_batch, TOTAL_AMOUNT, sum(amount) as amount, 
TOTAL_AMOUNT = sum(amount) as Matched, TOTAL_AMOUNT - sum(amount) as discrepency 
from settlements_statements
group by SETTLEMENT_ID

