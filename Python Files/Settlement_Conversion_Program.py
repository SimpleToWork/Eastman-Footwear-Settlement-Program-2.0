import getpass
import os
import pandas as pd
import numpy as np
import sqlalchemy
import datetime
from sqlalchemy import create_engine, inspect
import Database_Modules
from Database_Modules import print_color, create_folder, run_sql_scripts
from openpyxl import load_workbook
from google_sheets_api import GoogleSheetsAPI
import platform



def google_sheet_update(project_folder, program_name, method):
    text_folder = f'{project_folder}\\Text Files'
    create_folder(text_folder)
    client_secret_file = f'{project_folder}\\Text Files\\client_secret.json'
    token_file = f'{project_folder}\\Text Files\\token.json'
    sheet_id = '19FUWyywrtS4JTbOHW_GqDSEl0orqu99XCJJFa4upVlw'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    GsheetAPI = GoogleSheetsAPI(credentials_file=client_secret_file, token_file=token_file, scopes=SCOPES, sheet_id=sheet_id)

    computer_name = platform.node()
    user = getpass.getuser()
    time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_list = [time_now, computer_name, user, program_name, method, True]
    sheet_name = 'Eastman Footwear'

    GsheetAPI.insert_row_to_sheet(sheetname=sheet_name, gid=147287110,
                                  insert_range=['A', 1, "F", 1],
                                  data=[data_list])




def engine_setup(project_name='', hostname='', username='', password='', port=''):
    engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{project_name}?charset=utf8',pool_pre_ping=True, echo=False)
    return engine


def import_mexico_cheat_sheet(project_folder, engine):
    text_folder = f'{project_folder}\\Text Files'
    create_folder(text_folder)
    client_secret_file = f'{project_folder}\\Text Files\\client_secret.json'
    token_file = f'{project_folder}\\Text Files\\token.json'
    sheet_id = '1NKrBerJL848Fhb6KlS_ZBcXDitcGAXCVhlD20tOjx1o'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    GsheetAPI = GoogleSheetsAPI(credentials_file=client_secret_file, token_file=token_file, scopes=SCOPES,
                                sheet_id=sheet_id)
    df = GsheetAPI.get_data_from_sheet(sheetname='Conversions', range_name='A:D')

    scripts = []
    scripts.append(f'Drop Table if exists rlm_mxn_settlement_conversions')
    run_sql_scripts(engine=engine, scripts=scripts)

    sql_types = Database_Modules.Get_SQL_Types(df).data_types
    df.to_sql(name='rlm_mxn_settlement_conversions', con=engine, if_exists='append', index=False, schema=project_name,
                         chunksize=1000,
                         dtype=sql_types)

    print_color(f'Mexico Data Import to SQL', color='g')

    scripts = []
    scripts.append(f'Alter Table rlm_mxn_settlement_conversions add primary key(`settlement_id`)')
    run_sql_scripts(engine=engine, scripts=scripts)



def import_settlement_reference_data(engine,project_name ):
    file_to_import = f'C:\\Users\\{getpass.getuser()}\\Dropbox\\Eastman Footwear\\Settlement Program\\Settlement Reference Types\\Settlement Reference Types.csv'

    df = pd.read_csv(file_to_import)
    print(df)
    table_name= 'rlm_settlement_reference'
    if inspect(engine).has_table(table_name):
        engine.connect().execute(f'drop table if exists {table_name}')
    sql_types = Database_Modules.Get_SQL_Types(df).data_types
    Database_Modules.Change_Sql_Column_Types(engine=engine, Project_name=project_name, Table_Name=table_name, DataTypes=sql_types, DataFrame=df)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False, schema=project_name, chunksize=1000, dtype=sql_types)
    print_color(f'{table_name} imported to sql', color='g')
    scripts = []
    scripts.append(f'SET @MAX_ID=(SELECT MAX(ID) FROM rlm_settlement_reference);')
    scripts.append(f'''drop temporary table if exists rlm_settlement_existing_references;''')
    scripts.append(f'''create temporary table if not exists rlm_settlement_existing_references(primary key(Transaction_Type, Fee_Category, Fee_Type))
SELECT IFNULL(Transaction_Type,"") as Transaction_Type, IFNULL(Fee_Category,"") as Fee_Category, IFNULL(Fee_Type,"") as Fee_Type FROM rlm_settlement_reference;''')
    scripts.append(f'''drop temporary table if exists rlm_settlement_new_references;''')
    scripts.append(f'''create temporary table if not exists rlm_settlement_new_references(primary key(Transaction_Type, Fee_Category, Fee_Type))
SELECT DISTINCT `TRANSACTION-TYPE` as Transaction_Type, `AMOUNT-TYPE` as Fee_Category, `AMOUNT-DESCRIPTION` as Fee_Type FROM settlements;''')

    run_sql_scripts(engine=engine, scripts=scripts)
    df = pd.read_sql(f'''
        SELECT * FROM rlm_settlement_reference
        UNION
        SELECT @MAX_ID + ROW_NUMBER() OVER (PARTITION BY "") AS ID, A.*,
        "" AS `Credit Memo Class`, "" AS `Credit Memo Sub Class`, "" AS Notes,"" AS  `CLASS CODE`, "" AS  `SUBCLASS CODE` FROM
        (select * from rlm_settlement_new_references A left join rlm_settlement_existing_references  B using(Transaction_Type, Fee_Category, Fee_Type) 
        where B.Transaction_Type is null) A;
         ''',
                     con=engine)

    df.to_csv(file_to_import, index=False)

    print_color(f'Settlement Reference Types Exported with All Types', color='g')


def export_sku_without_upc(engine=None, start_date=None, end_date=None, export_path=None):
    create_folder(foldername=f'{export_path}\\Missing UPCs')
    scripts = []

    scripts.append(f'''update product_data a  left join all_listings B on a.account_name = b.account_name and a.sku = B.`SELLER-SKU`
        set a. upc = B.`PRODUCT-ID`
        where   B.`PRODUCT-ID` not like 'B%';
        ''')
    scripts.append(f'''drop table if exists rlm_inventory_by_upc;''')
    scripts.append(f'''create table if not exists rlm_inventory_by_upc 
        select * from rlm_inventory where upc != 0 group by upc;''')

    scripts.append(f'''create index asin on rlm_inventory_by_upc(asin);''')
    scripts.append(f'''create index upc on rlm_inventory_by_upc(upc);''')

    scripts.append(f'''drop table if exists settlement_skus;''')
    scripts.append(f'''create table if not exists settlement_skus(primary key(account_name, sku))
        select distinct account_name, sku from settlements
        where  DATE(CONVERT_TZ(`POSTED-DATE`,'US/Eastern','US/Pacific')) >=  "{start_date}"
        and DATE(CONVERT_TZ(`POSTED-DATE`,'US/Eastern','US/Pacific')) <=  "{end_date}"
        and (sku != "" and sku is not null);''')

    scripts.append(f'drop table if exists RLM_Missing_UPCS;')
    scripts.append(f'''create table if not exists RLM_Missing_UPCS (PRIMARY KEY(SKU))
        select *  from 
        (select sku, c.asin,  case when b.UPC = "" then g.Asin else b.UPC end as product_id, c.UPC,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.upc, F.upc), G.upc),H.upc),I.upc),J.upc) as RLM_UPC 
        from settlement_skus A
        left join product_data b using(account_name, sku)
        left join (select distinct asin, UPC from product_data where upc not like 'B%') C using(asin)
        --    left join (select distinct sku, fnsku as Asin from fba_inventory_event_detail) D using(sku)
        left join rlm_inventory_by_upc E on b.upc = e.upc
        left join rlm_inventory_by_upc F on C.asin= F.ASin
        left join rlm_inventory_by_upc G on right(C.upc,length(C.upc)-1) = G.upc
        left join rlm_inventory_by_upc H on case when b.upc = "" then c.Asin else b.upc end = H.asin
        left join rlm_inventory_by_upc I on REGEXP_SUBSTR(SKU,"[0-9]+")= I.UPC
        left join rlm_inventory_by_upc J on right(REGEXP_SUBSTR(SKU,"[0-9]+"),length(REGEXP_SUBSTR(SKU,"[0-9]+"))-1) = J.upc
        group by sku
        ) A
        where RLM_UPC is null
        order by sku;
        ''')

    scripts.append(f'''Update RLM_Missing_UPCS A INNER JOIN (  select SKU,
        CASE WHEN LENGTH(REGEXP_SUBSTR(SKU,"[0-9]+")) =  12 THEN REGEXP_SUBSTR(SKU,"[0-9]+") ELSE SKU END AS ADJ_SKU
        from RLM_Missing_UPCS a where upc is null or upc = "") b USING(SKU)
        INNER JOIN ( select * from rlm_inventory where upc != 0 group by upc) C ON B.ADJ_SKU = C.UPC
        set A.RLM_UPC = C.UPC;
        ''')

    scripts.append(f'delete from RLM_Missing_UPCS where rlm_upc is not null;')

    # scripts.append(f'drop table if exists RLM_INV_ADJ_Missing_UPCS')
    # scripts.append(f'''create table if not exists RLM_INV_ADJ_Missing_UPCS (PRIMARY KEY(SKU))
    #      select *  from
    #     (select sku, c.asin,  case when product_id = "" then g.Asin else product_id end as product_id, c.UPC,
    #     ifnull(ifnull(ifnull(ifnull(ifnull(E.upc, F.upc), G.upc),H.upc),I.upc),J.upc) as RLM_UPC
    #     from fba_inventory_event_detail A
	# 	left join product_data b using(sku)
	# 	left join (select distinct asin, PRODUCT_ID AS UPC from product_data where PRODUCT_ID not like 'B%') C using(asin)
	# 	left join (select distinct sku, fnsku as Asin from fba_inventory_event_detail) D using(sku)
	# 	left join (select * from rlm_inventory where upc != 0 group by upc) E using (UPC)
	# 	left join (select * from rlm_inventory where upc != 0 group by upc) F on ifnull(C.asin, D.asin)= F.ASin
	# 	left join (select * from rlm_inventory where upc != 0 group by upc) G on right(C.upc,length(C.upc)-1) = G.upc
	# 	left join (select * from rlm_inventory where upc != 0 group by upc) H on case when b.product_id = "" then c.Asin else b.product_id end = H.asin
	# 	left join (select * from rlm_inventory where upc != 0 group by upc) I on REGEXP_SUBSTR(SKU,"[0-9]+")= I.UPC
	# 	left join (select * from rlm_inventory where upc != 0 group by upc) J on right(REGEXP_SUBSTR(SKU,"[0-9]+"),length(REGEXP_SUBSTR(SKU,"[0-9]+"))-1) = J.upc
	# 	where date >=  "{start_date}"
	# 	and date <=  "{end_date}"
    #     and (sku != "" and sku is not null)
    #     and TRANSACTION_TYPE not in ("Shipments", "Receipts", "WhseTransfers", "CustomerReturns")
    #     group by sku) A
    #     where RLM_UPC is null
    #     order by sku;''')

    run_sql_scripts(engine=engine, scripts=scripts)
    print_color(f'Missing UPCs Logic Applied', color='g')

    df = pd.read_sql(f'Select * from RLM_Missing_UPCS', con=engine)
    df.to_csv(f'{export_path}\\Missing UPCs\\Settlement Missing UPCs.csv', index=False)
    print_color(f'Missing UPCs File Exported', color='g')

    # df = pd.read_sql(f'Select * from RLM_INV_ADJ_Missing_UPCS', con=engine)
    # df.to_csv(f'{export_path}\\Missing UPCs\\Inventory Adjustment Missing UPCs.csv', index=False)
    # print_color(f'Missing UPCs File Exported', color='g')


def generate_settlements_reference_table(engine=None, settlement_id=None, company_name=None):
    scripts=[]

    Table_Name = "rlm_settlements_data_table"

    engine.connect().execute(f'Drop Table if exists {Table_Name};')

    scripts.append(f'Drop Table if exists rlm_settlements_data_table;')
    scripts.append(f'''Create Table if not exists rlm_settlements_data_table
            		select row_number() over (partition by"" order by `POSTED-DATE` asc, `ORDER-ID`, sku) as ID,
		ACCOUNT_NAME, `SETTLEMENT-ID` AS SETTLEMENT_ID, CURRENCY,  `SETTLEMENT-START-DATE` as Start_Date, 
		 `SETTLEMENT-END-DATE` as End_Date, `POSTED-DATE` as POSTED_DATE, 
		ifnull(`ORDER-ID`,"") as ORDER_ID, ifnull(sku,"") as sku ,A.`TRANSACTION-TYPE` AS Transaction_type, A.`AMOUNT-TYPE` AS FEE_CATEGORY,
		A.`AMOUNT-DESCRIPTION` AS FEE_TYPE,AMOUNT,`QUANTITY-PURCHASED` AS QUANTITY_PURCHASED, ranking,
		count(*), "Not Imported" as `Status`, B.`CLASS CODE` as Credit_Memo_Class from
		(select *,
		row_number() over (partition by ACCOUNT_NAME, `SETTLEMENT-ID`, DATE(CONVERT_TZ(`POSTED-DATE`,'US/Eastern','US/Pacific')), `ORDER-ID`, 
		sku, `TRANSACTION-TYPE`, `AMOUNT-TYPE`,`AMOUNT-DESCRIPTION`, AMOUNT, `QUANTITY-PURCHASED`) as ranking
		from  settlements

		where 
		 `SETTLEMENT-ID` = "{settlement_id}"
		and ACCOUNT_NAME = "{company_name}"
		 ) A
		left join rlm_settlement_reference B ON 
		IFNULL(A.`TRANSACTION-TYPE`,"")= iFNULL(b.Transaction_type,"") AND  IFNULL(a.`AMOUNT-TYPE`,"")=  IFNULL(b.FEE_CATEGORY,"") and  IFNULL(a.`AMOUNT-DESCRIPTION`,"")=  IFNULL(b.FEE_TYPE,"")
		group by `SETTLEMENT-ID`, `POSTED-DATE` , `ORDER-ID`, sku, `TRANSACTION-TYPE`, `AMOUNT-TYPE`,`AMOUNT-DESCRIPTION`, AMOUNT,`QUANTITY-PURCHASED`, ranking;
        ''')



    scripts.append(f'alter table rlm_settlements_data_table modify column id int auto_increment primary key;')
    scripts.append(f'drop table if exists rlm_mxn_settlement_conversions;')
    # scripts.append(f'''create table if not exists rlm_mxn_settlement_conversions(
    #         settlement_id bigint,
    #         exchange_value decimal(20,10),
    #         primary key(settlement_id));''')
    #
    #
    # scripts.append(f'''insert into rlm_mxn_settlement_conversions values
    # ('14980914411',	'17.83794208'),
    # ('17946377551',	'17.87619737'),
    # ('18026964631',	'17.56010511'),
    # ('18113697851',	'17.40154556'),
    # ('18194688891',	'17.39091731'),
    # ('18282034011',	'17.25520525'),
    # ('18365151091',	'17.59194528'),
    # ('18451175181',	'17.36609626'),
    # ('18540833851',	'17.29806043'),
    # ('18632345621',	'17.37202966'),
    # ('18723060791',	'17.70046457'),
    # ('18812755281',	'18.26195038')
    # ; ''')

    run_sql_scripts(engine=engine, scripts=scripts)



    print_color(f'RLM Settlements Reference table Updated', color='g')


def export_settlement_data_daily(engine=None, settlement_id=None, export_path=None, account =None):
    create_folder(foldername=f'{export_path}\\Settlement Data For Reference')
    date_value = datetime.datetime.now().strftime('%Y-%m-%d')

    start_date = pd.read_sql(f'select min(Start_Date) as min_date from rlm_settlements_data_table', con=engine)['min_date'].iloc[0].strftime('%Y-%m-%d')
    end_date = pd.read_sql(f'select max(End_Date) as max_date from rlm_settlements_data_table;', con=engine)['max_date'].iloc[0].strftime('%Y-%m-%d')


    script = f'''Select * from settlements a  left join rlm_settlement_reference B ON 
            IFNULL(A.`TRANSACTION-TYPE`,"")= iFNULL(b.Transaction_type,"") AND  IFNULL(a.`AMOUNT-TYPE`,"")=  IFNULL(b.FEE_CATEGORY,"") and  IFNULL(a.`AMOUNT-DESCRIPTION`,"")=  IFNULL(b.FEE_TYPE,"")
        where  `SETTLEMENT-ID` = "{settlement_id}" '''
    print(script)
    df = pd.read_sql(script, con=engine)
    df.to_csv(f'{export_path}\\Settlement Data For Reference\\Settlements Data {start_date} - {end_date} {account} {settlement_id}.csv', index=False)

    print_color(f'Raw Settlement Data For {settlement_id} Exported', color='g')

    # DATE(CONVERT_TZ(POSTED_DATE, 'US/Eastern', 'US/Pacific')) >= "{start_date}"
    # AND
    # DATE(CONVERT_TZ(POSTED_DATE, 'US/Eastern', 'US/Pacific')) <= "{end_date}"


def sales_files_logic(engine=None):
    scripts = []
    scripts.append(f'drop table if exists program_settlement_order_conversion;')
    scripts.append(f'''create table if not exists program_settlement_order_conversion(primary key(company_name, order_id, sku))
        select ACCOUNT_NAME as company_name, SETTLEMENT_ID, CURRENCY,  ifnull(ORDER_ID,"") as Order_ID, SKU, POSTED_DATE,Start_Date,End_Date,
        sum(case when FEE_TYPE in  ("Principal") then QUANTITY_PURCHASED else null end ) as quantity,
        sum(AMOUNT) as total_amount,
        sum(case when FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal" then AMOUNT 
                 when TRANSACTION_TYPE  in ("Liquidations", "Liquidations Adjustments") and FEE_CATEGORY = "PRICE_TYPE" AND FEE_TYPE ="Principal" then AMOUNT     
        else 0 end) as Principal,
        sum(case when FEE_TYPE like '%tax%' then amount else 0 end) as Tax,
        b.`FULFILLMENT-CHANNEL`
        from rlm_settlements_data_table a
        left join (select distinct `AMAZON-ORDER-ID` as  order_id, sku, `FULFILLMENT-CHANNEL` from all_orders) b using(order_id, sku)
        where status = "Not Imported"
        and TRANSACTION_TYPE not in ("Payable to Amazon", "Current Reserve Amount", "Previous Reserve Amount Balance")
        and TRANSACTION_TYPE in ("Order", "TBYB Order Payment", "Liquidations", "Liquidations Adjustments")
        and `FULFILLMENT-CHANNEL` = "Amazon"
        group by ACCOUNT_NAME, ORDER_ID, SKU;''')


    scripts.append(f'drop table if exists program_settlement_order_conversion_fbm;')
    scripts.append(f'''create table if not exists program_settlement_order_conversion_fbm(primary key(company_name, order_id, sku))
            select ACCOUNT_NAME as company_name, SETTLEMENT_ID, CURRENCY,  ifnull(ORDER_ID,"") as Order_ID, SKU, POSTED_DATE,Start_Date,End_Date,
            sum(case when FEE_TYPE in  ("Principal") then QUANTITY_PURCHASED else null end ) as quantity,
            sum(AMOUNT) as total_amount,
             sum(case when FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal" then AMOUNT 
                 when TRANSACTION_TYPE  in ("Liquidations", "Liquidations Adjustments") and FEE_CATEGORY = "PRICE_TYPE" AND FEE_TYPE ="Principal" then AMOUNT     
                else 0 end) as Principal,
            sum(case when FEE_TYPE like '%tax%' then amount else 0 end) as Tax,
            b.`FULFILLMENT-CHANNEL` 
            from rlm_settlements_data_table a
            left join (select distinct  `AMAZON-ORDER-ID` as  order_id, sku, `FULFILLMENT-CHANNEL`  from all_orders) b using(order_id, sku)
            where status = "Not Imported"
            and TRANSACTION_TYPE not in ("Payable to Amazon", "Current Reserve Amount", "Previous Reserve Amount Balance")
            and TRANSACTION_TYPE  in ("Order", "TBYB Order Payment")
            and `FULFILLMENT-CHANNEL`  = "Merchant"
            group by ACCOUNT_NAME, ORDER_ID, SKU;''')

    scripts.append(f'drop table if exists program_settlement_order_conversion_merged;')
    scripts.append(f'''create table if not exists program_settlement_order_conversion_merged(primary key(company_name, order_id, sku))
            select
            b.UPC as product_id,
            ifnull(ifnull(ifnull(ifnull(ifnull(E.upc, F.upc), G.upc),H.upc),I.upc),J.upc) as RLM_UPC ,
            a.*,
            ifnull(ifnull(ifnull(ifnull(ifnull(E.DIVISION, F.DIVISION), G.DIVISION),H.DIVISION),I.DIVISION),J.DIVISION) as DIVISION ,
            ifnull(ifnull(ifnull(ifnull(ifnull(E.`DIV NAME`, F.`DIV NAME`), G.`DIV NAME`),H.`DIV NAME`),I.`DIV NAME`),J.`DIV NAME`) as `DIV NAME` ,
            ifnull(ifnull(ifnull(ifnull(ifnull(E.`SUB DIV`, F.`SUB DIV`), G.`SUB DIV`),H.`SUB DIV`),I.`SUB DIV`),J.`SUB DIV`) as `SUB DIV` ,
            ifnull(ifnull(ifnull(ifnull(ifnull(E.`SUB DIV DESC`, F.`SUB DIV DESC`), G.`SUB DIV DESC`),H.`SUB DIV DESC`),I.`SUB DIV DESC`),J.`SUB DIV DESC`) as `SUB DIV DESC` ,
            ifnull(ifnull(ifnull(ifnull(ifnull(E.WAREHOUSE, F.WAREHOUSE), G.WAREHOUSE),H.WAREHOUSE),I.WAREHOUSE),J.WAREHOUSE) as WAREHOUSE ,
            ifnull(ifnull(ifnull(ifnull(ifnull(E.`WAREHOUSE NAME`, F.`WAREHOUSE NAME`), G.`WAREHOUSE NAME`),H.`WAREHOUSE NAME`),I.`WAREHOUSE NAME`),J.`WAREHOUSE NAME`) as `WAREHOUSE NAME` ,
            ifnull(ifnull(ifnull(ifnull(ifnull(E.SEASON, F.SEASON), G.SEASON),H.SEASON),I.SEASON),J.SEASON) as SEASON ,
            ifnull(ifnull(ifnull(ifnull(ifnull(ifnull(E.STYLE, F.STYLE), G.STYLE),H.STYLE),I.STYLE),J.STYLE), A.sku) as STYLE ,
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
            left join (select distinct asin, UPC from product_data where upc not like 'B%') C using(asin)
            -- 	left join (select distinct sku, fnsku as Asin from fba_inventory_event_detail) D using(sku)
            left join (select * from rlm_inventory where upc != 0 group by upc) E on C.asin= e.ASin
            left join (select * from rlm_inventory where upc != 0 group by upc) F ON c.UPC = f.UPC
            left join (select * from rlm_inventory where upc != 0 group by upc) G on right(C.upc,length(C.upc)-1) = G.upc
            left join (select * from rlm_inventory where upc != 0 group by upc) H on case when b.upc = "" then c.Asin else b.upc end = H.asin
            left join (select * from rlm_inventory where upc != 0 group by upc) I on REGEXP_SUBSTR(SKU,"[0-9]+")= I.UPC
            left join (select * from rlm_inventory where upc != 0 group by upc) J on right(REGEXP_SUBSTR(SKU,"[0-9]+"),length(REGEXP_SUBSTR(SKU,"[0-9]+"))-1) = J.upc
            group by order_id, sku;
            ''')

    scripts.append(f'Drop table if exists RLM_Settlement_Orders_extract_usd;')
    scripts.append(f'''   Create table if not exists RLM_Settlement_Orders_extract_usd 
        select 
        "1" as Company,
        DIVISION,
        Case when company_name = "Brilliant Footwear" then  "EASFO02"
                when company_name = "Shoe Pro" then  "EASFO02"
                when company_name = "Staple On Retail Group" then  "EASFO02"
        end
        as `Customer_Number`,
         Case when company_name = "Brilliant Footwear" then  1
                when company_name = "Shoe Pro" then  2
                when company_name = "Staple On Retail Group" then  3
        END AS Store_Number,
        concat(SETTLEMENT_ID,"_",ifnull(DIVISION,""),"_",ifnull(`SUB DIV`,"")) as External_Order_Number,
        DATE_FORMAT(End_Date, "%m/%d/%Y") as Order_Date,
        DATE_FORMAT(End_Date, "%m/%d/%Y") as Start_Date,
        DATE_FORMAT(End_Date, "%m/%d/%Y") as Cancel_Date,
        82 as Terms,
        "RTG" as Routing,
        297 as Warehouse,
        "JDE" as Sales_Rep,
        concat(SETTLEMENT_ID,"_",ifnull(DIVISION,""),"_",ifnull(`SUB DIV`,""))  as Customer_PO_Number,
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
        STYLE as STYLE,
        "" as SKU200,
        COLOR as SKU300,
        -- "" as SKU300,
        `PACK/DIM` as SKU400,
        -- "" as SKU400,
        SIZE as Size_Code,
        -- "" as Size_Code,
        SUM(quantity) as Units,
        round(Principal / quantity,2) as Customer_Price,
        round(Principal / quantity,2) as Cop_Price,
        round(Principal / quantity,4) as Customer_Price_1,
        round(Principal / quantity,4) as Cop_Price_1,
        
        0 as Dollar_Discount,
        "" as Blank_7,
        "" as Blank_8,
        "" as Blank_9,
        "" as Blank_10,
        "" as Blank_11,
        "" as Blank_12,
        "" as Blank_13,
        "" as Blank_14,
        0 as Misc,
        0 as Freight,
        SUM(Tax) AS Tax,
        "" as Blank_15,
        "" as Blank_16,
        "" as Blank_17,
        "A" as Order_Origin
        from program_settlement_order_conversion_merged
        where currency = "USD"
        group by SETTLEMENT_ID, DIVISION,`SUB DIV`, STYLE,COLOR,`PACK/DIM`, SIZE,  round(Principal / quantity,2);''')

    scripts.append(f'Drop table if exists RLM_Settlement_Orders_extract_cad;')
    scripts.append(f'''Create table if not exists RLM_Settlement_Orders_extract_cad
         select 
        "1" as Company,
        DIVISION,
        Case when company_name = "Brilliant Footwear" then  "EASFO06"
                when company_name = "Shoe Pro" then  "EASFO06"
                when company_name = "Staple On Retail Group" then  "EASFO06"
        end
        as `Customer_Number`,
         Case when company_name = "Brilliant Footwear" then  1
                when company_name = "Shoe Pro" then  2
                when company_name = "Staple On Retail Group" then  3
        END AS Store_Number,
        concat(SETTLEMENT_ID,"_",ifnull(DIVISION,""),"_",ifnull(`SUB DIV`,"")) as External_Order_Number,
        DATE_FORMAT(End_Date, "%m/%d/%Y") as Order_Date,
        DATE_FORMAT(End_Date, "%m/%d/%Y") as Start_Date,
        DATE_FORMAT(End_Date, "%m/%d/%Y") as Cancel_Date,
        82 as Terms,
        "RTG" as Routing,
        297 as Warehouse,
        "JDE" as Sales_Rep,
        concat(SETTLEMENT_ID,"_",ifnull(DIVISION,""),"_",ifnull(`SUB DIV`,""))  as Customer_PO_Number,
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
        STYLE as STYLE,
        "" as SKU200,
        COLOR as SKU300,
--         "" as SKU300,
        `PACK/DIM` as SKU400,
--         "" as SKU400,
         SIZE as Size_Code,
--         "" as Size_Code,
        SUM(quantity) as Units,
        round(Principal / quantity,2) as Customer_Price,
        round(Principal / quantity,2) as Cop_Price,
        round(Principal / quantity,4) as Customer_Price_1,
        round(Principal / quantity,4) as Cop_Price_1,
        0 as Dollar_Discount,
        "" as Blank_7,
        "" as Blank_8,
        "" as Blank_9,
        "" as Blank_10,
        "" as Blank_11,
        "" as Blank_12,
        "" as Blank_13,
        "" as Blank_14,
        0 as Misc,
        0 as Freight,
        SUM(Tax) AS Tax,
        "" as Blank_15,
        "" as Blank_16,
        "" as Blank_17,
        "A" as Order_Origin
        from program_settlement_order_conversion_merged
        where currency = "CAD"
        group by SETTLEMENT_ID, DIVISION,`SUB DIV`, STYLE,COLOR,`PACK/DIM`, SIZE,   round(Principal / quantity,2);''')

    scripts.append(f'Drop table if exists RLM_Settlement_Orders_extract_mxn;')
    scripts.append(f'''Create table if not exists RLM_Settlement_Orders_extract_mxn
             select 
            "1" as Company,
            DIVISION,
            Case when company_name = "Brilliant Footwear" then  "EASFO02"
                    when company_name = "Shoe Pro" then  "EASFO02"
                    when company_name = "Staple On Retail Group" then  "EASFO02"
            end
            as `Customer_Number`,
             Case when company_name = "Brilliant Footwear" then  1
                    when company_name = "Shoe Pro" then  2
                    when company_name = "Staple On Retail Group" then  3
            END AS Store_Number,
            concat(SETTLEMENT_ID,"_",ifnull(DIVISION,""),"_",ifnull(`SUB DIV`,"")) as External_Order_Number,
            DATE_FORMAT(End_Date, "%m/%d/%Y") as Order_Date,
            DATE_FORMAT(End_Date, "%m/%d/%Y") as Start_Date,
            DATE_FORMAT(End_Date, "%m/%d/%Y") as Cancel_Date,
            82 as Terms,
            "RTG" as Routing,
            297 as Warehouse,
            "JDE" as Sales_Rep,
            concat(SETTLEMENT_ID,"_",ifnull(DIVISION,""),"_",ifnull(`SUB DIV`,""))  as Customer_PO_Number,
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
            STYLE as STYLE,
            "" as SKU200,
            COLOR as SKU300,
    --         "" as SKU300,
            `PACK/DIM` as SKU400,
    --         "" as SKU400,
             SIZE as Size_Code,
    --         "" as Size_Code,
            SUM(quantity) as Units,
            round(Principal / quantity / B.exchange_value,2) as Customer_Price,
            round(Principal / quantity/ B.exchange_value,2)  as Cop_Price,
            round(Principal / quantity / B.exchange_value,4) as Customer_Price_1,
            round(Principal / quantity/ B.exchange_value,4)  as Cop_Price_1,
            
            0 as Dollar_Discount,
            "" as Blank_7,
            "" as Blank_8,
            "" as Blank_9,
            "" as Blank_10,
            "" as Blank_11,
            "" as Blank_12,
            "" as Blank_13,
            "" as Blank_14,
            0 as Misc,
            0 as Freight,
            SUM(Tax) AS Tax,
            "" as Blank_15,
            "" as Blank_16,
            "" as Blank_17,
            "A" as Order_Origin
            from program_settlement_order_conversion_merged
            left join rlm_mxn_settlement_conversions B using(settlement_id)
            where currency = "MXN"
            group by SETTLEMENT_ID, DIVISION,`SUB DIV`, STYLE,COLOR,`PACK/DIM`, SIZE,  round(Principal / quantity / B.exchange_value,2);''')

    run_sql_scripts(engine=engine, scripts=scripts)
    print_color(f'RLM Settlements Sales Logic Applied', color='g')


def credit_files_logic(engine=None):
    scripts=[]
    scripts.append(f'drop table if exists program_settlement_credit_conversion;')
    scripts.append(f'''create table if not exists program_settlement_credit_conversion(
    primary key(Company_name, posted_date, ORDER_ID, SKU, Transaction_type, FEE_CATEGORY, FEE_TYPE, ranking))
    	select 
    	ACCOUNT_NAME as Company_name, 
    	Settlement_ID,
    	Currency,
    	ifnull(ORDER_ID,"") as Order_ID, 
    	SKU, 
    	Start_date,
    	End_date,
    	POSTED_DATE,
    	Transaction_type, FEE_CATEGORY, FEE_TYPE,
    	ranking,
    	sum(case when FEE_TYPE in  ("Principal") then QUANTITY_PURCHASED else null end ) as quantity,
    	sum(AMOUNT) as total_amount
    	from rlm_settlements_data_table
    	where status = "Not Imported"
--     	and TRANSACTION_TYPE not in ("Payable to Amazon", "Current Reserve Amount", "Previous Reserve Amount Balance")
    	and not  (TRANSACTION_TYPE in ("Order", "TBYB Order Payment")
    	and ((FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal")
    	 or ( FEE_TYPE  like "%Tax%")))
    	group by posted_date, ORDER_ID, SKU, Transaction_type, FEE_CATEGORY, FEE_TYPE, ranking;''')


    scripts.append(f'drop table if exists program_settlement_credit_conversion_merged;')
    scripts.append(f'''create table if not exists program_settlement_credit_conversion_merged(primary key(Company_name, order_id, sku, POSTED_DATE, Transaction_type, FEE_CATEGORY, FEE_TYPE, ranking))
          select
        b.upc as product_id,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.upc, F.upc), G.upc),H.upc),I.upc),J.upc) as RLM_UPC ,
        a.*,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.DIVISION, F.DIVISION), G.DIVISION),H.DIVISION),I.DIVISION),J.DIVISION) as DIVISION ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.`DIV NAME`, F.`DIV NAME`), G.`DIV NAME`),H.`DIV NAME`),I.`DIV NAME`),J.`DIV NAME`) as `DIV NAME` ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.`SUB DIV`, F.`SUB DIV`), G.`SUB DIV`),H.`SUB DIV`),I.`SUB DIV`),J.`SUB DIV`) as `SUB DIV` ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.`SUB DIV DESC`, F.`SUB DIV DESC`), G.`SUB DIV DESC`),H.`SUB DIV DESC`),I.`SUB DIV DESC`),J.`SUB DIV DESC`) as `SUB DIV DESC` ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.WAREHOUSE, F.WAREHOUSE), G.WAREHOUSE),H.WAREHOUSE),I.WAREHOUSE),J.WAREHOUSE) as WAREHOUSE ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.`WAREHOUSE NAME`, F.`WAREHOUSE NAME`), G.`WAREHOUSE NAME`),H.`WAREHOUSE NAME`),I.`WAREHOUSE NAME`),J.`WAREHOUSE NAME`) as `WAREHOUSE NAME` ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.SEASON, F.SEASON), G.SEASON),H.SEASON),I.SEASON),J.SEASON) as SEASON ,
        ifnull(ifnull(ifnull(ifnull(ifnull(ifnull(E.STYLE, F.STYLE), G.STYLE),H.STYLE),I.STYLE),J.STYLE), A.sku) as STYLE ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.COLOR, F.COLOR), G.COLOR),H.COLOR),I.COLOR),J.COLOR) as COLOR ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.`PACK/DIM`, F.`PACK/DIM`), G.`PACK/DIM`),H.`PACK/DIM`),I.`PACK/DIM`),J.`PACK/DIM`) as `PACK/DIM` ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.SIZE, F.SIZE), G.SIZE),H.SIZE),I.SIZE),J.SIZE) as SIZE ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.`CARTON QTY`, F.`CARTON QTY`), G.`CARTON QTY`),H.`CARTON QTY`),I.`CARTON QTY`),J.`CARTON QTY`) as `CARTON QTY` ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.`AVAIL IN STOCK`, F.`AVAIL IN STOCK`), G.`AVAIL IN STOCK`),H.`AVAIL IN STOCK`),I.`AVAIL IN STOCK`),J.`AVAIL IN STOCK`) as `AVAIL IN STOCK` ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.WIP, F.WIP), G.WIP),H.WIP),I.WIP),J.WIP) as WIP ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.`IN TRANSIT`, F.`IN TRANSIT`), G.`IN TRANSIT`),H.`IN TRANSIT`),I.`IN TRANSIT`),J.`IN TRANSIT`) as `IN TRANSIT` ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.COST, F.COST), G.COST),H.COST),I.COST),J.COST) as COST ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.SKU200, F.SKU200), G.SKU200),H.SKU200),I.SKU200),J.SKU200) as SKU200 ,
        ifnull(ifnull(ifnull(ifnull(ifnull(E.SIZE_SCALE, F.SIZE_SCALE), G.SIZE_SCALE),H.SIZE_SCALE),I.SIZE_SCALE),J.SIZE_SCALE) as SIZE_SCALE 
        from program_settlement_credit_conversion A 
        left join product_data b using(sku)
        left join (select distinct asin, UPC from product_data where upc not like 'B%') C using(asin)
      --   left join (select distinct sku, fnsku as Asin from fba_inventory_event_detail) D using(sku)
        left join (select * from rlm_inventory where upc != 0 group by upc) E on C.asin= e.ASin
        left join (select * from rlm_inventory where upc != 0 group by upc) F ON c.UPC = f.UPC
        left join (select * from rlm_inventory where upc != 0 group by upc) G on right(C.upc,length(C.upc)-1) = G.upc
        left join (select * from rlm_inventory where upc != 0 group by upc) H on case when b.upc = "" then c.Asin else b.upc end = H.asin
        left join (select * from rlm_inventory where upc != 0 group by upc) I on REGEXP_SUBSTR(SKU,"[0-9]+")= I.UPC
        left join (select * from rlm_inventory where upc != 0 group by upc) J on right(REGEXP_SUBSTR(SKU,"[0-9]+"),length(REGEXP_SUBSTR(SKU,"[0-9]+"))-1) = J.upc
        group by Company_name, order_id, sku, POSTED_DATE, Transaction_type, FEE_CATEGORY, FEE_TYPE, ranking;

        ''')


    scripts.append(f'Drop table if exists RLM_Settlement_credits_extract_usd;')
    scripts.append(f'''Create table if not exists RLM_Settlement_credits_extract_usd 
    		select 
    	"1" as Company,
    	ifnull(DIVISION,31) as DIVISION,
    	"" as Invoice_Number,
    	 DATE_FORMAT(End_Date, "%m/%d/%y")  as Invoice_Date,
 
    	-- left(SEASON,1) as SEASON,
        "S/00" as SEASON,
    	Case when company_name = "Brilliant Footwear" then  "EASFO02"
    		when company_name = "Shoe Pro" then  "EASFO02"
    		when company_name = "Staple On Retail Group" then  "EASFO02"
    	end
    	as `Customer_Number`,
    	 Case when company_name = "Brilliant Footwear" then  1
    				when company_name = "Shoe Pro" then  2
    				when company_name = "Staple On Retail Group" then  3
    		END AS Store_Number,
    	"" as Order_type,
    	concat(SETTLEMENT_ID,"_",ifnull(Division,31)) as Customer_Po_Number,
    	297 as Warehouse,
    	ifnull(`SUB DIV`,"000") as Sub_Division,
    	"" as Factor_Code,
    	"C" as Sales_Rea,
    	"JDE" as Sales_Rep,
    	82 as Terms_Code,
    	-sum(total_amount) as amount,
    	A.Transaction_type as Misc_Description,
    	B.`CLASS CODE` as Credit_Memo_Class,
    	B.`SUBCLASS CODE` as Credit_Memo_Sub_Class,
    	'66' as Business_Unit,
    	"" as Y_Tax,
    	"" as TAX_Amount,
    	"JDE" as Sales_Manager,
    	-- ifnull(Style,SKU) as Style,
    	cONCAT(ifnull(`SUB DIV`,"000"),"-AMZ") as Style,
    	"" as Fabric,
    	-- COLOR as Color
    	"ASST" as Color
    	from program_settlement_credit_conversion_merged A 
    	left join rlm_settlement_reference B ON 
        IFNULL(A.Transaction_type,"")= iFNULL(b.Transaction_type,"") AND  IFNULL(a.FEE_CATEGORY,"")=  IFNULL(b.FEE_CATEGORY,"") and  IFNULL(a.FEE_TYPE,"")=  IFNULL(b.FEE_TYPE,"")
    	where currency = "usd"
    	and `CLASS CODE` != "X"
    	group by ifnull(DIVISION,31), ifnull(`SUB DIV`,""), B.`CLASS CODE`, B.`SUBCLASS CODE`, A.Transaction_type;''')

    scripts.append(f'Drop table if exists RLM_Settlement_credits_extract_cad;')
    scripts.append(f'''Create table if not exists RLM_Settlement_credits_extract_cad 
    		select 
    	"1" as Company,
    	ifnull(DIVISION,31) as DIVISION,
    	"" as Invoice_Number,
    	 DATE_FORMAT(End_Date, "%m/%d/%y")  as Invoice_Date,
 
    	-- left(SEASON,1) as SEASON,
        "S/00" as SEASON,
    	Case when company_name = "Brilliant Footwear" then  "EASFO06"
    		when company_name = "Shoe Pro" then  "EASFO06"
    		when company_name = "Staple On Retail Group" then "EASFO06"
    	end
    	as `Customer_Number`,
    	 Case when company_name = "Brilliant Footwear" then  1
    				when company_name = "Shoe Pro" then  2
    				when company_name = "Staple On Retail Group" then  3
    		END AS Store_Number,
    	"" as Order_type,
    	concat(SETTLEMENT_ID,"_",ifnull(Division,31)) as Customer_Po_Number,
    	297 as Warehouse,
    	ifnull(`SUB DIV`,"000") as Sub_Division,
    	"" as Factor_Code,
    	"C" as Sales_Rea,
    	"JDE" as Sales_Rep,
    	82 as Terms_Code,
    	-sum(total_amount) as amount,
    	A.Transaction_type as Misc_Description,
    	B.`CLASS CODE` as Credit_Memo_Class,
    	B.`SUBCLASS CODE` as Credit_Memo_Sub_Class,
    	'66' as Business_Unit,
    	"" as Y_Tax,
    	"" as TAX_Amount,
    	"JDE" as Sales_Manager,
    	-- ifnull(Style,SKU) as Style,
    	cONCAT(ifnull(`SUB DIV`,"000"),"-AMZ") as Style,
    	"" as Fabric,
    	-- COLOR as Color
    	"ASST" as Color
    	from program_settlement_credit_conversion_merged A 
    	left join rlm_settlement_reference B ON 
        IFNULL(A.Transaction_type,"")= iFNULL(b.Transaction_type,"") AND  IFNULL(a.FEE_CATEGORY,"")=  IFNULL(b.FEE_CATEGORY,"") and  IFNULL(a.FEE_TYPE,"")=  IFNULL(b.FEE_TYPE,"")
    	where currency = "cad"
    	  and `CLASS CODE` != "X"
    	group by ifnull(DIVISION,31), ifnull(`SUB DIV`,""), B.`CLASS CODE`, B.`SUBCLASS CODE`, A.Transaction_type;''')

    scripts.append(f'Drop table if exists RLM_Settlement_credits_extract_mxn;')
    scripts.append(f'''Create table if not exists RLM_Settlement_credits_extract_mxn 
       		select 
       	"1" as Company,
       	ifnull(DIVISION,31) as DIVISION,
       	"" as Invoice_Number,
       	 DATE_FORMAT(End_Date, "%m/%d/%y")  as Invoice_Date,

       	-- left(SEASON,1) as SEASON,
           "S/00" as SEASON,
       	Case when company_name = "Brilliant Footwear" then  "EASFO06"
       		when company_name = "Shoe Pro" then  "EASFO06"
       		when company_name = "Staple On Retail Group" then "EASFO06"
       	end
       	as `Customer_Number`,
       	 Case when company_name = "Brilliant Footwear" then  1
       				when company_name = "Shoe Pro" then  2
       				when company_name = "Staple On Retail Group" then  3
       		END AS Store_Number,
       	"" as Order_type,
       	concat(A.SETTLEMENT_ID,"_",ifnull(Division,31)) as Customer_Po_Number,
       	297 as Warehouse,
       	ifnull(`SUB DIV`,"000") as Sub_Division,
       	"" as Factor_Code,
       	"C" as Sales_Rea,
       	"JDE" as Sales_Rep,
       	82 as Terms_Code,
       	round(-sum(total_amount) / C.exchange_value,2) as amount,
       	A.Transaction_type as Misc_Description,
       	B.`CLASS CODE` as Credit_Memo_Class,
       	B.`SUBCLASS CODE` as Credit_Memo_Sub_Class,
       	'66' as Business_Unit,
       	"" as Y_Tax,
       	"" as TAX_Amount,
       	"JDE" as Sales_Manager,
       	-- ifnull(Style,SKU) as Style,
       	cONCAT(ifnull(`SUB DIV`,"000"),"-AMZ") as Style,
       	"" as Fabric,
       	-- COLOR as Color
       	"ASST" as Color
       	from program_settlement_credit_conversion_merged A 
       	left join rlm_settlement_reference B ON 
           IFNULL(A.Transaction_type,"")= iFNULL(b.Transaction_type,"") AND  IFNULL(a.FEE_CATEGORY,"")=  IFNULL(b.FEE_CATEGORY,"") and  IFNULL(a.FEE_TYPE,"")=  IFNULL(b.FEE_TYPE,"")
       left join rlm_mxn_settlement_conversions C on A.Settlement_ID = C.Settlement_ID
       	where currency = "mxn"
       	  and `CLASS CODE` != "X"
       	group by ifnull(DIVISION,31), ifnull(`SUB DIV`,""), B.`CLASS CODE`, B.`SUBCLASS CODE`, A.Transaction_type;''')


    run_sql_scripts(engine=engine, scripts=scripts)
    print_color(f'RLM Settlements Credit Logic Applied', color='g')


def export_sales_conversion_files(engine=None, folder_path=None, sales_template=None, account=None, settlement_id=None):
    account = account.replace(" ","_")
    create_folder(foldername=f'{folder_path}\\Order Conversion Files')
    create_folder(foldername=f'{folder_path}\\Merchant Fulfilled Files')

    df_sales_template = pd.read_excel(sales_template, header=None)

    sales_df_usd = pd.read_sql(f'Select * from RLM_Settlement_Orders_extract_usd where division is not null order by division, style, SKU300',con=engine)
    sales_df_usd['Style_Year'] = sales_df_usd['Style_Year'].astype(str)
    sales_df_usd['SKU300'] = sales_df_usd['SKU300'].astype(str)
    sales_df_usd['Customer_Price'] = sales_df_usd['Customer_Price'].round(2)
    sales_df_usd['Cop_Price'] = sales_df_usd['Cop_Price'].round(2)
    sales_df_usd.columns =[x for x in range(len(sales_df_usd.columns))]

    sales_df_usd_1 = pd.read_sql( f'Select * from RLM_Settlement_Orders_extract_usd where division is null order by division, style, SKU300',con=engine)
    sales_df_usd_1['Style_Year'] = sales_df_usd_1['Style_Year'].astype(str)
    sales_df_usd_1['SKU300'] = sales_df_usd_1['SKU300'].astype(str)
    sales_df_usd_1['Customer_Price'] = sales_df_usd_1['Customer_Price'].round(2)
    sales_df_usd_1['Cop_Price'] = sales_df_usd_1['Cop_Price'].round(2)
    sales_df_usd_1.columns = [x for x in range(len(sales_df_usd.columns))]

    sales_df_cad = pd.read_sql(f'Select * from RLM_Settlement_Orders_extract_cad where division is not null order by division, style, SKU300',con=engine)
    sales_df_cad['Style_Year'] = sales_df_cad['Style_Year'].astype(str)
    sales_df_cad['SKU300'] = sales_df_cad['SKU300'].astype(str)
    sales_df_cad['Customer_Price'] = sales_df_cad['Customer_Price'].round(2)
    sales_df_cad['Cop_Price'] = sales_df_cad['Cop_Price'].round(2)
    sales_df_cad.columns = [x for x in range(len(sales_df_usd.columns))]

    sales_df_cad_1 = pd.read_sql( f'Select * from RLM_Settlement_Orders_extract_cad where division is  null order by division, style, SKU300',con=engine)
    sales_df_cad_1['Style_Year'] = sales_df_cad_1['Style_Year'].astype(str)
    sales_df_cad_1['SKU300'] = sales_df_cad_1['SKU300'].astype(str)
    sales_df_cad_1['Customer_Price'] = sales_df_cad_1['Customer_Price'].round(2)
    sales_df_cad_1['Cop_Price'] = sales_df_cad_1['Cop_Price'].round(2)
    sales_df_cad_1.columns = [x for x in range(len(sales_df_usd.columns))]

    sales_df_mxn = pd.read_sql(f'Select * from RLM_Settlement_Orders_extract_mxn where division is not null order by division, style, SKU300',con=engine)
    sales_df_mxn['Style_Year'] = sales_df_mxn['Style_Year'].astype(str)
    sales_df_mxn['SKU300'] = sales_df_mxn['SKU300'].astype(str)
    # sales_df_mxn['Customer_Price']
    sales_df_mxn['Customer_Price'] = sales_df_mxn['Customer_Price'].astype(float).round(2)
    sales_df_mxn['Cop_Price'] = sales_df_mxn['Cop_Price'].astype(float).round(2)

    sales_df_mxn.columns = [x for x in range(len(sales_df_usd.columns))]

    sales_df_mxn_1 = pd.read_sql(f'Select * from RLM_Settlement_Orders_extract_mxn where division is  null order by division, style, SKU300',con=engine)
    sales_df_mxn_1['Style_Year'] = sales_df_mxn_1['Style_Year'].astype(str)
    sales_df_mxn_1['SKU300'] = sales_df_mxn_1['SKU300'].astype(str)
    sales_df_mxn_1['Customer_Price'] = sales_df_mxn_1['Customer_Price'].round(2)
    sales_df_mxn_1['Cop_Price'] = sales_df_mxn_1['Cop_Price'].round(2)
    sales_df_mxn_1.columns = [x for x in range(len(sales_df_usd.columns))]



    final_sales_df_usd = pd.DataFrame()
    final_sales_df_usd = final_sales_df_usd.append(df_sales_template)
    final_sales_df_usd = final_sales_df_usd.append(sales_df_usd)

    final_sales_df_usd_1 = pd.DataFrame()
    final_sales_df_usd_1 = final_sales_df_usd_1.append(df_sales_template)
    final_sales_df_usd_1 = final_sales_df_usd_1.append(sales_df_usd_1)

    final_sales_df_cad = pd.DataFrame()
    final_sales_df_cad = final_sales_df_cad.append(df_sales_template)
    final_sales_df_cad = final_sales_df_cad.append(sales_df_cad)

    final_sales_df_cad_1 = pd.DataFrame()
    final_sales_df_cad_1 = final_sales_df_cad_1.append(df_sales_template)
    final_sales_df_cad_1 = final_sales_df_cad_1.append(sales_df_cad_1)

    final_sales_df_mxn = pd.DataFrame()
    final_sales_df_mxn = final_sales_df_mxn.append(df_sales_template)
    final_sales_df_mxn = final_sales_df_mxn.append(sales_df_mxn)

    final_sales_df_mxn_1 = pd.DataFrame()
    final_sales_df_mxn_1 = final_sales_df_mxn_1.append(df_sales_template)
    final_sales_df_mxn_1 = final_sales_df_mxn_1.append(sales_df_mxn_1)


    start_date = pd.read_sql(f'Select min(Start_Date) as min_date from program_settlement_credit_conversion', con=engine)['min_date'].iloc[0].strftime("%Y-%m-%d")
    end_date = pd.read_sql(f'Select max(End_Date) as max_date from program_settlement_credit_conversion', con=engine)['max_date'].iloc[0].strftime("%Y-%m-%d")

    filepath = f'{folder_path}\\Order Conversion Files\\SLS_{start_date}-{end_date}_USD_{account}_{settlement_id}_Connected.xls'
    filepath_1 = f'{folder_path}\\Order Conversion Files\\SLS_{start_date}-{end_date}_USD_{account}_{settlement_id}Non-Connected.xls'

    filepath_2 = f'{folder_path}\\Order Conversion Files\\SLS_{start_date}-{end_date}_CAD_{account}_{settlement_id}_Connected.xls'
    filepath_3 = f'{folder_path}\\Order Conversion Files\\SLS_{start_date}-{end_date}_CAD_{account}_{settlement_id}Non-Connected.xls'

    filepath_4 = f'{folder_path}\\Order Conversion Files\\SLS_{start_date}-{end_date}_MXN_{account}_{settlement_id}_Connected.xls'
    filepath_5 = f'{folder_path}\\Order Conversion Files\\SLS_{start_date}-{end_date}_MXN_{account}_{settlement_id}Non-Connected.xls'

    if final_sales_df_usd.shape[0]>2:
        final_sales_df_usd.to_excel(filepath, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")
    if final_sales_df_usd_1.shape[0] > 2:
        final_sales_df_usd_1.to_excel(filepath_1, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")
    if final_sales_df_cad.shape[0] > 2:
        final_sales_df_cad.to_excel(filepath_2, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")
    if final_sales_df_cad_1.shape[0] > 2:
        final_sales_df_cad_1.to_excel(filepath_3, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")
    if final_sales_df_mxn.shape[0] > 2:
        final_sales_df_mxn.to_excel(filepath_2, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")
    if final_sales_df_mxn_1.shape[0] > 2:
        final_sales_df_mxn_1.to_excel(filepath_3, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")

    print_color(f'Sales Conversion File Exported', color='g')



    df = pd.read_sql(f'select * from program_settlement_order_conversion_fbm WHERE CURRENCY = "USD"', con=engine)
    df = df.astype(str)
    if df.shape[0]>0:
        filepath_4 = f'{folder_path}\\Merchant Fulfilled Files\\FBM_{start_date}-{end_date}_USD_{account}_{settlement_id}_Connected.csv'
        df.to_csv(filepath_4,index=False)
        print_color(f'Merchant Fulfilled Orders Exported', color='b')
    df = pd.read_sql(f'select * from program_settlement_order_conversion_fbm WHERE CURRENCY = "CAD"', con=engine)
    df = df.astype(str)
    if df.shape[0] > 0:
        filepath_4 = f'{folder_path}\\Merchant Fulfilled Files\\FBM_{start_date}-{end_date}_CAD_{account}_{settlement_id}_Connected.csv'
        df.to_csv(filepath_4, index=False)
        print_color(f'Merchant Fulfilled Orders Exported', color='b')
    df = pd.read_sql(f'select * from program_settlement_order_conversion_fbm WHERE CURRENCY = "MXN"', con=engine)
    df = df.astype(str)
    if df.shape[0] > 0:
        filepath_4 = f'{folder_path}\\Merchant Fulfilled Files\\FBM_{start_date}-{end_date}_MXN_{account}_{settlement_id}_Connected.csv'
        df.to_csv(filepath_4, index=False)
        print_color(f'Merchant Fulfilled Orders Exported', color='b')


def export_credit_conversion_files(engine=None, folder_path=None, credit_template=None,  account=None, settlement_id=None):
    create_folder(foldername=f'{folder_path}\\Credit Conversion Files')


    df_credit_template = pd.read_excel(credit_template, header=None)

    credits_df_usd = pd.read_sql(f'Select * from RLM_Settlement_credits_extract_usd where not (style is not null and color is  null and style != "") order by division, style, Color',con=engine)
    credits_df_usd['Sub_Division'] = credits_df_usd['Sub_Division'].astype(str)
    credits_df_usd['Business_Unit'] = credits_df_usd['Business_Unit'].astype(str)
    credits_df_usd['Style'] = credits_df_usd['Style'].astype(str)
    credits_df_usd.columns =[x for x in range(26)]

    credits_df_usd_1 = pd.read_sql( f'Select * from RLM_Settlement_credits_extract_usd where (style is not null and color is  null and style != "") order by division, style, Color',con=engine)
    credits_df_usd_1['Sub_Division'] = credits_df_usd_1['Sub_Division'].astype(str)
    credits_df_usd_1['Business_Unit'] = credits_df_usd_1['Business_Unit'].astype(str)
    credits_df_usd_1['Style'] = credits_df_usd_1['Style'].astype(str)
    credits_df_usd_1.columns =[x for x in range(26)]

    credits_df_cad = pd.read_sql(f'Select * from RLM_Settlement_credits_extract_cad where not (style is not null and color is  null and style != "") order by division, style, Color',con=engine)
    credits_df_cad['Sub_Division'] = credits_df_cad['Sub_Division'].astype(str)
    credits_df_cad['Business_Unit'] = credits_df_cad['Business_Unit'].astype(str)
    credits_df_cad['Style'] = credits_df_cad['Style'].astype(str)
    credits_df_cad.columns = [x for x in range(26)]

    credits_df_cad_1 = pd.read_sql( f'Select * from RLM_Settlement_credits_extract_cad where (style is not null and color is  null and style != "") order by division, style, Color',con=engine)
    credits_df_cad_1['Sub_Division'] = credits_df_cad_1['Sub_Division'].astype(str)
    credits_df_cad_1['Business_Unit'] = credits_df_cad_1['Business_Unit'].astype(str)
    credits_df_cad_1['Style'] = credits_df_cad_1['Style'].astype(str)
    credits_df_cad_1.columns = [x for x in range(26)]

    credits_df_mxn = pd.read_sql(f'Select * from RLM_Settlement_credits_extract_mxn where not (style is not null and color is  null and style != "") order by division, style, Color',con=engine)
    credits_df_mxn['Sub_Division'] = credits_df_mxn['Sub_Division'].astype(str)
    credits_df_mxn['Business_Unit'] = credits_df_mxn['Business_Unit'].astype(str)
    credits_df_mxn['Style'] = credits_df_mxn['Style'].astype(str)
    credits_df_mxn.columns = [x for x in range(26)]

    credits_df_mxn_1 = pd.read_sql(f'Select * from RLM_Settlement_credits_extract_mxn where (style is not null and color is  null and style != "") order by division, style, Color',con=engine)
    credits_df_mxn_1['Sub_Division'] = credits_df_mxn_1['Sub_Division'].astype(str)
    credits_df_mxn_1['Business_Unit'] = credits_df_mxn_1['Business_Unit'].astype(str)
    credits_df_mxn_1['Style'] = credits_df_mxn_1['Style'].astype(str)
    credits_df_mxn_1.columns = [x for x in range(26)]



    start_date = pd.read_sql(f'Select min(posted_date) as min_date from program_settlement_credit_conversion', con=engine)['min_date'].iloc[0]
    end_date = pd.read_sql(f'Select max(posted_date) as max_date from program_settlement_credit_conversion', con=engine)['max_date'].iloc[0]

    final_credits_df_usd = pd.DataFrame()
    final_credits_df_usd = final_credits_df_usd.append(df_credit_template)
    final_credits_df_usd = final_credits_df_usd.append(credits_df_usd)

    final_credits_df_usd_1 = pd.DataFrame()
    final_credits_df_usd_1 = final_credits_df_usd_1.append(df_credit_template)
    final_credits_df_usd_1 = final_credits_df_usd_1.append(credits_df_usd_1)

    final_credits_df_cad = pd.DataFrame()
    final_credits_df_cad = final_credits_df_cad.append(df_credit_template)
    final_credits_df_cad = final_credits_df_cad.append(credits_df_cad)

    final_credits_df_cad_1 = pd.DataFrame()
    final_credits_df_cad_1 = final_credits_df_cad_1.append(df_credit_template)
    final_credits_df_cad_1 = final_credits_df_cad_1.append(credits_df_cad_1)

    final_credits_df_mxn = pd.DataFrame()
    final_credits_df_mxn = final_credits_df_mxn.append(df_credit_template)
    final_credits_df_mxn = final_credits_df_mxn.append(credits_df_mxn)

    final_credits_df_mxn_1 = pd.DataFrame()
    final_credits_df_mxn_1 = final_credits_df_mxn_1.append(df_credit_template)
    final_credits_df_mxn_1 = final_credits_df_mxn_1.append(credits_df_mxn_1)


    filepath = f'{folder_path}\\Credit Conversion Files\\CRD_{start_date}-{end_date}_USD_{account}_{settlement_id}_Connected.xls'
    filepath_1 = f'{folder_path}\\Credit Conversion Files\\CRD_{start_date}-{end_date}_USD_{account}_{settlement_id}_Non-Connected Styles.xls'
    filepath_2 = f'{folder_path}\\Credit Conversion Files\\CRD_{start_date}-{end_date}_CAD_{account}_{settlement_id}_Connected.xls'
    filepath_3 = f'{folder_path}\\Credit Conversion Files\\CRD_{start_date}-{end_date}_CAD_{account}_{settlement_id}_Non-Connected Styles.xls'
    filepath_4 = f'{folder_path}\\Credit Conversion Files\\CRD_{start_date}-{end_date}_MXN_{account}_{settlement_id}_Connected.xls'
    filepath_5 = f'{folder_path}\\Credit Conversion Files\\CRD_{start_date}-{end_date}_MXN_{account}_{settlement_id}_Non-Connected Styles.xls'

    if final_credits_df_usd.shape[0]>2:
        final_credits_df_usd.to_excel(filepath, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")
    if final_credits_df_usd_1.shape[0] > 2:
        final_credits_df_usd_1.to_excel(filepath_1, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")
    if final_credits_df_cad.shape[0] > 2:
        final_credits_df_cad.to_excel(filepath_2, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")
    if final_credits_df_cad_1.shape[0] > 2:
        final_credits_df_cad_1.to_excel(filepath_3, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")
    if final_credits_df_mxn.shape[0] > 2:
        final_credits_df_mxn.to_excel(filepath_2, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")
    if final_credits_df_mxn_1.shape[0] > 2:
        final_credits_df_mxn_1.to_excel(filepath_3, header=None, index=False, startrow=0, startcol=0, sheet_name="Sheet1")


    print_color(f'Credit Conversion File Exported', color='g')


def reconciliation_process(engine=None, settlement_id =None, account=None):
    scripts = []
    scripts.append(f'''CREATE TABLE if not exists `settlement_reconciliation` (
        Account varchar(35) not null,
       `Type` varchar(19) NOT NULL DEFAULT '',
       `settlement_id` varchar(50) NOT NULL,
       `Start_Date` date DEFAULT NULL,
       `End_Date` date DEFAULT NULL,
       `Total_Amount` decimal(34,2) DEFAULT NULL,
              `FBA_Orders_USD` decimal(34,2) DEFAULT NULL,
           `FBA_Orders_CAD` decimal(34,2) DEFAULT NULL,
           `FBA_Orders_MXN` decimal(34,2) DEFAULT NULL,
           `FBA_tax_USD` decimal(34,2) DEFAULT NULL,
           `FBA_tax_CAD` decimal(34,2) DEFAULT NULL,
           `FBA_tax_MXN` decimal(34,2) DEFAULT NULL,
           `FBM_Orders_USD` decimal(34,2) DEFAULT NULL,
           `FBM_Orders_CAD` decimal(34,2) DEFAULT NULL,
           `FBM_Orders_MXN` decimal(34,2) DEFAULT NULL,
           `FBM_tax_USD` decimal(34,2) DEFAULT NULL,
           `FBM_tax_CAD` decimal(34,2) DEFAULT NULL,
           `FBM_tax_MXN` decimal(34,2) DEFAULT NULL,
           `Credits_USD` decimal(34,2) DEFAULT NULL,
           `Credits_CAD` decimal(34,2) DEFAULT NULL,
           `Credits_MXN` decimal(34,2) DEFAULT NULL,
       `Exclusions` decimal(12,2) DEFAULT NULL,
        `Unmapped` decimal(12,2) DEFAULT NULL,
       PRIMARY KEY (TYPE, `settlement_id`)
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;''')

    scripts.append(f'Delete from Settlement_reconciliation where settlement_id= "{settlement_id}" and type= "Raw Settlement Data";')
    scripts.append(f'''insert into  Settlement_reconciliation
        select ACCOUNT_NAME as COMPANY_NAME, "Raw Settlement Data" as Type, settlement_id,Start_Date,End_Date, sum(AMOUNT) as Total_Amount, 
        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment") and  FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal" and `FULFILLMENT-CHANNEL` = "Amazon" and currency = "USD" then amount else 0 end) as FBA_Orders_USD,
        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment")  and  FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal" and `FULFILLMENT-CHANNEL` = "Amazon" and currency = "CAD" then amount else 0 end) as FBA_Orders_CAD,
        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment")  and  FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal" and `FULFILLMENT-CHANNEL` = "Amazon" and currency = "MXN" then amount else 0 end) as FBA_Orders_MXN,

        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment")  and  FEE_TYPE like '%tax%' and  currency = "USD" and `FULFILLMENT-CHANNEL` = "Amazon" then amount else 0 end) as FBA_tax_USD,
        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment") and   FEE_TYPE like '%tax%' and currency = "CAD" and `FULFILLMENT-CHANNEL` = "Amazon" then amount else 0 end) as FBA_tax_CAD,
        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment") and   FEE_TYPE like '%tax%' and currency = "MXN" and `FULFILLMENT-CHANNEL` = "Amazon" then amount else 0 end) as FBA_tax_MXN,

        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment") and  FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal" and `FULFILLMENT-CHANNEL` = "Merchant" and currency = "USD" then amount else 0 end) as FBM_Orders_USD,
        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment") and  FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal" and `FULFILLMENT-CHANNEL` = "Merchant" and currency = "CAD" then amount else 0 end) as FBM_Orders_CAD,
        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment") and  FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal" and `FULFILLMENT-CHANNEL` = "Merchant" and currency = "MXN" then amount else 0 end) as FBM_Orders_MXN,

        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment") and  FEE_TYPE like '%tax%' and  currency = "USD" and `FULFILLMENT-CHANNEL` = "Merchant" then amount else 0 end) as FBM_tax_USD,
        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment") and   FEE_TYPE like '%tax%' and currency = "CAD" and `FULFILLMENT-CHANNEL` = "Merchant" then amount else 0 end) as FBM_tax_CAD,
        sum(case when TRANSACTION_TYPE in ("Order", "TBYB Order Payment") and   FEE_TYPE like '%tax%' and currency = "MXN" and `FULFILLMENT-CHANNEL` = "Merchant" then amount else 0 end) as FBM_tax_MXN,

        SUM(case when TRANSACTION_TYPE not in ("Payable to Amazon", "Current Reserve Amount", "Previous Reserve Amount Balance") 
        and Credit_Memo_Class != "X" 
        and not (TRANSACTION_TYPE in ("Order", "TBYB Order Payment")
        and ((FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal")
        or ( FEE_TYPE  like "%Tax%"))) and  currency = "USD" then amount else 0 end) as Credits_USD,
        SUM(case when TRANSACTION_TYPE not in ("Payable to Amazon", "Current Reserve Amount", "Previous Reserve Amount Balance") 
        and Credit_Memo_Class != "X" 
        and not (TRANSACTION_TYPE in ("Order", "TBYB Order Payment")
        and ((FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal")
        or ( FEE_TYPE  like "%Tax%"))) and  currency = "CAD" then amount else 0 end) as Credits_CAD,
        
            SUM(case when TRANSACTION_TYPE not in ("Payable to Amazon", "Current Reserve Amount", "Previous Reserve Amount Balance") 
        and Credit_Memo_Class != "X" 
        and not (TRANSACTION_TYPE in ("Order", "TBYB Order Payment")
        and ((FEE_CATEGORY = "ItemPrice" AND FEE_TYPE ="Principal")
        or ( FEE_TYPE  like "%Tax%"))) and  currency = "MXN" then amount else 0 end) as Credits_MXN,
      
      
        sum(case when TRANSACTION_TYPE  in ("Payable to Amazon", "Current Reserve Amount", "Previous Reserve Amount Balance") or  Credit_Memo_Class = "X" then amount else 0 end) as Exclusions,
        sum(case when Credit_Memo_Class is null then amount else 0 end) as Unmapped
        from rlm_settlements_data_table A 
        left join (select distinct `amazon-order-id` as  order_id, sku, `FULFILLMENT-CHANNEL` from all_orders) b using(order_id, sku);''')

    scripts.append(f'SET @SETTLEMENT_ID = (SELECT DISTINCT SETTLEMENT_ID FROM rlm_settlements_data_table);')
    scripts.append(f'SET @Start_Date = (SELECT DISTINCT Start_Date FROM rlm_settlements_data_table);')
    scripts.append(f'SET @End_Date = (SELECT DISTINCT End_Date FROM rlm_settlements_data_table);')

    scripts.append(
        f'Delete from Settlement_reconciliation where settlement_id= "{settlement_id}" and type=  "LOGIC";')
    scripts.append(f'''insert into  Settlement_reconciliation
        SELECT "{account}", "LOGIC", @SETTLEMENT_ID, @Start_Date, @End_Date, 
        SUM(AMOUNT) as Total_amount,
        SUM(CASE WHEN TYPE = "FBA_Orders_USD" THEN AMOUNT ELSE 0 END) AS FBA_Orders_USD,
        SUM(CASE WHEN TYPE = "FBA_Orders_CAD" THEN AMOUNT ELSE 0 END) AS FBA_Orders_CAD,
        SUM(CASE WHEN TYPE = "FBA_Orders_MXN" THEN AMOUNT ELSE 0 END) AS FBA_Orders_MXN,
 
        SUM(CASE WHEN TYPE = "FBA_tax_USD" THEN AMOUNT ELSE 0 END) AS FBA_tax_USD,
        SUM(CASE WHEN TYPE = "FBA_tax_CAD" THEN AMOUNT ELSE 0 END) AS FBA_tax_CAD,
        SUM(CASE WHEN TYPE = "FBA_tax_MXN" THEN AMOUNT ELSE 0 END) AS FBA_tax_MXN,
 
        SUM(CASE WHEN TYPE = "FBM_Orders_USD" THEN AMOUNT ELSE 0 END) AS FBM_Orders_USD,
        SUM(CASE WHEN TYPE = "FBM_Orders_CAD" THEN AMOUNT ELSE 0 END) AS FBM_Orders_CAD,
        SUM(CASE WHEN TYPE = "FBM_Orders_MXN" THEN AMOUNT ELSE 0 END) AS FBM_Orders_MXN,
 
        SUM(CASE WHEN TYPE = "FBM_tax_USD" THEN AMOUNT ELSE 0 END) AS FBM_tax_USD,
        SUM(CASE WHEN TYPE = "FBM_tax_CAD" THEN AMOUNT ELSE 0 END) AS FBM_tax_CAD,
        SUM(CASE WHEN TYPE = "FBM_tax_MXN" THEN AMOUNT ELSE 0 END) AS FBM_tax_MXN,
 
        SUM(CASE WHEN TYPE = "Credits_USD" THEN AMOUNT ELSE 0 END) AS Credits_USD,
        SUM(CASE WHEN TYPE = "Credits_CAD" THEN AMOUNT ELSE 0 END) AS Credits_CAD,
        SUM(CASE WHEN TYPE = "Credits_MXN" THEN AMOUNT ELSE 0 END) AS Credits_MXN,
 
        0 AS EXCLUSIONS,
        0 as UNMAPPED
        FROM
        (select "FBA_Orders_USD" AS TYPE, sum(Customer_Price_1* Units) AS AMOUNT from RLM_Settlement_Orders_extract_usd
        union
        select "FBA_Orders_CAD", sum(Customer_Price_1* Units) from RLM_Settlement_Orders_extract_cad
        union
        select "FBA_Orders_MXN", sum(Customer_Price_1* Units) from RLM_Settlement_Orders_extract_mxn
        union
        select "FBA_tax_USD", sum(Tax) from RLM_Settlement_Orders_extract_usd
        union
        select "FBA_tax_CAD", sum(Tax) from RLM_Settlement_Orders_extract_cad
        union
        select "FBA_tax_MXN", sum(Tax) from RLM_Settlement_Orders_extract_mxn
        union
        select "FBM_Orders_USD", sum(Principal) from program_settlement_order_conversion_fbm where CURRENCY = "usd"
        union
        select "FBM_Orders_CAD", sum(Principal) from program_settlement_order_conversion_fbm where CURRENCY = "cad"
        union
        select "FBM_Orders_MXN", sum(Principal) from program_settlement_order_conversion_fbm where CURRENCY = "mxn"
        union
        select "FBM_tax_USD", sum(Tax) from program_settlement_order_conversion_fbm where CURRENCY = "usd"
        union
        select "FBM_tax_CAD", sum(Tax) from program_settlement_order_conversion_fbm where CURRENCY = "cad"
        union
        select "FBM_tax_MXN", sum(Tax) from program_settlement_order_conversion_fbm where CURRENCY = "mxn"
        union
        select "Credits_USD", -sum(amount) from   RLM_Settlement_credits_extract_usd        
        union
        select "Credits_CAD", -sum(amount) from RLM_Settlement_credits_extract_cad
        union
        select "Credits_MXN", -sum(amount) from RLM_Settlement_credits_extract_mxn
        ) A;''')

    run_sql_scripts(engine=engine, scripts=scripts)


def export_reconciliation_list(engine=None, export_folder=None):
    # create_folder(export_folder)
    df = pd.read_sql(f'Select * from Settlement_reconciliation ORDER BY settlement_id;', con=engine)
    file_name = f'{export_folder}\\Settlement_Reconciliations.csv'
    df.to_csv(file_name, index=False)

    print_color(f'Data Exported to {file_name}', color='p')


def generate_inventory_reference_table(engine=None, company_name=None, start_date=None, end_date=None):
    scripts = []

    Table_Name = "rlm_inventory_event_detail_data_table"
    # script = f'SELECT Table_Schema, Table_Name From information_schema.tables where TABLE_SCHEMA = "{Project_name}" and TABLE_NAME = "{Table_Name}"'
    # #
    # df1 = pd.read_sql(script, con=engine)

    scripts.append(f'Drop Table if exists rlm_inventory_event_detail_data_table')
    scripts.append(f''' Create Table if not exists rlm_inventory_event_detail_data_table
    SELECT *, "Not Imported" as Status FROM
       (Select COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION,
       ROW_NUMBER() OVER (PARTITION BY COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, 
       SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION 
       ORDER BY COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION) AS RANKING
       from fba_inventory_event_detail
        where date >= "{start_date}"
        and date <= "{end_date}"
        and COMPANY_NAME = "{company_name}"
        and TRANSACTION_TYPE not in ("Shipments", "Receipts", "VendorReturns")
    ) A
    GROUP BY  COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, 
    FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION, RANKING; 
    ''')

    run_sql_scripts(engine=engine, scripts=scripts)
    print_color(f'RLM Inventory Adjustments table Updated', color='g')


def inventory_files_logic(engine=None):
    scripts = []

    scripts.append(f'drop table if exists program_inventory_adjustment_conversion;')
    scripts.append(f'''create table if not exists program_inventory_adjustment_conversion
    select A.*, 
    case when B.asin = "" and B.product_id = "" then A.FNSKU 
    	when  B.product_id = "" then B.asin 
        else  B.product_id 
    end as Product_asin, 
    ifnull(ifnull(ifnull(ifnull(ifnull(E.upc, F.upc), G.upc),H.upc),I.upc),J.upc) as RLM_UPC,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.DIVISION, F.DIVISION), G.DIVISION),H.DIVISION),I.DIVISION),J.DIVISION) as DIVISION,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.`DIV NAME`, F.`DIV NAME`), G.`DIV NAME`),H.`DIV NAME`),I.`DIV NAME`),J.`DIV NAME`) as `DIV NAME`,
    ifnull(ifnull(ifnull(ifnull(ifnull(E. `SUB DIV`, F. `SUB DIV`), G. `SUB DIV`),H. `SUB DIV`),I. `SUB DIV`),J. `SUB DIV`) as  `SUB DIV`,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.`SUB DIV DESC`, F.`SUB DIV DESC`), G.`SUB DIV DESC`),H.`SUB DIV DESC`),I.`SUB DIV DESC`),J.`SUB DIV DESC`) as `SUB DIV DESC`,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.`WAREHOUSE NAME`, F.`WAREHOUSE NAME`), G.`WAREHOUSE NAME`),H.`WAREHOUSE NAME`),I.`WAREHOUSE NAME`),J.`WAREHOUSE NAME`) as `WAREHOUSE NAME`,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.SEASON, F.SEASON), G.SEASON),H.SEASON),I.SEASON),J.SEASON) as SEASON,
    ifnull(ifnull(ifnull(ifnull(ifnull(ifnull(E.STYLE, F.STYLE), G.STYLE),H.STYLE),I.STYLE),J.STYLE),A.sku) as STYLE,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.COLOR, F.COLOR), G.COLOR),H.COLOR),I.COLOR),J.COLOR) as COLOR,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.`PACK/DIM`, F.`PACK/DIM`), G.`PACK/DIM`),H.`PACK/DIM`),I.`PACK/DIM`),J.`PACK/DIM`) as `PACK/DIM`,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.SIZE, F.SIZE), G.SIZE),H.SIZE),I.SIZE),J.SIZE) as SIZE,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.`CARTON QTY`, F.`CARTON QTY`), G.`CARTON QTY`),H.`CARTON QTY`),I.`CARTON QTY`),J.`CARTON QTY`) as `CARTON QTY`,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.`AVAIL IN STOCK`, F.`AVAIL IN STOCK`), G.`AVAIL IN STOCK`),H.`AVAIL IN STOCK`),I.`AVAIL IN STOCK`),J.`AVAIL IN STOCK`) as `AVAIL IN STOCK`,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.WIP, F.WIP), G.WIP),H.WIP),I.WIP),J.WIP) as WIP,
    ifnull(ifnull(ifnull(ifnull(ifnull(E. `IN TRANSIT`, F. `IN TRANSIT`), G. `IN TRANSIT`),H. `IN TRANSIT`),I. `IN TRANSIT`),J. `IN TRANSIT`) as  `IN TRANSIT`,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.COST, F.COST), G.COST),H.COST),I.COST),J.COST) as COST,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.SKU200, F.SKU200), G.SKU200),H.SKU200),I.SKU200),J.SKU200) as SKU200,
    ifnull(ifnull(ifnull(ifnull(ifnull(E.SIZE_SCALE, F.SIZE_SCALE), G.SIZE_SCALE),H.SIZE_SCALE),I.SIZE_SCALE),J.SIZE_SCALE) as SIZE_SCALE
    from
    rlm_inventory_event_detail_data_table a
    left join product_data b using(sku)
    left join (select distinct asin, PRODUCT_ID AS UPC from product_data where PRODUCT_ID not like 'B%') C using(asin)
    left join (select distinct sku, fnsku as Asin from fba_inventory_event_detail) D using(sku)
    left join (select * from rlm_inventory where upc != 0 group by upc) E using (UPC)
    left join (select * from rlm_inventory where upc != 0 group by upc) F on ifnull(C.asin, D.asin)= F.ASin
    left join (select * from rlm_inventory where upc != 0 group by upc) G on right(C.upc,length(C.upc)-1) = G.upc
    left join (select * from rlm_inventory where upc != 0 group by upc) H on case when b.product_id = "" then c.Asin else b.product_id end = H.asin
    left join (select * from rlm_inventory where upc != 0 group by upc) I on REGEXP_SUBSTR(SKU,"[0-9]+")= I.UPC
    left join (select * from rlm_inventory where upc != 0 group by upc) J on right(REGEXP_SUBSTR(SKU,"[0-9]+"),length(REGEXP_SUBSTR(SKU,"[0-9]+"))-1) = J.upc
    where A.status = "Not Imported"
    GROUP BY COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION;''')

    scripts.append(f'Drop table if exists RLM_inventory_adjustments_extract;')
    scripts.append(f'''Create table if not exists RLM_inventory_adjustments_extract       
            select 
            DIVISION,
            left(SEASON,1) as SEASON,
            right(SEASON,2) as SEASON_YEAR,
            Style,
            SKU200,
            COLOR as `Color Code`,
            `PACK/DIM` as PACK,
            SIZE_SCALE,
            SIZE,
            sum(QUANTITY) as Units,
            case when TRANSACTION_TYPE = "VendorReturns" then 297 else 297 end as WAREHOUSE_IN,
            case when TRANSACTION_TYPE = "VendorReturns" then 297 else 297 end as WAREHOUSE_OUT
            from program_inventory_adjustment_conversion
            group by DIVISION, SEASON, Style, COLOR,  SIZE,  `PACK/DIM`, SIZE_SCALE
            order by division, Style, color;''')

    # scripts.append(f'''update rlm_inventory_event_detail_data_table A inner join
    #     program_inventory_adjustment_conversion using(COMPANY_NAME, DATE, TRANSACTION_TYPE, FNSKU, SKU, FULFILLMENT_CENTER_ID, QUANTITY, DISPOSITION)
    #     set a.status ="Complete"''')

    run_sql_scripts(engine=engine, scripts=scripts)
    print_color(f'RLM Inventory Adjustment Logic Applied', color='g')


def export_inventory_adjustment_conversion_files(engine=None, folder_path=None, start_date=None, end_date=None, account=None):

    col_list = [f"Column {x}" for x in range(1,80)]
    df = pd.DataFrame(tuple(col_list))
    df = df.transpose()


    # start_date =  pd.read_sql(f'Select min(date) as min_date from program_inventory_adjustment_conversion', con=engine)[ 'min_date'].iloc[0]
    # end_date = pd.read_sql(f'Select max(date) as max_date from program_inventory_adjustment_conversion', con=engine)['max_date'].iloc[0]
    print(start_date, end_date)

    inv_adj_df = pd.read_sql(f'Select * from RLM_inventory_adjustments_extract where division is not null order by division, Style,  `Color Code`', con=engine)
    inv_adj_df.columns = ['Division', 'Season', 'Season Year', 'Style#', 'SKU200',
                          'Color code','Pack', 'Size Scale', 'Sizes', 'Units', 'Warehouse', 'Warehouse']

    if inv_adj_df.shape[0] > 0:
        filename = f'{folder_path}\\{account} Inventory Adjustment Conversion File {start_date} - {end_date} Connected.xls'
        writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        inv_adj_df.to_excel(writer, sheet_name='Sheet1', index=False,  startrow=1)
        df.to_excel(writer, sheet_name='Sheet1', index=False,  startrow=0, header=False)
        writer.save()


    inv_adj_df = pd.read_sql( f'Select * from RLM_inventory_adjustments_extract where division is null order by division, Style,  `Color Code`',con=engine)
    inv_adj_df.columns = ['Division', 'Season', 'Season Year', 'Style#', 'SKU200',
                          'Color code','Pack', 'Size Scale', 'Sizes', 'Units', 'Warehouse', 'Warehouse']

    inv_adj_df= inv_adj_df.astype(str)
    if inv_adj_df.shape[0]>0:
        filename = f'{folder_path}\\{account} Inventory Adjustment Conversion File {start_date} - {end_date} Non Connected.xls'
        writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        inv_adj_df.to_excel(writer, sheet_name='Sheet1', index=False, startrow=1)
        df.to_excel(writer, sheet_name='Sheet1', index=False, startrow=0, header=False)
        writer.save()


def generate_files(engine, start_date, export_path, sales_template, credit_template):
    settlement_folder_export = f'{export_path}\\Settlement Exports'
    create_folder(settlement_folder_export)
    reconciliation_folder_export = f'{export_path}\\Reconciliations'
    create_folder(reconciliation_folder_export)


    inventory_adjustment_export = f'{export_path}\\Inventory Adjustment Conversion Files'
    create_folder(inventory_adjustment_export)


    accounts = pd.read_sql(f'Select distinct account_NAME AS account from SETTLEMENTS', con=engine)['account'].unique().tolist()


    for each_account in accounts:
        settlement_data_folder = f'{export_path}\\Settlement Data For Reference'
        existing_files = os.listdir(settlement_data_folder)
        existing_files = [x for x in existing_files if 'ini' not in x]
        print(existing_files)
        print(each_account)

        existing_settlements = [x.split(" ")[-1].split(".csv")[0] for x in existing_files if each_account in x]
        # print(existing_settlements)
        # existing_settlements=[]
        existing_settlements_str= str(set(existing_settlements)).replace("{","").replace("}","")
        # print(existing_settlements)
        # print(each_account)

        if len(existing_settlements)>0:
            script = f'''Select distinct `SETTLEMENT-ID`, `SETTLEMENT-START-DATE`, `SETTLEMENT-END-DATE`  from settlements
                    where account_name = '{each_account}' and status = "POSTED"
                        and  DATE(CONVERT_TZ(`POSTED-DATE`,'US/Eastern','US/Pacific')) >=  '{start_date}'
                        and  `SETTLEMENT-ID` not in ({existing_settlements_str})

                          order by `SETTLEMENT-END-DATE`
                         ;'''

        else:
            script = f'''Select distinct `SETTLEMENT-ID`, `SETTLEMENT-START-DATE`, `SETTLEMENT-END-DATE`  from settlements
                where account_name = '{each_account}' and status = "POSTED"
                and  DATE(CONVERT_TZ(`POSTED-DATE`,'US/Eastern','US/Pacific')) >=  '{start_date}'

                 order by `SETTLEMENT-END-DATE`;
                   '''
        print(script)
        settlement_ids = pd.read_sql(script, con=engine)
        print_color(settlement_ids, color='g')

        for i in range(settlement_ids.shape[0]):
            settlement_id = settlement_ids['SETTLEMENT-ID'].iloc[i]
            start_date = settlement_ids['SETTLEMENT-START-DATE'].iloc[i]
            end_date = settlement_ids['SETTLEMENT-END-DATE'].iloc[i]
            generate_settlements_reference_table(engine=engine, settlement_id=settlement_id, company_name=each_account)
            export_settlement_data_daily(engine=engine, settlement_id=settlement_id, export_path=export_path, account=each_account)
            sales_files_logic(engine=engine)
            credit_files_logic(engine=engine)
            export_sales_conversion_files(engine=engine, folder_path=settlement_folder_export, sales_template=sales_template, account=each_account, settlement_id=settlement_id)
            export_credit_conversion_files(engine=engine, folder_path=settlement_folder_export, credit_template=credit_template, account=each_account, settlement_id=settlement_id)
            reconciliation_process(engine=engine, settlement_id=settlement_id, account=each_account)
            # break

        export_reconciliation_list(engine=engine, export_folder=reconciliation_folder_export)

        # scripts = []
        # scripts.append(f'drop table if exists settlement_data_lookup;')
        # scripts.append(f'''create table if not exists settlement_data_lookup
        #     Select distinct settlement_id, SETTLEMENT_START_DATE_1, SETTLEMENT_END_DATE_1  from settlements_statements
        #     where COMPANY_NAME = 'Brilliant Footwear' and status = "POSTED"
        #     and  DATE(CONVERT_TZ(POSTED_DATE,'US/Eastern','US/Pacific')) >=  '{start_date}';''')
        #
        # scripts.append(f'set @last_lookup_date:= (select max(SETTLEMENT_END_DATE_1) from settlement_data_lookup);')
        # scripts.append(f'drop table if exists settlement_data_dates;')
        # scripts.append(f'''create table if not exists settlement_data_dates
        #     select *, datediff(end_date, Start_Date) as date_range from
        #     (Select start_date, ifnull(lead(Start_Date, 1) over (partition by "") - interval 1 day,  @last_lookup_date) as end_date
        #     from
        #     (select SETTLEMENT_START_DATE_1 + interval 1 day as Start_Date from settlement_data_lookup group by SETTLEMENT_START_DATE_1 order by SETTLEMENT_START_DATE_1) A) B
        #     order by Start_Date;''')
        #
        # run_sql_scripts(engine=engine, scripts=scripts)
        #
        # inventory_files = os.listdir(inventory_adjustment_export)
        # inventory_files = [x for x in inventory_files if each_account in x]
        # inventory_files = [x.split("Inventory Adjustment Conversion File")[-1].split(".")[0].strip().split(" ")[0] for x in inventory_files]
        # inventory_files_to_exclude = str(set(list(dict.fromkeys(inventory_files)))).replace("{","").replace("}","")
        # print(inventory_files)
        # if len(inventory_files)>0:
        #     df = pd.read_sql(f'Select * from settlement_data_dates where start_date not in ({inventory_files_to_exclude})', con=engine)
        # else:
        #     df = pd.read_sql(f'Select * from settlement_data_dates',con=engine)
        # print(df)
        #
        # for i in range(df.shape[0]):
        #     start_date = df['start_date'].iloc[i]
        #     end_date = df['end_date'].iloc[i]
        #     print(start_date, end_date)
        #     generate_inventory_reference_table(engine=engine, company_name=each_account, start_date=start_date, end_date=end_date)
        #     inventory_files_logic(engine=engine)
        #     export_inventory_adjustment_conversion_files(engine=engine, folder_path=inventory_adjustment_export,
        #                                                  start_date=start_date, end_date=end_date, account=each_account)

        # break


def run_program(project_name, start_date, export_path, sales_template, credit_template, project_folder):
    hostname = 'localhost'
    username = 'root'
    password = 'Simple123'
    port = 3306

    engine = engine_setup(project_name=project_name , hostname=hostname, username=username, password=password, port=port)

    import_settlement_reference_data(engine=engine, project_name=project_name)
    import_mexico_cheat_sheet(project_folder, engine)
    # export_sku_without_upc(engine=engine,start_date=start_date, end_date = datetime.datetime.now().strftime('%Y-%m-%d'), export_path=export_path)
    # generate_files(engine=engine, start_date =start_date, export_path=export_path, sales_template=sales_template, credit_template=credit_template)
    # google_sheet_update(project_folder=project_folder, program_name="Eastman Settlement Program", method="Settlement Conversion Program")


if __name__ == "__main__":
    project_name = 'eastman_footwear_amazon_seller_central'
    export_path = f'C:\\Users\\{getpass.getuser()}\\Dropbox\\Eastman Footwear\\Settlement Program'
    sales_template = f'G:\\My Drive\\Simple To Work\\9 - New Projects\\Eastman Footwear\\Eastman-Footwear-Settlement-Program\\Text Files\\RLM Template.xls'
    credit_template = f'G:\\My Drive\\Simple To Work\\9 - New Projects\\Eastman Footwear\\Eastman-Footwear-Settlement-Program\\Text Files\\Credit Import Template.xls'
    project_folder = f'C:\\Users\\{getpass.getuser()}\\Desktop\\New Projects\\Eastman Footwear\\Eastman-Footwear-Settlement-Program-2.0'
    start_date = "2022-04-28"

    run_program(project_name , start_date, export_path, sales_template, credit_template, project_folder)

