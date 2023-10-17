import os
import ftplib
import pandas as pd
from datetime import datetime
import getpass
from sqlalchemy import create_engine
import Database_Modules




def engine_setup(Project_name='', hostname = '', username='', password='', port=''):
    engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{Project_name}?charset=utf8',pool_pre_ping=True, echo=False)
    return engine
def getFile(ftp, folder, filename):
    try:
        ftp.retrbinary("RETR " + filename, open(folder + filename, 'wb').write)
        print(f'{filename} Exported')
    except:
        pass
def delFile(ftp, file):
    ftp.delete(file)


def recruit_files():

    # FTP Creds
    ftp = ftplib.FTP("ftp.efny.com")
    ftp.login("RLMInvt", "472*F!Gwz")
    ftp.cwd('/')  # ------------------------------------> For testing only

    # Data Holder
    data = []
    data_output = []

    # FTP Data Files and Extraction
    ftp.dir(data.append)

    check_files = os.listdir(folder)
    for line in data:
        file = line.split(" ")[-1]
        if file not in check_files:
            print(file)
            getFile(ftp,folder, file)
        # delFile(ftp,file)

    print("Complete")


def import_data_to_sql():

    today = datetime.now().strftime('%m%d%y')
    check_files = os.listdir(folder)
    inventory_file = f'MARKETPLACE_INVENTORY_{today}'
    Table_Name = "rlm_inventory"
    for file in check_files:
        if inventory_file in file:
            engine.connect().execute(f'Drop Table if Exists {Table_Name};')
            df = pd.read_csv(f'{folder}\\{file}', delimiter="|")
            sql_types = Database_Modules.Get_SQL_Types(df).data_types
            df.to_sql(name=Table_Name, con=engine, if_exists='append', index=False, schema=Project_name,chunksize=1000, dtype=sql_types)
            print(f'{inventory_file} Imported To SQL')

    prepack_file = f'MARKETPLACE_PREPACK_{today}'
    Table_Name = "rlm_prepack"
    for file in check_files:
        if prepack_file in file:
            engine.connect().execute(f'Drop Table if Exists {Table_Name};')
            df = pd.read_csv(f'{folder}\\{file}', delimiter="|", low_memory=False)
            sql_types = Database_Modules.Get_SQL_Types(df).data_types
            df.to_sql(name=Table_Name, con=engine, if_exists='append', index=False, schema=Project_name, chunksize=1000,dtype=sql_types)
            print(f'{prepack_file} Imported To SQL')

    warehouse_info = f'WAREHOUSE_INFO'
    Table_Name = "rlm_warehouse_info"
    for file in check_files:
        if warehouse_info in file:
            engine.connect().execute(f'Drop Table if Exists {Table_Name};')
            df = pd.read_csv(f'{folder}\\{file}', delimiter=",", low_memory=False)
            df.columns = ['warehouse','Name','Address 1','City_State','Country','Zip_Postal Code','Currency','Whse Type','Company number for Stock Locator','Warehouse Locator','Trading Partner ID']
            sql_types = Database_Modules.Get_SQL_Types(df).data_types
            df.to_sql(name=Table_Name, con=engine, if_exists='append', index=False, schema=Project_name, chunksize=1000, dtype=sql_types)
            print(f'{warehouse_info} Imported To SQL')

    upc_sku_info = f'UPC_SKU_Data_{today}'
    Table_Name = "upc_sku_data"
    for file in check_files:
        if upc_sku_info in file:
            engine.connect().execute(f'Drop Table if Exists {Table_Name};')
            df = pd.read_csv(f'{folder}\\{file}', delimiter=",", low_memory=False)
            sql_types = Database_Modules.Get_SQL_Types(df).data_types
            df.to_sql(name=Table_Name, con=engine, if_exists='append', index=False, schema=Project_name, chunksize=1000,dtype=sql_types)
            print(f'{upc_sku_info} Imported To SQL')


def import_excel_files_to_sql():
    files = os.listdir(folder1)
    for file in files:

        if file == "RLM WAREHOUSES.csv":
            df = pd.read_csv(f'{folder1}\\{file}')
            Table_Name = 'rlm_warehouses_updated'
            engine.connect().execute(f'Drop Table if exists {Table_Name}; ')
            sql_types = Database_Modules.Get_SQL_Types(df).data_types
            df.to_sql(name=Table_Name, con=engine, if_exists='append', index=False, schema=Project_name,chunksize=1000, dtype=sql_types)
            print(f'Warehouse File Imported')


def rlm_data_logic():
    scripts = []


    # if
    scripts.append(f'''drop temporary table if exists RLM_Inventory_Expanded;''')
    scripts.append(f'''create temporary table if not exists RLM_Inventory_Expanded(primary key(DIVISION, STYLE, Color, Size,Pack_Dim, WAREHOUSE, SKU200))
        select
        a.DIVISION,a.`DIV NAME` as Div_Name,a.`SUB DIV` as Sub_Div,a.`SUB DIV DESC` as Sub_Div_Desc,a.WAREHOUSE,a.`WAREHOUSE NAME` as Warehouse_Name,
        a.SEASON,a.STYLE,ifnull(a.COLOR,"") as COLOR,a.`PACK/DIM` as Pack_Dim,ifnull(a.SIZE,"") as Size,a.UPC,a.`CARTON QTY` as Carton_QTY,a.`AVAIL IN STOCK` as Avail_In_Stock,
        a.WIP,a.`IN TRANSIT` as In_Transit,a.COST,ifnull(a.SKU200,"") as SKU200,
        b.SEASON as SEASON_1,b.PREPACK_CODE,b.SIZE_SCALE_1,b.SIZE_SCALE_2,
        b.SIZE_SCALE_3,b.PREPACK_DESC,b.TOTAL_UNITS,
        b.SIZE_1,b.QTY_1,b.SIZE_2,b.QTY_2,b.SIZE_3,b.QTY_3,b.SIZE_4,b.QTY_4,b.SIZE_5,b.QTY_5,
        b.SIZE_6,b.QTY_6,b.SIZE_7,b.QTY_7,b.SIZE_8,b.QTY_8,b.SIZE_9,b.QTY_9,b.SIZE_10,b.QTY_10,
        b.SIZE_11,b.QTY_11,b.SIZE_12,b.QTY_12,b.SIZE_13,b.QTY_13,b.SIZE_14,b.QTY_14,b.SIZE_15,b.QTY_15,
        b.SIZE_16,b.QTY_16,b.SIZE_17,b.QTY_17,b.SIZE_18,b.QTY_18,b.SIZE_19,b.QTY_19,b.SIZE_20,b.QTY_20,
        b.SIZE_21,b.QTY_21,b.SIZE_22,b.QTY_22,b.SIZE_23,b.QTY_23,b.SIZE_24,b.QTY_24,b.SIZE_25,b.QTY_25,
        b.SIZE_26,b.QTY_26,b.SIZE_27,b.QTY_27,b.SIZE_28,b.QTY_28,b.SIZE_29,b.QTY_29,b.SIZE_30,b.QTY_30
        from rlm_inventory A left join rlm_prepack B on a.DIVISION = b.DIVISION and a.`PACK/DIM`= b.PREPACK_CODE
        and left(a.SEASON,1) = b.SEASON;''')
    ####################################################################################################################

    scripts.append(f'''DROP TABLE IF EXISTS RLM_Inventory_Final;''')
    scripts.append(f'''create table if not exists RLM_Inventory_Final(
        DIVISION INT,
        Div_Name VARCHAR(65),
        Sub_Div INT, 
        Sub_Div_Desc VARCHAR(65),
        WAREHOUSE INT,
        Warehouse_Name VARCHAR(65),
        SEASON VARCHAR(10),
        STYLE  VARCHAR(25),
        COLOR  VARCHAR(25),
        Pack_Dim VARCHAR(10),
        SIZE VARCHAR(10),
        UPC VARCHAR(35),
        Carton_QTY INT, 
        Avail_In_Stock INT,
        In_Transit INT,
        WIP INT,
        COST FLOAT(12,2),
        SKU200 VARCHAR(25),
        SIZE_SCALE_1 VARCHAR(10),
        SIZE_SCALE_2 VARCHAR(10),
        SIZE_SCALE_3 VARCHAR(10),
        PREPACK_DESC VARCHAR(100),
        TOTAL_UNITS INT,
        TYPE VARCHAR(25),
        SIZE_DETAIL VARCHAR(25),
        QTY INT
        );''')

    ####################################################################################################################

    scripts.append(f'''DROP PROCEDURE if exists RLM_Inventory_Table_Setup;''')

    for i in range(1,30):
        script = f'''Insert into RLM_Inventory_Final Select DIVISION, Div_Name, Sub_Div, Sub_Div_Desc, 
            WAREHOUSE, Warehouse_Name, SEASON, STYLE, COLOR, Pack_Dim, SIZE, UPC, Carton_QTY, Avail_In_Stock, In_Transit,WIP,  COST, SKU200,
            SIZE_SCALE_1, SIZE_SCALE_2, SIZE_SCALE_3, PREPACK_DESC, TOTAL_UNITS, 'SIZE_{i},', SIZE_{i},QTY_{i}
            From RLM_Inventory_Expanded Where TOTAL_UNITS is not null;'''
        scripts.append(script)

    ####################################################################################################################
    scripts.append(f'''insert into RLM_Inventory_Final
        Select DIVISION, Div_Name, Sub_Div, Sub_Div_Desc,
                WAREHOUSE, Warehouse_Name, SEASON, STYLE, COLOR, Pack_Dim, SIZE, UPC, Carton_QTY, Avail_In_Stock, In_Transit,WIP, COST, SKU200,
                SIZE_SCALE_1, SIZE_SCALE_2, SIZE_SCALE_3, PREPACK_DESC, TOTAL_UNITS, "NO SIZE", SIZE, 1
                From RLM_Inventory_Expanded
                where TOTAL_UNITS is null and
                (Pack_Dim = "M" or Pack_Dim = "*N" or Pack_Dim ="W");''')
    ####################################################################################################################
    scripts.append(f'''UPDATE RLM_Inventory_Final
        SET UPC = 0
        WHERE  (Pack_Dim != "M" AND Pack_Dim != "*N" AND Pack_Dim !="W");
        ''')

    scripts.append(f'''UPDATE RLM_Inventory_Final a INNER JOIN (SELECT DISTINCT UPC, STYLE, COLOR, SIZE_DETAIL FROM
        RLM_Inventory_Final WHERE UPC != 0) b USING(STYLE, COLOR,SIZE_DETAIL)
        SET A.UPC = B.UPC
        WHERE A.UPC = 0;''')
    ####################################################################################################################
    scripts.append(f'''delete from RLM_Inventory_Final where SIZE_DETAIL is null and TYPE != "No size";''')
    scripts.append(f'''delete from RLM_Inventory_Final where QTY = 0 and TYPE != "no size";''')
    scripts.append(f'''delete from RLM_Inventory_Final where TYPE != "No size" and Size != "ppk";''')
    ####################################################################################################################
    scripts.append(f'''alter table RLM_Inventory_Final
        add column UNITS_PER_SIZE int,
        add column WIP_PER_SIZE INT,
        add column IN_TRANSIT_PER_SIZE int,
        ADD PRIMARY KEY(DIVISION, WAREHOUSE, SEASON, STYLE, COLOR, Pack_Dim, TYPE, SIZE_DETAIL,SKU200);''')
    scripts.append(f'''update RLM_Inventory_Final A
        inner join
        (select *,
        ifnull(round(Avail_In_Stock/ TOTAL_UNITS *QTY,0),Avail_In_Stock) as UPS,
        ifnull(round(WIP/ TOTAL_UNITS *QTY,0),WIP) as WPS,
        ifnull(round(IN_TRANSIT/ TOTAL_UNITS *QTY,0),IFNULL(IN_TRANSIT,0)) as TPS
          from RLM_Inventory_Final) b
        USING(DIVISION, WAREHOUSE, SEASON, STYLE, COLOR, Pack_Dim, TYPE,SIZE_DETAIL, SKU200)
        SET A.Units_Per_Size = B.UPS,
        A.WIP_PER_SIZE = B.WPS,
        A.IN_TRANSIT_PER_SIZE = B.TPS;
        ''')


    ####################################################################################################################
    df = pd.read_sql('SELECT Table_Schema, Table_Name From information_schema.tables where TABLE_SCHEMA = "eastman_footwear_amazon" and TABLE_NAME = "RLM_Warehouses_updated"', con=engine)
    scripts.append(f'''drop table if exists RLM_Warehouses;''')
    if df.shape[0] == 0:
        scripts.append(f'''Create Table if not exists RLM_Warehouses
              Select distinct WAREHOUSE,"" as EXCLUDE, "" as PRIORITY  from RLM_INVENTORY;''')
    else:
        scripts.append(f'''Create Table if not exists RLM_Warehouses
             select a.WAREHOUSE, ifnull(b.exclude,"") as `EXCLUDE`, ifnull(b.priority,"") as PRIORITY from
              (Select distinct WAREHOUSE from RLM_INVENTORY) A
              left join
              RLM_Warehouses_updated b using(WAREHOUSE)''')

    ####################################################################################################################
    scripts.append(f'''alter table rlm_inventory modify upc varchar(15);''')
    # scripts.append(f'''update PRODUCT_DATA A inner join
    #            (Select distinct upc, style, color,size, `PACK/DIM` as Case_Pack, COST  from rlm_inventory where upc != 0) b
    #            on a.PRODUCT_ID = b.upc
    #            set a.Style = b.STYLE,
    #            a.Product_Cost = b.COST,
    #            a.Case_Pack = b.Case_Pack;''')


    ####################################################################################################################
    scripts.append(f'''DROP TABLE IF EXISTS RLM_INVENTORY_FILTERED;''')
    scripts.append(f'''CREATE TABLE IF NOT EXISTS RLM_INVENTORY_FILTERED
        select * from RLM_Inventory_Final a inner join
        (select distinct WAREHOUSE from RLM_Warehouses where `Exclude` != "X") b
        using(warehouse);''')
    ####################################################################################################################
    scripts.append(f'''drop table if exists RLM_STYLE_UPC;''')
    scripts.append(f'''create table if not exists RLM_STYLE_UPC (primary key(style, COLOR,  SIZE, UPC))
        select concat(style, COLOR,  SIZE) as Lookup, a.* from
        (SELECT distinct UPC, style, COLOR,  SIZE_DETAIL as SIZE, Sub_Div, Sub_Div_Desc, left(SEASON,1) as season,
        right(SEASON,2) as YEAR from RLM_INVENTORY_FILTERED) A 
          group by style, COLOR,  SIZE, UPC;''')

    ####################################################################################################################


    Database_Modules.run_sql_scripts(engine, scripts)


def export_rlm_datas():
    export1 = f'{folder1}\RLM Inventory Data.csv'
    export2 = f'{folder1}\RLM UPCS.csv'
    export3 = f'{folder1}\RLM WAREHOUSES.csv'
    export4 = f'{folder1}\RLM WAREHOUSES ADDRESSES.csv'
    export5 = f'{folder1}\\UPC_SKU_Data.csv'
    export6 = f'{folder1}\\STYLES.csv'


    script = f'Select * from RLM_INVENTORY_FILTERED'
    df = pd.read_sql(script, con=engine)
    df.to_csv(export1, index=False)

    print('RLM Inventory Exported')

    script = f'Select * from RLM_STYLE_UPC'
    df = pd.read_sql(script, con=engine)
    df.to_csv(export2, index=False)

    print('RLM UPCS Exported')

    script = f'Select * from RLM_Warehouses order by PRIORITY'
    df = pd.read_sql(script, con=engine)
    df.to_csv(export3, index=False)

    print('RLM Warehouse Exported')

    script = f'Select * from rlm_warehouse_info order by warehouse'
    df = pd.read_sql(script, con=engine)
    df.to_csv(export4, index=False)

    print('RLM Warehouse Addresses Exported')
    #
    # script = f'Select * from UPC_SKU_data'
    # df = pd.read_sql(script, con=engine)
    # df.to_csv(export5, index=False)
    #
    # print('UPC SKU Data Exported')

    script = f'Select DISTINCT STYLE from RLM_INVENTORY_FILTERED'
    df = pd.read_sql(script, con=engine)
    df.to_csv(export6, index=False)

    print('Distinct Styles Exported')


def convert_store_codes():
    file= f'{folder1}\\FBA Store Codes.xlsx'
    xl = pd.ExcelFile(file)
    worksheet_count = len(xl.sheet_names)
    sheet_names = xl.sheet_names
    print(worksheet_count)
    df1 = pd.DataFrame()
    for sheet in xl.sheet_names:
        df = pd.read_excel(file,sheet_name=sheet)
        df.insert(0,"Store",sheet)
        df1 =df1.append(df)
    print(df1)
    df1.to_csv(f'{folder1}\\FBA Store Codes.csv', index=False)


def Run_Program():
    recruit_files()
    import_data_to_sql()
    import_excel_files_to_sql()
    rlm_data_logic()
    export_rlm_datas()
    convert_store_codes()


if __name__ == "__main__":
    folder = f'G:\\My Drive\\Simple To Work\\9 - New Projects\\Eastman Footwear\\Eastman_Footwear_FBA_Shipment_Tool\\Data Files\\RLM Data\\'
    folder1 = f'G:\\My Drive\\Simple To Work\\9 - New Projects\\Eastman Footwear\\Eastman_Footwear_FBA_Shipment_Tool\\Data Files'
    Project_name = 'eastman_footwear_amazon_seller_central'
    engine = engine_setup(Project_name=Project_name, hostname='localhost', username='root',password='Simple123', port=3306)
    Run_Program()

