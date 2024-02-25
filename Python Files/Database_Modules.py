import os
import pandas as pd
import numpy as np
import sqlalchemy
import crayons
import time
import requests
import platform
import getpass
import json
import datetime


def record_program_performance(x, program_name, method):
    ip = requests.get('https://api.ipify.org').content.decode('utf8')

    database_name = "stw_task_manager"

    computer_name = platform.node()
    user = getpass.getuser()
    time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print_color(f'Data imported', color='g')

    url = x.webhook_url
    run_task_webhook = f'{url}/method_performance'
    print_color(f'Attempting to Hit: {run_task_webhook}')

    headers = {'Content-Type': 'application/json'}

    data = {
        'DateTime':time_now,
        'Computer':computer_name,
        'User':user,
        'Program Name':program_name,
        'Function':method,
        'Success':True
    }
    print_color(data, color='r')

    data = {"ip": ip, "data": data}

    response = requests.post(url=run_task_webhook, headers=headers, json=json.dumps(data))

    print("Request URL:", response.request.url)
    print("Request method:", response.request.method)
    print("Request headers:", response.request.headers)
    print("Request body:", response.request.body)

    print_color(f"Request status: {response.status_code}", color='g')
    print_color(f"Request content: {response.content}", color='y')


class objdict(dict):
    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise AttributeError("No such attribute: " + name)

class ProgramCredentials:
    def __init__(self, environment):
        filename = __file__
        filename = filename.replace('/', "\\")
        folder_name = '\\'.join(filename.split('\\')[:-2])

        file_name = f'{folder_name}\\credentials.json'

        f = json.load(open(file_name))

        self.username = f['username']
        self.password = f['password']
        self.hostname = f['hostname']
        self.port = f['port']
        self.project_name = f['project_name']

        self.export_path = f['export_path'].replace("%USERNAME%", getpass.getuser())
        self.sales_template = f['sales_template']
        self.credit_template = f['credit_template']
        self.project_folder = f['project_folder'].replace("%USERNAME%", getpass.getuser())
        self.start_date = f['start_date']

        self.folder = f['folder']
        self.folder1 = f['folder1']

        self.webhook_url = f['webhook_url']



    def set_attributes(self, params):

        params = objdict(params)
        for key, val in params.items():
            params[key] = objdict(val)

        return params



def print_color(*text, color='', _type=''):
    ''' color_choices = ['r','g','b', 'y']
        _type = ['error','warning','success','sql','string','df','list']
    '''
    color = color.lower()
    _type = _type.lower()

    if color == "g" or _type == "success":
        crayon_color = crayons.green
    elif color == "r" or _type == "error":
        crayon_color = crayons.red
    elif color == "y" or _type in ("warning", "sql"):
        crayon_color = crayons.yellow
    elif color == "b" or _type in ("string", "list"):
        crayon_color = crayons.blue
    elif color == "p" or _type == "df":
        crayon_color = crayons.magenta
    else:
        crayon_color = crayons.normal

    print(*map(crayon_color, text))


def run_sql_scripts(engine, scripts, tryexcept=False):

    time_list = []
    if tryexcept == True:
        for script in scripts:
            time_now = time.time()
            print_color(script, color='y')
            # try:
            engine.execute(script)
            print_color(f'Script Complete -- Took {time.time() - time_now} seconds to Run --', color='p')
            time_list.append(time.time() - time_now)
            # except:
            #     pass
    else:
        for script in scripts:
            time_now = time.time()
            print_color(script, color='y')
            engine.execute(script)
            time_list.append(time.time() - time_now)
            print_color(f'Script Complete -- Took {time.time() - time_now} seconds to Run --', color='p')

    # for script in scripts:
    #     print_color(script.replace("/n",""), color='p')
    # for time_item in time_list:
    #     print_color(time_item, color='b')


def engine_setup(project_name='', hostname='', username='', password='', port=''):
    engine = project_name(f'mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{project_name}?charset=utf8',pool_pre_ping=True, echo=False)
    return engine



class Get_SQL_Types():
    def __init__(self,DataFrame):
        columnLenghts = np.vectorize(len)

        ## CONVERT DATAFRAME TYPES TO PROPER NUMERIC OR INTERGER BASED COLUMN TYPES
        col_is_numeric = DataFrame.replace(np.nan, 0).replace("nan", 0).replace("Nan", 0).apply(
            lambda s: pd.to_numeric(s, errors='coerce')).notnull().all().tolist()
        col_list = DataFrame.columns.tolist()
        df_original_types = DataFrame.dtypes.tolist()
        # print(df_original_types)
        for i, val in enumerate(col_is_numeric):
            if val == True:
                # print(df_original_types[i])
                if "datetime" not in str(df_original_types[i]):
                    decimal_level = DataFrame[col_list[i]].replace(np.nan, 0).replace("nan", 0).astype(str).str.split(".", n=2, expand=True)
                    # print(decimal_level)
                    if len(decimal_level.columns) > 1:
                        decimal_level = decimal_level[1].unique().tolist()
                        if len(decimal_level) == 1 and decimal_level[0] == '0':
                            DataFrame[col_list[i]] = DataFrame[col_list[i]].replace(np.nan, 0)
                            DataFrame[col_list[i]] = pd.to_numeric(DataFrame[col_list[i]], errors='ignore', downcast='integer')
                        else:
                            DataFrame[col_list[i]] = pd.to_numeric(DataFrame[col_list[i]], errors='ignore')
                    else:
                        DataFrame[col_list[i]] = DataFrame[col_list[i]].replace(np.nan, 0)
                        DataFrame[col_list[i]] = pd.to_numeric(DataFrame[col_list[i]], errors='ignore',downcast='integer')

        # print(DataFrame.dtypes)

        ## GET THE APPROPRIATE MYSQL COLUMN TYPES FOR THE DATAFRAME OBJECT
        data_types = dict()
        for col in DataFrame.columns:
            Col_Length = columnLenghts(DataFrame[col].values.astype(str)).max(axis=0)
            Col_Type = DataFrame[col].dtypes
            # print("column", col, Col_Length, Col_Type)
            if Col_Type == "object":
                if Col_Length > 255:
                    column_type = {col:sqlalchemy.types.TEXT()}
                    data_types.update(column_type)
                elif Col_Length >= 100:
                    column_type = {col:sqlalchemy.types.VARCHAR(255)}
                    data_types.update(column_type)
                elif  Col_Length >= 50:
                    column_type = {col:sqlalchemy.types.VARCHAR(100)}
                    data_types.update(column_type)
                elif  Col_Length >= 25:
                    column_type = {col:sqlalchemy.types.VARCHAR(50)}
                    data_types.update(column_type)
                elif Col_Length >= 15:
                    column_type = {col:sqlalchemy.types.VARCHAR(25)}
                    data_types.update(column_type)
                elif Col_Length >= 10:
                    column_type = {col:sqlalchemy.types.VARCHAR(15)}
                    data_types.update(column_type)
                elif Col_Length >= 5:
                    column_type = {col:sqlalchemy.types.VARCHAR(10)}
                    data_types.update(column_type)
                elif Col_Length >= 1:
                    column_type = {col:sqlalchemy.types.VARCHAR(5)}
                    data_types.update(column_type)
                elif  Col_Length == 0:
                    column_type = {col: sqlalchemy.types.VARCHAR(5)}
                    data_types.update(column_type)
            if Col_Type == "float" or Col_Type == "float64":
                new_data = DataFrame[col].to_frame()
                new_data = new_data.fillna(0)
                new_data[col] = new_data[col].astype(str)
                new = new_data[col].str.split(".", n = 1, expand = True)
                new.columns = ["First","Second"]
                Integer_Depth = columnLenghts(new['First'].values.astype(str)).max(axis=0)
                Decimal_Depth = columnLenghts(new['Second'].values.astype(str)).max(axis=0)
                # print_color(col, 'Integer_Depth', Integer_Depth, color='p')
                # print_color(col, 'Decimal_Depth',Decimal_Depth,color='p')
                if Decimal_Depth <=2:

                    if Col_Length <=10:
                        column_type = {col: sqlalchemy.types.Numeric(12,2)}
                        # column_type = {col: sqlalchemy.types.FLOAT(precision=12, asdecimal=True,decimal_return_scale=3)}
                    elif Col_Length <=20:
                        column_type = {col: sqlalchemy.types.Numeric(20, 2)}
                        # column_type = {col: sqlalchemy.types.FLOAT(20, 2)}
                    data_types.update(column_type)
                else:
                    if Col_Length <=10:
                        # column_type = {col: sqlalchemy.types.FLOAT(12,4)}
                        column_type = {col: sqlalchemy.types.Numeric(12, 4)}
                    elif Col_Length <=20:
                        # column_type = {col: sqlalchemy.types.FLOAT(20, 4)}
                        column_type = {col: sqlalchemy.types.Numeric(20, 4)}
                    data_types.update(column_type)
            if Col_Type == "int32" or Col_Type == "int64":
                if Col_Length >= 10:
                        column_type = {col: sqlalchemy.types.BIGINT()}
                else:
                    column_type = {col: sqlalchemy.types.INTEGER()}
                data_types.update(column_type)
            if Col_Type == "datetime64[ns]" or Col_Type == "datetime64":
                date_level = len(DataFrame[col].astype(str).str.split(" ", n=1, expand=True).columns)
                if date_level ==1:
                    column_type = {col: sqlalchemy.types.DATE()}
                    data_types.update(column_type)
                else:


                    column_type = {col: sqlalchemy.types.DATETIME()}
                    data_types.update(column_type)
            if Col_Type == "bool":
                column_type = {col: sqlalchemy.types.BOOLEAN()}
                data_types.update(column_type)

            # print("Column", col, Col_Type, column_type)

        self.data_types = data_types


class Add_Sql_Missing_Columns():
    def __init__(self, engine='',Project_name='', Table_Name='', DataFrame=''):
        ''' CHECK IF THE TABLE EXISTS'''
        print(Project_name, Table_Name)
        script = f'SELECT Table_Schema, Table_Name From information_schema.tables where TABLE_SCHEMA = "{Project_name}" and TABLE_NAME = "{Table_Name}"'
        df1 = pd.read_sql(script, con=engine)
        if df1.shape[0] == 1:
            ''' IF THE TABLE EXISTS GET THE FIRST ROW OF THAT TABLE'''
            script1 = f'Select column_name AS `COLUMN` From information_schema.columns b1 where b1.table_schema = "{Project_name}" And b1.table_name ="{Table_Name}" order by ORDINAL_POSITION;'

            df2 = pd.read_sql(script1, con=engine)
            # print(df2)

            ''' CONVERT COLUMN NAMES OF BOTH THE DATAFRAME BEING ASSESED AND THE TABLE IMPORTED
                MAKES THE LIST VALUES ALL LOWERCASE            
            '''
            col_dict = {}
            col_one_list = [x.lower() for x in DataFrame.columns]
            for col in DataFrame.columns.tolist():
                new_col = col.lower()
                col_dict.update({new_col:col})

            col_two_list = df2['COLUMN'].str.lower().tolist()
            ''' GET THE DIFFERENCE OF COLUMNS IF THERE IS A DIFFERENCE AND INPUT INTO A LIST'''
            col_diff = list(set(col_one_list).difference(set(col_two_list)))
            # print(col_one_list)
            # print(col_two_list)
            if col_diff != []:
                print_color('Difference of Columns',col_diff, color='b')

            columnLenghts = np.vectorize(len)
            for column in col_diff:
                script2 = ""
                col = col_dict.get(column)
                # print(col)
                Col_Length = columnLenghts(DataFrame[col].values.astype(str)).max(axis=0)
                Col_Type = DataFrame[col].dtypes
                print(Col_Type, Col_Length)
                if Col_Type == "object":
                    if Col_Length > 255:
                        script2 = f'Alter Table {Table_Name} add column `{col}` TEXT'
                    elif Col_Length >= 100:
                        script2 = f'Alter Table {Table_Name} add column `{col}` VARCHAR(255)'
                    elif Col_Length >= 50:
                        script2 = f'Alter Table {Table_Name} add column `{col}` VARCHAR(100)'
                    elif Col_Length >= 25:
                        script2 = f'Alter Table {Table_Name} add column `{col}` VARCHAR(50)'
                    elif Col_Length >= 15:
                        script2 = f'Alter Table {Table_Name} add column `{col}` VARCHAR(25)'
                    elif Col_Length >= 10:
                        script2 = f'Alter Table {Table_Name} add column `{col}` VARCHAR(15)'
                    elif Col_Length >= 5:
                        script2 = f'Alter Table {Table_Name} add column `{col}` VARCHAR(10)'
                    elif Col_Length >= 0:
                        script2 = f'Alter Table {Table_Name} add column `{col}` VARCHAR(5)'
                if Col_Type == "float" or Col_Type == "float64":
                    new_data = DataFrame[col].to_frame()
                    new_data = new_data.fillna(0)
                    new_data[col] = new_data[col].astype(str)
                    new = new_data[col].str.split(".", n=1, expand=True)
                    new.columns = ["First", "Second"]
                    Decimal_Depth = columnLenghts(new['Second'].values.astype(str)).max(axis=0)
                    if Decimal_Depth <= 2:
                        if Col_Length <= 10:
                            script2 = f'Alter Table {Table_Name} add column `{col}` FLOAT(12,2)'
                        elif Col_Length <= 20:
                            script2 = f'Alter Table {Table_Name} add column `{col}` FLOAT(20,2)'
                    else:
                        if Col_Length <= 10:
                            script2 = f'Alter Table {Table_Name} add column `{col}` FLOAT(12,4)'
                        elif Col_Length <= 20:
                            script2 = f'Alter Table {Table_Name} add column `{col}` FLOAT(20,4)'
                if  Col_Type == "int8" or Col_Type == "int16" or Col_Type == "int32" or Col_Type == "int64":
                    if Col_Length >= 10:
                        script2 = f'Alter Table {Table_Name} add column `{col}` BIGINT'
                    else:
                        script2 = f'Alter Table {Table_Name} add column `{col}` INT'
                if Col_Type == "datetime64[ns]" or Col_Type == "datetime64":
                    script2 = f'Alter Table {Table_Name} add column `{col}` DATE'
                if Col_Type == "bool":
                    script2 = f'Alter Table {Table_Name} add column `{col}` BOOL'
                print(script2)
                if script2 != "":
                    engine.connect().execute(script2)


class Change_Sql_Column_Types():
    def __init__(self, engine='', Project_name='', Table_Name='', DataTypes='', DataFrame=''):

        # df3 = View_SQL_Column_Lengths(engine=engine, Project_Name=Project_name, Table_Name=Table_Name).DataFrame
        script = f'Select Ordinal_Position as "#", column_name AS `COLUMN`, upper(COLUMN_TYPE) as TYPE From information_schema.columns b1 where b1.table_schema = "{Project_name}" And b1.table_name = "{Table_Name}" order by ORDINAL_POSITION;'

        df2 = pd.read_sql(script, con=engine)
        df = DataFrame
        modify_script = ""
        DataType = DataTypes
        for i in range(df2.shape[0]):
            column = str(df2['COLUMN'].iloc[i])
            Column_Type = str(df2['TYPE'].iloc[i]).replace("'", '').replace('b', '')                    #THIS IS THE MYSQL COLUMN TYPE
            # sql_column_length = df3.loc[df3['Column_Name'] == column]['Char_Length'].iloc[0]

            if column in DataType:
                dataframe_column_type = str(DataType[column]).replace(" ", "")                          # THIS IS THE DATAFRAME COLUMN TYPE
                # dataframe_column_type = dataframe_column_type

                # print_color(column, Column_Type, dataframe_column_type, color='p')
                if Column_Type == "FLOAT(12,4)" or Column_Type == "FLOAT(12,2)" or Column_Type == "FLOAT(20,4)" or Column_Type == "FLOAT(20,2)" or Column_Type == "VARCHAR(5)":
                    df[column] = df[column].replace(np.nan, 0)

                if Column_Type != dataframe_column_type:
                    if (Column_Type == "INT(11)" or Column_Type == "INT") and dataframe_column_type == "INTEGER":
                        next = "next"
                    elif Column_Type == "BIGINT(20)" and dataframe_column_type == "BIGINT":
                        next = "next"
                    elif (Column_Type == "BIGINT(20)" or Column_Type == "BIGINT") and dataframe_column_type == "INTEGER":
                        next = "next"
                    elif Column_Type == "DATETIME" and "VARCHAR" in dataframe_column_type:
                        next = "next"
                    elif Column_Type == "DATE" and "VARCHAR" in dataframe_column_type:
                        next = "next"
                    elif (Column_Type == "FLOAT(12,4)" or Column_Type == "FLOAT(20,4)" or Column_Type == "FLOAT(12,2)" or Column_Type == "FLOAT(20,2)") and dataframe_column_type == "INTEGER":
                        next = "next"
                    elif Column_Type == "TINYINT(1)" and dataframe_column_type == "BOOLEAN":
                        next = "next"
                    elif "FLOAT(20,4)" in Column_Type and "FLOAT(12,4)" in dataframe_column_type:
                        next = "next"
                    elif "FLOAT(20,2)" in Column_Type and "FLOAT(12,2)" in dataframe_column_type:
                        next = "next"
                    elif "DECIMAL(20,4)" in Column_Type and "DECIMAL(12,4)" in dataframe_column_type:
                        next = "next"
                    elif "DECIMAL(20,2)" in Column_Type and "DECIMAL(12,2)" in dataframe_column_type:
                        next = "next"
                    elif "NUMERIC(20,4)" in Column_Type and "NUMERIC(12,4)" in dataframe_column_type:
                        next = "next"
                    elif "NUMERIC(20,2)" in Column_Type and "NUMERIC(12,2)" in dataframe_column_type:
                        next = "next"
                    elif "VARCHAR" in Column_Type and ("NUMERIC" in dataframe_column_type or "DECIMAL" in dataframe_column_type or "FLOAT" in dataframe_column_type
                                                       or "INTEGER" in dataframe_column_type or "BIGINT" in dataframe_column_type):
                        next = "next"
                    elif Column_Type == "DATE" and "DATETIME" in dataframe_column_type:
                        next = "next"
                    elif "VARCHAR" in Column_Type and "BOOLEAN" in dataframe_column_type:
                        next = "next"
                    elif "TEXT" in Column_Type and "VARCHAR" in dataframe_column_type:
                        next = "next"
                    elif "TEXT" in Column_Type and "FLOAT" in dataframe_column_type:
                        next = "next"
                    elif "VARCHAR" in Column_Type and "VARCHAR" in dataframe_column_type:
                        database_column_length = int(Column_Type.split("(")[1].split(")")[0])
                        dataframe_column_length = int(dataframe_column_type.split("(")[1].split(")")[0])

                        if dataframe_column_length > database_column_length:
                            print_color(column, Column_Type, dataframe_column_type, color='y')
                            # print_color(database_column_length, dataframe_column_length, color='y')
                            # print(column, Column_Type, dataframe_column_length, database_column_length, dataframe_column_type,dataframe_column_length)
                            if modify_script == "":
                                modify_script += "MODIFY COLUMN `" + column + "` " + "VARCHAR(" + str(
                                    dataframe_column_length) + ")"
                            else:
                                modify_script += ", \nMODIFY COLUMN `" + column + "` " + "VARCHAR(" + str(
                                    dataframe_column_length) + ")"
                    else:
                        check_values = df[column].unique()
                        # print(check_values)
                        if len(check_values) == 1 and (check_values[0] == 0 or str(check_values[0]) == 'nan'):
                            next = "next"
                        else:
                            # print_color(column, Column_Type, dataframe_column_type, color='y')
                            if modify_script == "":
                                modify_script += "MODIFY COLUMN `" + column + "` " + dataframe_column_type
                            else:
                                modify_script += ", \nMODIFY COLUMN `" + column + "` " + dataframe_column_type

        alter_script = "ALTER TABLE " + Table_Name + '\n'
        if modify_script != "":
            main_script = alter_script + modify_script
            # print_color(main_script, color='p')
            engine.connect().execute(main_script)

        self.DataFrame = DataFrame


class create_folder():
    def __init__(self, foldername=""):
        if not os.path.exists(foldername):
            os.mkdir(foldername)
