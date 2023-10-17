from __future__ import print_function
import os
import sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
import os.path
from sys import argv
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pandas as pd
import numpy as np
from Database_Modules import print_color
# from sqlalchemy import inspect
# import global_modules
# from global_modules import engine_setup, print_color, run_sql_scripts


class GoogleSheetsAPI():
    def __init__(self, credentials_file=None, token_file=None, scopes=None, sheet_id=None):
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.sheet_id = sheet_id
        self.scopes = scopes


        self.service = self.service_setup()

    def service_setup(self):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists( self.token_file ):
            creds = Credentials.from_authorized_user_file( self.token_file , self.scopes)
            # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_file, self.scopes)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open( self.token_file , 'w') as token:
                token.write(creds.to_json())

        service = build('sheets', 'v4', credentials=creds)

        return service

    def create_new_spreadsheet(self, title):
        spreadsheet = {
            'properties': {
                'title': title
            }
        }
        spreadsheet = self.service.spreadsheets().create(body=spreadsheet,fields='spreadsheetId').execute()
        spreadsheet_id = spreadsheet.get('spreadsheetId')

        print('Spreadsheet ID: {0}'.format(spreadsheet.get('spreadsheetId')))

    def get_data_from_sheet(self, sheetname, range_name):
        """Shows basic usage of the Sheets API.
        Prints values from a sample spreadsheet.
        """


        # Call the Sheets API
        sheet = self.service.spreadsheets()
        range_value = f'{sheetname}!{range_name}'
        print_color(range_value, color='y')
        result = sheet.values().get(spreadsheetId=self.sheet_id,
                                    range=range_value).execute()
        values = result.get('values', [])
        df = pd.DataFrame(values)
        if df.shape[0] > 0:
            new_header = df.iloc[0]
            df.columns = [x.lower().replace(" ", "_") for x in new_header]
            df = df[1:]

        return df

    def get_row_count(self, sheetname):
        # spreadsheet = self.service.spreadsheets()
        sheet = self.service.spreadsheets()
        response = sheet.values().get(spreadsheetId=self.sheet_id,range=f'{sheetname}').execute()

        display_a1 = response["range"]
        print_color(f'Range: {display_a1}', color='g')
        first_cell_a1 = display_a1.split('!')[1].split(':')[0]


        if response.get("values") is None:
            print_color(f'No data found in sheet {sheetname}', color='r')


        number_of_rows = len(response["values"])
        number_of_columns = 0

        print_color(number_of_rows, color='y')
        print_color(first_cell_a1, color='y')

        return number_of_rows

    def write_data_to_sheet(self, data,sheetname, row_number, include_headers=True, clear_data=False):

        for col in data.columns:
            if data[col].dtype == float:
                pass
                # print(data[col].dtype)
            else:
                data[col] = data[col].astype(str)
        data = data.replace(np.nan, "")
        data = data.replace('None', "")

        if include_headers is True:
            column_values = [x.replace("_", " ").title() for x in data.columns]
        values = data.values.tolist()
        if include_headers is True:
            values.insert(0, column_values)
        # values = [['TEST', 'TEST 2',
        #            'Abc','Def']]
        # for i in range(0, len(values), 5000):
        #     upated_values = values[i:i+5000]
        body = {
            'values': values
        }
        #
        #     print(sheet_id)
        if clear_data is True:
            self.service.spreadsheets().values().clear(spreadsheetId=self.sheet_id, range="A1:AC1000000")

        result = self.service.spreadsheets().values().update(
            spreadsheetId=self.sheet_id, range=f'{sheetname}!A{row_number}',
            valueInputOption='USER_ENTERED', body=body).execute()

        print(f'Data Updated')

    def insert_row_to_sheet(self, sheetname, gid,
                            insert_range= ['A',1,'A',1],
                            data=None,
                            insert_dropdown=False, dropdown_values =[],
                            dropdown_range =['A',1,'A',1],
                            copy_area=False,
                            copy_source_range =[0,1,1,1,1],
                            copy_destinations_range = [0,1,1,1,1],
                            copy_pasteType = None
                            ):

        request_body = [
                {
                    'insertDimension': {
                        'range': {
                            'sheetId': gid,
                            'dimension': 'ROWS',
                            'startIndex': insert_range[1],
                            'endIndex': insert_range[3] + 1
                        },
                        'inheritFromBefore': True
                    }
                }
            ]

        dropdown = None
        if insert_dropdown is True:
            dropdown = {
                "setDataValidation": {
                    "range": {
                        "sheetId": gid,
                        "startRowIndex": dropdown_range[1],
                        "endRowIndex": dropdown_range[3],
                        "startColumnIndex": dropdown_range[0],
                        "endColumnIndex": dropdown_range[2]
                    },
                    "rule": {
                        "condition": {
                            "type": 'ONE_OF_LIST',
                            "values":dropdown_values,
                        },
                        "showCustomUi": True,
                        "strict": True
                    }
                }
            }

        copy_paste = None
        if copy_area is True:
            copy_paste= {
                "copyPaste": {
                    'source': {
                        "sheetId": copy_source_range[0],
                        "startColumnIndex":  copy_source_range[1],
                        "startRowIndex":  copy_source_range[2],
                        "endColumnIndex":  copy_source_range[3],
                        "endRowIndex":  copy_source_range[4],
                        },
                    'destination': {
                        "sheetId": copy_destinations_range[0],
                        "startColumnIndex":  copy_destinations_range[1],
                        "startRowIndex":  copy_destinations_range[2],
                        "endColumnIndex":  copy_destinations_range[3],
                        "endRowIndex":  copy_destinations_range[4],
                        },
                    'pasteType': copy_pasteType,
                    'pasteOrientation': 'NORMAL'
                }
            }

        body = {'requests': request_body}
        print_color(body, color='p')

        self.service.spreadsheets().batchUpdate(spreadsheetId=self.sheet_id,body=body).execute()
        if dropdown is not None:
            body = {'requests': [dropdown]}
            print_color(body, color='p')
            result = self.service.spreadsheets().batchUpdate(spreadsheetId=self.sheet_id, body=body).execute()
            print_color(f'result: {result}', color='p')

        if copy_paste is not None:
            body = {'requests': [copy_paste]}
            print_color(body, color='p')
            result = self.service.spreadsheets().batchUpdate(spreadsheetId=self.sheet_id, body=body).execute()
            print_color(f'result: {result}', color='p')

        if data is not None:
            body = {
                'values': data
            }

            sheet_range = f'{sheetname}!{insert_range[0]}{insert_range[1]}:{insert_range[2]}{insert_range[3]}'
            print_color(sheet_range, color='y')

            result = self.service.spreadsheets().values().append(
                spreadsheetId=self.sheet_id,range=sheet_range,
                valueInputOption='USER_ENTERED',
                # insertDataOption='INSERT_ROWS',
                body=body).execute()

        # self.service.spreadsheets().append_row(insertRow, table_range=f"{start_column}{start_row}")

