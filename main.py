from datetime import datetime
from datetime import timezone
import requests
import json
import numpy as np

import json
import sys
import time
from pprint import pprint
from datetime import date, datetime, timedelta
from time import sleep

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
import openpyxl
from openpyxl import Workbook, load_workbook

import pickle
import os.path
import requests
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

def addRow_to_Gsheet(sheet_id,sheet_range,in_array):
    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    # The ID and range of a sample spreadsheet.
    SPREADSHEET_ID = sheet_id
    RANGE_NAME = sheet_range    

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE_NAME).execute()
    values = result.get('values', [])

    #print(values[0])

    values = in_array
    body = {
    "majorDimension": "ROWS",
    "values": values
    }
    result = sheet.values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
    valueInputOption="USER_ENTERED", body=body).execute()
    print('{a} cells updated.'.format(a=result.get('updatedCells')))

def unixToUTC(in_array_time):
    out_array_time =[]
    in_array_time = in_array_time
    for mytime in in_array_time:
        myUnixTime = mytime
        myOutputTime = ''
        myFMT = '%Y-%m-%d %H:%M'

        myOutputTime = datetime.utcfromtimestamp(int(myUnixTime)).strftime(myFMT)

        #print(myOutputTime)
        out_array_time.append(myOutputTime)
    #print(out_array_time) 
    return out_array_time   

def UTCtounix(in_array_UTCtime):
    out_array_Unixtime=[]
    for mytime in in_array_UTCtime:
        myUTC_time = mytime
        myFMT = '%Y-%m-%d %H:%M'
        dt = datetime.strptime(myUTC_time, myFMT).replace(tzinfo=timezone.utc)

        out_time = dt.timestamp()
        out_array_Unixtime.append(round(out_time))
    #print(out_time)
    return out_array_Unixtime

def get_info_by_time(in_code,out_time_spot,out_type):
    #in_array_time=['2019-01-01','2020-02-24']
    in_code = in_code
    out_time_spot = out_time_spot

    in_array_time=[out_time_spot[0],out_time_spot[-1]]
    outString = ''
    outArray =[]

    for mycode in in_code:
        sub_array = []

        outtime = UTCtounix(in_array_time)
        outString = outString+ mycode +'\n---Date----------Close--Open--High--Low--Volume\n'
        url = 'https://dchart-api.vndirect.com.vn/dchart/history?resolution=D&symbol={x}&from={y}&to={z}'.format(x=mycode,y=outtime[0],z=outtime[1])
        response = requests.get(url)
        
        data = json.loads(response.content.decode('utf-8'))
        arr = np.array(data['t'])
        for myTimeSpot in out_time_spot:
            UnixSpot = UTCtounix([myTimeSpot])[0]
            while True:
                result = np.where(arr == UnixSpot)

                if len(result[0]) != 0:
                    break
                else:
                    UnixSpot += 24*60*60
            
            
            i = result[0][0]
            outString = outString + str(unixToUTC([data['t'][i]])) +' : '+str(data['c'][i]) + '--' +str(data['o'][i])+ '--'+str(data['h'][i])+ '--'+str(data['l'][i])+ '--'+str(data['v'][i])+'\n'
            sub_array.append([mycode,str(unixToUTC([data['t'][i]])[0]),str(data['c'][i]),str(data['o'][i]),str(data['h'][i]),str(data['l'][i]),str(data['v'][i])])
        outArray.append(sub_array)
    if out_type == 'string_type':
        return outString
    elif out_type == 'array_type':
        return outArray

def get_all_code():
    out_array = {'code':[],'company':[],'object':[],'floor':[],'listedDate':[]}
    url = 'https://finfo-api.vndirect.com.vn/stocks?utm_source=trade-hn.vndirect.com.vn'
    response = requests.get(url)
    data = json.loads(response.content.decode('utf-8'))
    for myCode in data['data']:
        out_array['code'].append(myCode['symbol'])
        out_array['company'].append(myCode['companyName'])
        out_array['object'].append(myCode['object'])
        out_array['floor'].append(myCode['floor'])
        out_array['listedDate'].append(myCode['listedDate'])
    print('Have {a} in total'.format(a=str(len(out_array['code']))))
    return out_array

def all_code_to_Gsheet():
    sheet_id = '1hFQUjP6g5NVS2WjfMymI4ixUVoBkKS0gScnURMCrY2Q'
    sheet_name_2 = 'all_company'
    toGsheet_array =[]
    for i in range(0,len(all_info['code'])-1):
        sub_array = []
        sub_array.append(all_info['code'][i])
        sub_array.append(all_info['company'][i])
        sub_array.append(all_info['object'][i])
        sub_array.append(all_info['floor'][i])
        sub_array.append(all_info['listedDate'][i])
        toGsheet_array.append(sub_array)
    addRow_to_Gsheet(sheet_id,sheet_name_2,toGsheet_array)

def get_Gsheet_info(sheet_id,sheet_range):
    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    # The ID and range of a sample spreadsheet.
    SPREADSHEET_ID = sheet_id
    RANGE_NAME = sheet_range
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE_NAME).execute()
    values = result.get('values', [])

    return values

if __name__ == '__main__':
    print(unixToUTC(['1580515200']))
    print(unixToUTC(['1582934400']))
    sheet_id = '1hFQUjP6g5NVS2WjfMymI4ixUVoBkKS0gScnURMCrY2Q'
    sheet_name_1 = 'info'
    sheet_name_2 = 'all_company'
    sheet_name_3 = 'master'
    
    #all_info = get_all_code()
    #myCode = all_info['code'][:100]
    
    myCode = []
    myTime = []

    all_info = get_Gsheet_info(sheet_id,sheet_name_3+'!B1:B')
    for a in all_info[1:]:
        myCode.append(a[0])
    
    all_info = get_Gsheet_info(sheet_id,sheet_name_3+'!A1:A')
    for b in all_info[1:]:
        myTime.append(b[0])

    
    #myCode = ['HPG']
    #myTime = ['2017-11-01','2019-01-01','2019-03-01','2019-06-01','2019-09-01','2019-12-01','2019-12-31','2020-01-31','2020-02-15','2020-02-19','2020-02-24','2020-02-25']
    all_data = get_info_by_time(myCode,myTime,'array_type')
    for code in all_data:
        addRow_to_Gsheet(sheet_id,sheet_name_1,code)
        sleep(1)
        #print(code)

    
    
    