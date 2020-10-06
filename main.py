
from datetime import date, datetime, timedelta,timezone

import json
import numpy as np

import json
import sys
from pprint import pprint

from time import sleep

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from openpyxl import Workbook, load_workbook

import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

def unixToUTC(in_array_time):
    myFMT = '%Y-%m-%d %H:%M'
    
    if type(in_array_time) == type([]):
        out_array_time =[]
        in_array_time = in_array_time
        for mytime in in_array_time:
            myUnixTime = mytime
            myOutputTime = ''    
            myOutputTime = datetime.fromtimestamp(int(myUnixTime),timezone(timedelta(hours=+7)))                   
            out_array_time.append(myOutputTime.strftime(myFMT))
        #print(out_array_time) 
        return out_array_time
    else:
        myOutputTime = datetime.fromtimestamp(int(in_array_time),timezone(timedelta(hours=+7)))
        return  myOutputTime.strftime(myFMT)

def UTCtounix(in_array_UTCtime):
    myFMT = '%Y-%m-%d %H:%M%z'
    tz ='+0700'
    if type(in_array_UTCtime) == type([]):
        out_array_Unixtime=[]
        for mytime in in_array_UTCtime:
            myUTC_time = mytime
            
            dt = datetime.strptime(myUTC_time+tz, myFMT)
            
            out_time = dt.timestamp()
            out_array_Unixtime.append(round(out_time))
        #print(out_time)
        return out_array_Unixtime
    else:
        return datetime.strptime(in_array_UTCtime+tz, myFMT).timestamp()

def make_time_ser(start_time,end_time,time_step):
    time_ser = []
    unixTimeStep = 0

    if time_step == 'D':
        unixTimeStep = 60*60*24
    elif 'h' in time_step:
        unixTimeStep = int(time_step.replace('h',''))*60*60
    elif 'm' in time_step:
        unixTimeStep = int(time_step)*60 
    
    subTime = UTCtounix(start_time)
    while subTime < UTCtounix(end_time):
        time_ser.append(subTime)
        subTime = subTime+unixTimeStep
    
    return time_ser

def get_info(stockID,start_time,end_time,time_step):

    outtime = UTCtounix([start_time,end_time])
    url = 'https://dchart-api.vndirect.com.vn/dchart/history?resolution={t}&symbol={x}&from={y}&to={z}'.format(t=time_step,x=stockID,y=outtime[0],z=outtime[1])
    response = requests.get(url)
    
    data = json.loads(response.content.decode('utf-8'))
    outArray = []
    for i in range(0,len(data['t'])-1):
        outArray.append([stockID,str(unixToUTC([data['t'][i]])[0]),float(data['c'][i])*1000,float(data['o'][i])*1000,float(data['h'][i])*1000,float(data['l'][i])*1000,float(data['v'][i])])
        
    return outArray

def get_all_code():
    out_array = {'code':[],'company':[],'object':[],'floor':[],'listedDate':[],'industryName':[]}
    url = 'https://finfo-api.vndirect.com.vn/stocks?utm_source=trade-hn.vndirect.com.vn'
    response = requests.get(url)
    data = json.loads(response.content.decode('utf-8'))
    for myCode in data['data']:
        out_array['code'].append(myCode['symbol'])
        out_array['company'].append(myCode['companyName'])
        out_array['object'].append(myCode['object'])
        out_array['floor'].append(myCode['floor'])
        out_array['listedDate'].append(myCode['listedDate'])
        out_array['industryName'].append(myCode['industryName'])
    print('Have {a} in total'.format(a=str(len(out_array['code']))))
    return out_array

def all_code_to_Gsheet():

    toGsheet_array =[]
    all_info = get_all_code()
    try:
        for i in range(0,len(all_info['code'])-1):
            sub_array = []
            sub_array.append(all_info['code'][i])
            sub_array.append(all_info['company'][i])
            sub_array.append(all_info['object'][i])
            sub_array.append(all_info['floor'][i])
            sub_array.append(all_info['listedDate'][i])
            sub_array.append(all_info['industryName'][i])
            toGsheet_array.append(sub_array)
        myGsuite.addRow_to_Gsheet(sheet_id,sheet_name_2,toGsheet_array)
        return 'OK'
    except:
        return 'Error'

def get_service():
    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

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
    return service

class Gsheet:

    def __init__(self,service):
        self.service = service

    def get_Gsheet_info(self,sheet_id,sheet_range):
        sheet = self.service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id,
                                    range=sheet_range).execute()
        values = result.get('values', [])

        return values

    def addRow_to_Gsheet(self,sheet_id,sheet_range,in_array):
        sheet = self.service.spreadsheets()
        values = in_array
        body = {
        "majorDimension": "ROWS",
        "values": values
        }
        result = sheet.values().append(spreadsheetId=sheet_id, range=sheet_range,
        valueInputOption="USER_ENTERED", body=body).execute()
        print('{0} cells updated.'.format(result.get('updatedCells')))

    def get_last_row(self,sheet_id,sheet_range):
        sheet = self.service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id,
                                    range=sheet_range).execute()
        values = result.get('values', [])
        last_row = len(values)
        return last_row

    def clear_content(self,sheet_id,sheet_range):
        sheet = self.service.spreadsheets()
        body = {}
        result = sheet.values().clear(spreadsheetId=sheet_id,
                                    range=sheet_range,body=body).execute()

def dataMonitoring(myStock):

    
    for stock in myStock:
        outArray = get_info(stock,start_time,end_time,time_step)      
        myGsuite.addRow_to_Gsheet(sheet_id,sheet_name_1,outArray)

if __name__ == '__main__':
    print("START!")

    global myGsuite,sheet_id,sheet_name_1,sheet_name_2,sheet_name_3,myStock,start_time,end_time,time_step
    myGsuite = Gsheet(get_service())
    sheet_id = '1gp-EEFYxVB05rWvimTKxeX_e6ZUkYwgWY3-1q0_H0Ew'
    sheet_name_1 = 'info'
    sheet_name_2 = 'all_company'
    sheet_name_3 = 'master'
   

    start_time = '2020-10-06 09:00'
    end_time = '2020-10-06 15:00'
    time_step = '15'

    #initial G sheet
    myGsuite.clear_content(sheet_id,sheet_name_1 + '!A2:G10000')
    sleep(10)

    myStock = ['FRT','VPG','CRE','GEX','VOS','GVR']
    dataMonitoring(myStock)

    """
    #get all stock code from g sheet
    allStock = myGsuite.get_Gsheet_info(sheet_id,sheet_name_2)
    for i in range(1,len(allStock)):
        sleep(2)
        print(str(i) + " / "+ str(len(allStock)+1)+" -- "+allStock[i][0])
        dataMonitoring([allStock[i][0]])


    
    #up all stocks id and info to G sheet
    status = all_code_to_Gsheet()
    print("up stock id to G sheet: {a}".format(a=status))

    
    """


    
    
    