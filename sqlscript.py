#!/usr/bin/env python

import pyodbc
import requests
import time
import json
import pandas as pd

from clickhouse_driver import Client
from datetime import datetime


server = 'localhost'
database = 'temp'
username = 'sa'
password = 'qwerty@12345'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()


# #Test querries
# tsql = "Alter TABLE twitterApp alter column Id bigint"
# with cursor.execute(tsql):
#     print ('Success!')

# #Select Query
# tsql = "SELECT * FROM twitterApp;"
# with cursor.execute(tsql):
#     row = cursor.fetchone()
#     while row:
#         print (str(row[0]) + " " + str(row[1]))
#         row = cursor.fetchone()


def getTweetsFromClickhouse():
    
    client = Client(host='localhost')

    data = client.execute_iter('SELECT * FROM twitterApp.tweets LIMIT 5000', with_column_types=True)
    columns = [column[0] for column in next(data)]

    df = pd.DataFrame.from_records(data, columns=columns)
    tweetsArray = json.loads(df.to_json(orient='records'))

    return tweetsArray


count = 0 
totalinsertiontime = 0


tweets = getTweetsFromClickhouse()

for tweet in tweets:
    count+=1
    print(count)
    #Insert Query
    tsql = "INSERT INTO twitterApp VALUES (?,?,?,?,?,?);"

    datetimeValue = datetime.fromtimestamp(tweet['Time']/1000)

    starttime = time.time()
    with cursor.execute(tsql, datetimeValue ,tweet['Id'],tweet['Possibly_Sensitive'],tweet['Language'],tweet['Source'],tweet['Text']):
        print ('Successfully Inserted!')

    endtime = time.time()

    totaltime = endtime - starttime
    totalinsertiontime += totaltime

print(totalinsertiontime)





