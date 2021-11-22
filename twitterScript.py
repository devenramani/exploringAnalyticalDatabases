# -*- coding: utf-8 -*-

import requests
import time
import datetime

runforpages = 10100

def insertRecord(tweet):

    tweet['text'] = tweet['text'].replace("#","").replace("'","").replace("&amp;","and")

    tweetdate = datetime.datetime.strptime(tweet['created_at'],"%Y-%m-%dT%H:%M:%S.%fZ")

    insertUrl = "http://localhost:8123/?query=INSERT INTO twitterApp.tweets(Time,Id,Possibly_Sensitive,Language,Source,Text) values('{td}','{tid}','{ps}','{lang}','{src}','{txt}')".format(td = tweetdate,tid = tweet['id'], ps = "true", lang = tweet['lang'], src = tweet['source'], txt = (tweet['text']).encode("ascii", errors="ignore").decode())

    starttime = time.time()
    
    response = requests.post(insertUrl)

    endtime = time.time()

    #print(insertUrl)
    print(response.status_code)

    totaltime = 0
    insertstatusflag = 0

    if(response.status_code == 200):
        totaltime = endtime - starttime
        insertstatusflag = 1

    return totaltime,insertstatusflag


response = requests.get(
    'https://api.twitter.com/2/tweets/search/recent?query=Covid19&tweet.fields=id,created_at,geo,lang,possibly_sensitive,source&max_results=100',
    headers={'Authorization': 'Bearer *insert token*'})

responseJson = response.json()

responseData = responseJson['data']
responseMeta = responseJson['meta']

next_token = responseMeta['next_token']

totalinsertiontime = 0
totaltweetsinserted = 0
pagecount = 1

while pagecount <= runforpages:
    print("Page : {0}".format(pagecount))

    for tweet in responseData:
        print(tweet['id'])

        #insert into database
        totaltime,inserttweetcount = insertRecord(tweet)
        
        totaltweetsinserted += inserttweetcount
        totalinsertiontime += totaltime

    if(pagecount % 400 == 0):
        print("sleeping for 11 mins....")
        time.sleep(660)

    next_url = "https://api.twitter.com/2/tweets/search/recent?query=Covid19&tweet.fields=id,created_at,geo,lang,possibly_sensitive,source&max_results=100&next_token={nt}"
    next_url = next_url.format(nt = next_token)

    next_response = requests.get(
        next_url,
        headers={'Authorization': 'Bearer *insert token*'})

    next_responseJson = next_response.json()

    responseData = next_responseJson['data']
    responseMeta = next_responseJson['meta']
    next_token = next_responseJson['meta']['next_token']

    pagecount +=1


print("Total insertion time for {0} records : {1}".format(totaltweetsinserted,totalinsertiontime))
