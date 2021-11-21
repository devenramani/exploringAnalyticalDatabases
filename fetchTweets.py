# -*- coding: utf-8 -*-

import requests

def insertRecord(tweet):
    insertUrl = "http://localhost:8123/?query=INSERT INTO twitterApp.tweets(Time,Id,Possibly_Sensitive,Language,Source,Text) values(now(),{tid},{ps},{lang},{src},{txt})".format(tid = tweet['id'], ps = "true", lang = tweet['lang'], src = tweet['source'], txt = (tweet['text'].replace('#','').encode("ascii", errors="ignore").decode()))
    response = requests.post(insertUrl)
    print(response.content)


response = requests.get(
    'https://api.twitter.com/2/tweets/search/recent?query=Covid19&tweet.fields=id,created_at,geo,lang,possibly_sensitive,source&max_results=10',
    headers={'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAAGMMVwEAAAAAV2PT0v2ZJZhKXsbabUc64KR2dUc%3DvL2ge6VI4dNNzZY5avYhBWhCZxDiBMVcPTXOxVTsUQW7KZZfud'})

responseJson = response.json()

responseData = responseJson['data']
responseMeta = responseJson['meta']

next_token = responseMeta['next_token']

count = 1

while count < 2:
    print("Page : {0}".format(count))

    for tweet in responseData:
        print(tweet['id'])

        #insert into database
        insertRecord(tweet)
        
    next_url = "https://api.twitter.com/2/tweets/search/recent?query=Covid19&tweet.fields=id,created_at,geo,lang,possibly_sensitive,source&max_results=10&next_token={nt}"
    next_url = next_url.format(nt = next_token)

    next_response = requests.get(
        next_url,
        headers={'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAAGMMVwEAAAAAV2PT0v2ZJZhKXsbabUc64KR2dUc%3DvL2ge6VI4dNNzZY5avYhBWhCZxDiBMVcPTXOxVTsUQW7KZZfud'})

    next_responseJson = next_response.json()

    responseData = next_responseJson['data']
    responseMeta = next_responseJson['meta']

    count +=1
