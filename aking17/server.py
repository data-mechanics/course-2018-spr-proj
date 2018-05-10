from tqdm import tqdm
import json
import jsonschema
from flask import Flask, jsonify, abort, make_response, request
import pymongo
import pandas as pd
from gmplot import gmplot
import pprint
import math
import pprint

df1 = pd.read_csv('hubway_stations.csv', sep=',')
stationDF = pd.read_csv('station_availability.csv', sep = ',')


#stationDF.astype(str)

def makeDict(i):
    df = i
    d = {}


x = df1['lat']
y = df1['lng']


s = stationDF.to_dict()
s = str(s)
#Find the stations that have many or few bikes
def canGetABike():
    low = []
    high = []
    low_chance_bike = []
    high_chance_bike = []
    station =  stationDF.columns.astype(int)

    for row in range(len(stationDF)):
        for col in range(1,len(stationDF.loc[0])):
            vals = stationDF.iloc[row,col]
            vals = vals[1:-1].split(',')
            nbike = float(vals[0])
            nempty = int(vals[1])
            total = nbike+nempty
            if nbike/total>.75:
                high_chance_bike += [station[col]]
            elif nbike/total<.25:
                low_chance_bike += [station[col]]

        low += [low_chance_bike]
        high += [high_chance_bike]
        low_chance_bike = []
        high_chance_bike = []
    return low, high
lows, highs = canGetABike()


def allDayLow(low):
    badlat = []
    badlng = []
    count = 0

    for i in df1.id:
        count+=1
        if i in low:
            badlat += [df1.iloc[count,4]]
            badlng += [df1.iloc[count,5]]

    ret = zip(badlat,badlng)
    return ret

def allDayHigh(high):
    goodlat = []
    goodlng = []
    count = 0

    for i in df1.id:
        count+=1
        if i in high:
            goodlat += [df1.iloc[count,4]]
            goodlng += [df1.iloc[count,5]]

    ret = zip(goodlat,goodlng)
    return ret

locateGood = [allDayHigh(x) for x in highs]
locateBad = [allDayLow(x) for x in lows]


def parseData():
    df = pd.read_csv('goodbad.csv')
    lst =[]
    for index, row in df.iterrows():
        lst += [{
            "good" : row['Good'],
            "bad" : row['Bad']
        }]
    return lst


print("Populating database...")
d = parseData()
client = pymongo.MongoClient()
db = client.local
i=1
'''
for j in tqdm(d):
    db.goodbad1.insert({"_id" : i, "data": j})
    i +=1
'''



    
app = Flask(__name__)

@app.route('/', methods=['GET'])
def get_client():
    return open('examplemap.html','r').read()

', methods=['GET'])
def get_year(hour):
    for result in db.goodbad1.find({"_id":hour}):
        return jsonify(result["data"])

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found.'}), 404)

if __name__ == '__main__':
    app.run(debug=True)
