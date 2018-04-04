import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import *

def greatCircleDistance(coord1,coord2):
  def haversin(x):
    return sin(x/2)**2 
  return 2 * asin(sqrt(
      haversin(radians(coord2[0])-radians(coord1[0])) +
      cos(radians(coord1[0])) * cos(radians(coord2[0])) * haversin(radians(coord2[1])-radians(coord1[1]))))*6371

client = dml.pymongo.MongoClient()
repo = client.repo  	
repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')
url = 'http://datamechanics.io/data/ashleyyu_bzwtong/cityOfBostonPolice.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
police = json.loads(response)
policeStation = police['data']['fields'][3]['statistics']['values']
coordinates = []
for i in policeStation:
	coordinates.append((i['lat'],i['long']))

print (type(police))
print (coordinates)

print (greatCircleDistance((42.334445,-71.089922),(42.371228,-71.038591)))

