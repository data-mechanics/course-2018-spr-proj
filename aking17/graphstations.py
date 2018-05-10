#
import csv
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from gmplot import gmplot
import math

import datetime


x = []
y = []
csv_file = 'hubway_stations.csv'

file = 'stationstatus.csv'

df = pd.read_csv(file, sep = ',')
#print(df.describe)

df1 = pd.read_csv('hubway_stations.csv', sep=',')

def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return math.sqrt((x1-x2)**2 + (y1-y2)**2)
columns = ['Station', 'Closest station']
closestDF = pd.DataFrame(columns = columns)
def smallestDist():
	x = df1['lat']
	y = df1['lng']
	loc = zip(x,y)
	means = []
	i = 0
	for p in loc:
		d = [(dist(p,q)) for q in loc if q!=p]
		#closestDF.loc[i] = [p, min(d)[1]]
		#i +=1
		means += [min(d)]
	return [max(means)*69 , min(means)*69 , (sum(means)/len(means))*69]

#create new csv file
cols = df.station_id.head(49).tolist()
#cols = ['update'] + cols
print(cols)
newdf = pd.DataFrame(columns=cols)
j = 1 
k = 1
lst = []
for i in range(df.shape[0]):

	lst += [(df.loc[i, 'nbBikes'],df.loc[i, 'nbEmptyDocks'])]
	k +=1
	if i == 8233:
		break
	elif k%50 == 0:
		#print(i)
		newdf.loc[j] = lst
		j += 1
		k = 1
		lst = []
		
		

#print(newdf)
newdf.to_csv('station_availability.csv')
#likelihood of getting a bike at a station
stationDF = pd.read_csv('station_availability.csv', sep = ',')
def canGetABike():
	stationDF.head(2)
	#for row in range(2):
	low = []
	high = []
	low_chance_bike = []
	high_chance_bike = []
	station =  stationDF.columns.astype(int)

	for row in range(len(stationDF)):
		for col in range(1,len(stationDF.loc[0])):
			vals = stationDF.iloc[row,col]
			#print(vals)
			vals = vals[1:-1].split(',')
			nbike = float(vals[0])
			nempty = int(vals[1])
			total = nbike+nempty
			if nbike/total>.75:
				high_chance_bike += [station[col]]
			elif nbike/total<.25:
				low_chance_bike += [station[col]]
		#print(low_chance_bike)
		#print(high_chance_bike)

		

		low += [low_chance_bike]
		high += [high_chance_bike]
		low_chance_bike = []
		high_chance_bike = []
	return low, high
import math
lows, highs = canGetABike()
#print(lows)
#print(highs)


def allDayLow(low):
	x = df1['lat']
	y = df1['lng']
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
	x = df1['lat']
	y = df1['lng']
	goodlat = []
	goodlng = []
	count = 0
	#print(df1.id.dtype)
	for i in df1.id:
		count+=1
		if i in high:
			goodlat += [df1.iloc[count,4]]
			goodlng += [df1.iloc[count,5]]
	ret = zip(goodlat,goodlng)
	return ret

def plotLocations(low, high):
	badlat = low[0]
	badlng = low[1]
	goodlat = high[0]
	goodlng = high[1]
	#print(goodlng)
	#print(goodlat)
	x = df1['lat']
	y = df1['lng']
	plt.plot(x,y, 'go')
	plt.plot(badlat,badlng,'ro')
	plt.plot(goodlat,goodlng,'bo')
	print('')
	plt.xlabel("fixed")
	plt.show()


locateGood = [allDayHigh(x) for x in highs]
locateBad = [allDayLow(x) for x in lows]
#print(len(locateBad),len(locateGood))
import sys
xlat = df1['lat']
ylng = df1['lng']
#gmap = gmplot.GoogleMapPlotter.from_geocode("Boston")
gmap = gmplot.GoogleMapPlotter(42.349046000000001, -71.096831000000009, 13)
gmap.scatter(xlat,ylng,'#3B0B39',size=60, marker=False)

for i in range(len(xlat)):
	lat = str(xlat[i])
	lng = str(ylng[i])
	sys.stdout.write('new google.maps.LatLng('+lat+lng+'),')

goodbadDF = pd.DataFrame(columns = ['Good','Bad'])
add = []
for i in range(168):
    add = [locateGood[i],locateBad[i]]
    goodbadDF.loc[i] = add
    add = []
goodbadDF.to_csv('goodbad.csv')

for i in locateGood[0]:
	lat = i[0]
	lng = i[1]
	gmap.marker(lat, lng, 'cornflowerblue')

for i in locateBad[0]:
	lat = i[0]
	lng = i[1]
	gmap.marker(lat, lng, 'red')

gmap.draw("my_map.html")

for i in range(len(xlat)):
	sys.stdout.write('new google.maps.Marker({position: new google.maps.LatLng(' + str(xlat[i]) +','+ str(ylng[i]) + '), map: map, icon: \"http://maps.google.com/mapfiles/ms/icons/blue-dot.png\", title: \"Average Station\"}),\n')
plotLocations(locateBad[0],locateGood[0])
"""
print(locateGood)
for i in range(len(locateGood)):
	plotLocations(locateBad[i], locateGood[i])


#ith open('Hubway_station_value.csv','w') as h:
  #  writer = csv.writer(h)
   # for row in df:
    #	if df.station_id 

# r[1] = station id
# r[3] = num bikes
# r[4] = empty docks

"""