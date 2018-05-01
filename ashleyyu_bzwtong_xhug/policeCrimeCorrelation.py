import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import *
import random

#find the correlation between crimerate and distance to closest police stations.
class policeCrimeCorrelation(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong_xhug'
    reads = ['ashleyyu_bzwtong_xhug.crimerate']
    writes = ['ashleyyu_bzwtong_xhug.policeCrimeCorrelation']


    def avg(x): # Average
        return sum(x)/len(x)
    
    def correlation(x, y):
        assert len(x) == len(y)
        n = len(x)
        assert n > 0
        avg_x = policeCrimeCorrelation.avg(x)
        avg_y = policeCrimeCorrelation.avg(y)
        diffprod = 0
        xdiff2 = 0
        ydiff2 = 0
        for idx in range(n):
            xdiff = x[idx] - avg_x
            ydiff = y[idx] - avg_y
            diffprod += xdiff * ydiff
            xdiff2 += xdiff * xdiff
            ydiff2 += ydiff * ydiff
    
        return diffprod / sqrt(xdiff2 * ydiff2)
    
    #takes two coordinates and calcualtes the distance between them in kilometers
    def distanceToPolice(coord1,coord2):
      def haversin(x):
        return sin(x/2)**2 
      return 2 * asin(sqrt(
          haversin(radians(coord2[0])-radians(coord1[0])) +
          cos(radians(coord1[0])) * cos(radians(coord2[0])) * haversin(radians(coord2[1])-radians(coord1[1]))))*6371

        
    @staticmethod
    def execute(trial):

        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        #get locations of all crime incidents. In trial mode only get 50 incidents
        crimedata = repo['ashleyyu_bzwtong.crimerate'].find()
        if (trial == True):
            locations = [tuple(row['Location'].replace('(', '').replace(')', '').split(',')) 
        				for row in crimedata if row['Location'] != '(0.00000000, 0.00000000)' 
        				and row['Location'] != '(-1.00000000, -1.00000000)' ][:200]
        else:
            locations = [tuple(row['Location'].replace('(', '').replace(')', '').split(',')) 
        				for row in crimedata if row['Location'] != '(0.00000000, 0.00000000)' 
        				and row['Location'] != '(-1.00000000, -1.00000000)' ]
        locations = [(float(lat), float(lon)) for (lat, lon) in locations]

        #get locations of Boston Police Departments
        url = 'http://datamechanics.io/data/ashleyyu_bzwtong/cityOfBostonPolice.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        police = json.loads(response)
        policeStation = police['data']['fields'][3]['statistics']['values']
        coordinates = []
        for i in policeStation:
        		coordinates.append((i['lat'],i['long']))

        #for each crime location, calculate its distance to the closest police station
        distanceToCops = []
        for crime in locations:
            #print('crime ',type(crime),crime)
            dis = 100
            for station in coordinates:
                #print('station ',type(station),station)
                dist = policeCrimeCorrelation.distanceToPolice(crime,station)
                if dist < dis:
                    dis = dist
            distanceToCops.append(dis)
        #print(max(distanceToCops),min(distanceToCops))
        
        #group distances into different ranges
        #distanceRangeCount: (in the area of less than 0.5km away from police stations, x number of crime happened),
        #(in the area of 0.5~1km away from police stations, y number of crime happened), (1~1.5km away)......
        distanceRangeCount = [[0.5,0],[1,0],[1.5,0],[2,0],[2.5,0],[3,0],[3.5,0],[4,0],[4.5,0],[5,0],[5.5,0]]

        #count the crime frequency for each distance range
        for i in distanceToCops:
            index = int(i//0.5)
            if index>10: index = 10
            distanceRangeCount[index][1]+=1
        #print(distanceRangeCount)

        #calculate correlation coefficient
        rangeCountTuples = [(a,b) for [a,b] in distanceRangeCount]
        x_distance = [a for (a,b) in rangeCountTuples]
        y_frequency = [b for (a,b) in rangeCountTuples]

        print(distanceRangeCount)

        coefficient = round(policeCrimeCorrelation.correlation(x_distance,y_frequency),3)

        print('correlation coefficient between distance to police station and crimerate is ', coefficient)

        repo.logout()
  
        endTime = datetime.datetime.now()
  
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
 
        this_script = doc.agent('alg:ashleyyu_bzwtong_xhug#policeCrimeCorrelation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Computation':'py'})
        resource_properties= doc.entity('dat:ashleyyu_bzwtong_xhug#policeCrimeCorrelation', {'prov:label':'MongoDB', prov.model.PROV_TYPE:'ont:DataResource'})
        get_policeCrimeCorrelation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_policeCrimeCorrelation, this_script)
        doc.usage(get_policeCrimeCorrelation, resource_properties, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
 
        policeCrimeCorr = doc.entity('dat:ashleyyu_bzwtong_xhug#policeCrimeCorrelation', {prov.model.PROV_LABEL:'Calculated Correlations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(policeCrimeCorr, this_script)
        doc.wasGeneratedBy(policeCrimeCorr, get_policeCrimeCorrelation, endTime)
        doc.wasDerivedFrom(policeCrimeCorr, resource_properties, get_policeCrimeCorrelation, get_policeCrimeCorrelation, get_policeCrimeCorrelation)

        repo.logout()
                  
        return doc

policeCrimeCorrelation.execute(False)
doc = policeCrimeCorrelation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
