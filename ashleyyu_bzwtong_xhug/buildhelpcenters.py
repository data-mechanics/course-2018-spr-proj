import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
from math import *

def dist(p, q):
	(x1,y1) = p
	(x2,y2) = q
	return (x1-x2)**2 + (y1-y2)**2

def plus(args):
	p = [0,0]
	for (x,y) in args:
		p[0] += x
		p[1] += y
	return tuple(p)

def scale(p, c):
	(x,y) = p
	return (x/c, y/c)

def product(R, S):
	return [(t,u) for t in R for u in S]

def aggregate(R, f):
	keys = {r[0] for r in R}
	return [(key, f([v for (k,v) in R if k == key])) for key in keys]

# calculates the distance between two coordinates on earth in kilometers
def distanceToPolice(coord1,coord2):
  def haversin(x):
    return sin(x/2)**2 
  return 2 * asin(sqrt(
      haversin(radians(coord2[0])-radians(coord1[0])) +
      cos(radians(coord1[0])) * cos(radians(coord2[0])) * haversin(radians(coord2[1])-radians(coord1[1]))))*6371

def notWithinOneKm(lista,listb):
	for i in lista:
		for j in listb:
			if distanceToPolice(i,j) >= 1:
				return True


class buildHelpCenters(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong_xhug'
    reads = ['ashleyyu_bzwtong.crimerate']
    writes = ['ashleyyu_bzwtong.helpcenter']
    
    @staticmethod
    def execute(trial):
        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')
        startTime = datetime.datetime.now()
        
        #get locations of all crime incidents. In trial mode only get 50 incidents
        crimedata = repo['ashleyyu_bzwtong.crimerate'].find()
        if (trial == True):
            locations = [tuple(row['Location'].replace('(', '').replace(')', '').split(',')) 
        				for row in crimedata if row['Location'] != '(0.00000000, 0.00000000)' 
        				and row['Location'] != '(-1.00000000, -1.00000000)' ][:50]
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

        # K-mean algorithm. random initial means. Terminate after 15 loops to avoid taking too long
        oldmeans = []
        counter = 0
        means = [(42.351062, -71.143527), (42.351062, -71.143527), (42.336344, -71.048599), (42.33495488126205, -71.06702640200916), (42.299449015530804, -71.11691299585614)]
        while oldmeans != means and counter<15:
        		oldmeans = means
        		counter += 1
        		distanceToMean = [(m, p, dist(m,p)) for (m, p) in product(means, locations)]
        		distanceWithKeys = [(p, dist(m,p)) for (m, p, d) in distanceToMean]
        		shortestDistance = aggregate(distanceWithKeys, min)
        		shortestPairs = [(m, p) for ((m,p,d), (p2,d2)) in product(distanceToMean, shortestDistance) if p==p2 and d==d2]
        		totalDistance = aggregate(shortestPairs, plus)
        		Mean1 = [(m, 1) for (m, _) in shortestPairs]
        		MeanC = aggregate(Mean1, sum)
        		means = [scale(t,c) for ((m,t),(m2,c)) in product(totalDistance, MeanC) if m == m2]
        		print ('means: ')
        		print (sorted(means))
        		print ('oldmeans: ')
        		print (sorted(oldmeans))
                # after 9 loops, the means are fairly accurate. As long as the means are far away from police stations,
                # the means are good enough to be student help centers
        		if counter >= 9:
        			if notWithinOneKm(means,coordinates):
        				print ('final means: ',sorted(means))
        				break
        helpcenter = {"helpcenters": means}
        print(helpcenter)
        if (trial == True): return
        repo.dropCollection("helpcenters")
        # repo.dropCollection("crimerate_clusters")
        repo.createCollection("helpcenters")
        repo['ashleyyu_bzwtong.helpcenters'].insert_one(helpcenter)
        repo['ashleyyu_bzwtong.helpcenters'].metadata({'complete':True})
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


        this_script = doc.agent('alg:ashleyyu_bzwtong#aggnonpublicschools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_properties = doc.entity('dat:ashleyyu_bzwtong#buildHelpCenters', {'prov:label':' Non Public Schools Aggregate Zips', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_buildHelpCenters = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_buildHelpCenters, this_script)
        doc.usage(get_buildHelpCenters, resource_properties, startTime,None,
                  {prov.model.PROV_TYPE:'ont:Computation'})


        buildHelpCenters = doc.entity('dat:ashleyyu_bzwtong#buildHelpCenters', {prov.model.PROV_LABEL:'help centers for students in emergency', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(buildHelpCenters, this_script)
        doc.wasGeneratedBy(buildHelpCenters, get_buildHelpCenters, endTime)
        doc.wasDerivedFrom(buildHelpCenters, resource_properties, get_buildHelpCenters, get_buildHelpCenters, get_buildHelpCenters)



        repo.logout()
                  
        return doc


buildHelpCenters.execute(False)
doc = buildHelpCenters.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

