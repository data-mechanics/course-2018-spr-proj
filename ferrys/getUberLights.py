import urllib.request
import dml
import prov.model
import datetime
import uuid
import json
import copy

class getUberLights(dml.Algorithm):
    contributor = 'ferrys'
    reads = ['ferrys.uber', 'ferrys.streetlights']
    writes = ['ferrys.uberlights']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ferrys', 'ferrys')
        api_key = dml.auth['services']['googlegeocoding']['key']
        
        # uber
        uber = repo.ferrys.uber.find()
        projected_uber = getUberLights.project(uber, lambda t: (t['Destination Display Name'].replace(' ', '+')))
        
        # streetlights
        streetlights = repo.ferrys.streetlights.find()
        projected_streetlights = getUberLights.project(streetlights, lambda t: [float(x) for x in t['the_geom'].replace(')', ' ').replace('(', ' ').split()[1:]])
        
        cache = {}
        uber_light = []
        for uber in projected_uber:
            lights = []
            for streetlight in projected_streetlights:
                if uber in cache:
                    uber_lat = cache[uber][0]
                    uber_long = cache[uber][1]
                else:
                    url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + uber + 'MA&key='+ api_key
                    response = urllib.request.urlopen(url).read().decode("utf-8")
                    google_json = json.loads(response)
                    try:
                        uber_lat = google_json["results"][0]["geometry"]["location"]['lat']
                        uber_long = google_json["results"][0]["geometry"]["location"]['lng']
                        cache[uber] = (uber_lat,uber_long)
                    except IndexError:
                        print("Address not found")
                        uber_lat = 0
                        uber_long = 0
                        cache[uber] = (uber_lat,uber_long)
                if getUberLights.is_close([uber_lat, uber_long], streetlight):
                    print("lit")
                    lights += [streetlight]
            uber_light.append({
                    "uber_destination":(uber_lat, uber_long),
                    "lights":(lights)
                    })
        uber_light_cp = copy.deepcopy(uber_light)
        
        repo.dropCollection("uberlights")
        repo.createCollection("uberlights")
        repo['ferrys.uberlights'].insert_many(uber_light)
        repo['ferrys.uberlights'].metadata({'complete':True})
        print(repo['ferrys.uberlights'].metadata())


        with open("../datasets/Uber_Destinations_With_Streetlights.json", 'w') as file:
                json.dump(uber_light_cp, file)
                
        repo.logout()
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass
#        '''
#            Create the provenance document describing everything happening
#            in this script. Each run of the script will generate a new
#            document describing that invocation event.
#            '''
#
#        # Set up the database connection.
#        client = dml.pymongo.MongoClient()
#        repo = client.repo
#        repo.authenticate('alice_bob', 'alice_bob')
#        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
#        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
#        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
#        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
#        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
#
#        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
#        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
#        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
#        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
#        doc.wasAssociatedWith(get_found, this_script)
#        doc.wasAssociatedWith(get_lost, this_script)
#        doc.usage(get_found, resource, startTime, None,
#                  {prov.model.PROV_TYPE:'ont:Retrieval',
#                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
#                  }
#                  )
#        doc.usage(get_lost, resource, startTime, None,
#                  {prov.model.PROV_TYPE:'ont:Retrieval',
#                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
#                  }
#                  )
#
#        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
#        doc.wasAttributedTo(lost, this_script)
#        doc.wasGeneratedBy(lost, get_lost, endTime)
#        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)
#
#        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
#        doc.wasAttributedTo(found, this_script)
#        doc.wasGeneratedBy(found, get_found, endTime)
#        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)
#
#        repo.logout()
#                  
#        return doc

    def union(R, S):
        return R + S
    def intersect(R, S):
        return [t for t in R if t in S]
    def product(R, S):
        return [(t,u) for t in R for u in S]
    def select(R, s):
        return [t for t in R if s(t)]
    def aggregate(R):
        keys = {r[0] for r in R}
        return [(key, [v for (k,v) in R if k == key]) for key in keys]
    def project(R, p):
        return [p(t) for t in R]
    
    def is_close(uber_dropoff, streetlight_coordinate):
        return abs(uber_dropoff[0] - streetlight_coordinate[0]) < .0001 and abs(uber_dropoff[1] - streetlight_coordinate[1]) < .0001


    
getUberLights.execute()
#doc = example.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof