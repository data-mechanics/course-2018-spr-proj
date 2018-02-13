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
        
        if trial:
            projected_uber = projected_uber[:10]
            projected_streetlights = projected_streetlights[:10]
            
        cache = {}
        uber_light = []
        for uber in projected_uber:
            lights = []
            for streetlight in projected_streetlights:
                streetlight_lat = streetlight[1]
                streetlight_long = streetlight[0]
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
                    
                if getUberLights.is_close([uber_lat, uber_long], [streetlight_lat,streetlight_long]):
                    lights += [(streetlight_lat,streetlight_long)]
            uber_light.append({
                    "uber_destination":(uber_lat, uber_long),
                    "lights":(len(lights))
                    })
    
        repo.dropCollection("uberlights")
        repo.createCollection("uberlights")
        repo['ferrys.uberlights'].insert_many(uber_light)
        repo['ferrys.uberlights'].metadata({'complete':True})
        print(repo['ferrys.uberlights'].metadata())

                
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
        repo.authenticate('ferrys', 'ferrys')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ferrys/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/ferrys/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('geocode', 'https://maps.googleapis.com/maps/api/geocode')

        this_script = doc.agent('alg:ferrys#getUberLights', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        uber_travel = doc.entity('dat:ferrys#uber', {prov.model.PROV_LABEL:'uber', prov.model.PROV_TYPE:'ont:DataSet'})
        streetlight_locations = doc.entity('dat:ferrys#streetlights', {prov.model.PROV_LABEL:'streetlights', prov.model.PROV_TYPE:'ont:DataSet'})
        geocode_locations = doc.entity('geocode:json', {'prov:label':'Google Geocode API', prov.model.PROV_TYPE:'ont:DataResource'})
        
        get_uber_lights = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_uber_lights, this_script)

        doc.usage(get_uber_lights, geocode_locations, startTime, None, 
                  {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?address=$&key=$'})
        doc.usage(get_uber_lights, uber_travel, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_uber_lights, streetlight_locations, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        uber_lights = doc.entity('dat:ferrys#uberlights', {prov.model.PROV_LABEL: 'Uber Destinations with Streetlights', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(uber_lights, this_script)
        doc.wasGeneratedBy(uber_lights, get_uber_lights, endTime)
        doc.wasDerivedFrom(uber_lights, uber_travel, get_uber_lights, get_uber_lights, get_uber_lights)
        doc.wasDerivedFrom(uber_lights, streetlight_locations, get_uber_lights, get_uber_lights, get_uber_lights)
        doc.wasDerivedFrom(uber_lights, geocode_locations, get_uber_lights, get_uber_lights, get_uber_lights)

        repo.logout()
        return doc

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


#    
getUberLights.execute(True)
#doc = getUberLights.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof