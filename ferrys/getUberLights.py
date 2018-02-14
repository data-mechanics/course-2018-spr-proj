import urllib.request
import dml
import prov.model
import datetime
import uuid
import json
import copy
import shapely.geometry
from tqdm import tqdm

class getUberLights(dml.Algorithm):
    '''
        Returns the number of streetlights in an uber boundary
    '''
    contributor = 'ferrys'
    reads = ['ferrys.uber_boundaries', 'ferrys.streetlights']
    writes = ['ferrys.uberlights']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ferrys', 'ferrys')
        
        # uber
        uber = repo.ferrys.uber_boundaries.find()
        projected_uber = getUberLights.project(uber, lambda t: (t['properties']['MOVEMENT_ID'], [tuple(x) for x in t['geometry']['coordinates'][0][0]]))
        
        # streetlights
        streetlights = repo.ferrys.streetlights.find()
        projected_streetlights = getUberLights.project(streetlights, lambda t: [float(x) for x in t['the_geom'].replace(')', ' ').replace('(', ' ').split()[1:]])
        
        if trial:
            projected_uber = projected_uber[:100]
            projected_streetlights = projected_streetlights[:700]
            
        uber_light = []
        for uber in tqdm(projected_uber):
            lights = []
            polygon = shapely.geometry.Polygon(uber[1])
            for streetlight in tqdm(projected_streetlights):
                streetlight_long = streetlight[0]
                streetlight_lat = streetlight[1]
                point = shapely.geometry.Point(streetlight_long, streetlight_lat)
                if polygon.contains(point):
                    lights += [(streetlight_long,streetlight_lat)]
            uber_light.append({
                    "uber_id":(uber[0]),
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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:ferrys#getUberLights', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        uber_boundaries = doc.entity('dat:ferrys#uber_boundaries', {prov.model.PROV_LABEL:'uber_boundaries', prov.model.PROV_TYPE:'ont:DataSet'})
        streetlight_locations = doc.entity('dat:ferrys#streetlights', {prov.model.PROV_LABEL:'streetlights', prov.model.PROV_TYPE:'ont:DataSet'})
        
        get_uber_lights = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_uber_lights, this_script)

        doc.usage(get_uber_lights, uber_boundaries, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_uber_lights, streetlight_locations, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        uber_lights = doc.entity('dat:ferrys#uberlights', {prov.model.PROV_LABEL: 'Uber Boundaries with Streetlights', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(uber_lights, this_script)
        doc.wasGeneratedBy(uber_lights, get_uber_lights, endTime)
        doc.wasDerivedFrom(uber_lights, uber_boundaries, get_uber_lights, get_uber_lights, get_uber_lights)
        doc.wasDerivedFrom(uber_lights, streetlight_locations, get_uber_lights, get_uber_lights, get_uber_lights)

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


#    
#getUberLights.execute(True)
#doc = getUberLights.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof