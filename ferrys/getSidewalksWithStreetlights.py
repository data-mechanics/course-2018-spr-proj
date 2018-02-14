import urllib.request
import dml
import prov.model
import datetime
import uuid
import json
import copy
import rtree
from tqdm import tqdm
import shapely.geometry

class getSidewalksWithStreetlights(dml.Algorithm):
    contributor = 'ferrys'
    reads = ['ferrys.sidewalks', 'ferrys.streetlights']
    writes = ['ferrys.sidewalkswithstreetlights']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ferrys', 'ferrys')
        
        # sidewalks
        sidewalks = repo.ferrys.sidewalks.find()
        projected_sidewalks = getSidewalksWithStreetlights.project(sidewalks, lambda t: t['geometry']['coordinates'][0])
        
        # streetlights
        streetlights = repo.ferrys.streetlights.find()
        projected_streetlights = getSidewalksWithStreetlights.project(streetlights, lambda t: [float(x) for x in t['the_geom'].replace(')', ' ').replace('(', ' ').split()[1:]])
        
        if trial:
            projected_sidewalks = projected_sidewalks[:10]
            projected_streetlights = projected_streetlights[:10]
        
        index = rtree.index.Index()
        for i in tqdm(range(len(projected_streetlights))):
            lon = projected_streetlights[i][0]
            lat = projected_streetlights[i][1]
            index.insert(i, shapely.geometry.Point(lon, lat).bounds)

        side_light = []
        for sidewalk in tqdm(projected_sidewalks):
            lights = []
            for point in sidewalk:
                lon = point[0]
                lat = point[1]
                try:
                    nearest = next(index.nearest((lon,lat,lon,lat), 1))
                except TypeError:
                    pass
                if getSidewalksWithStreetlights.is_close(point, projected_streetlights[nearest]):
                    print("lit")
                    lights += [projected_streetlights[nearest]]
            side_light.append({
                    'sidewalk': sidewalk,
                    'streetlights': lights
            })
        
        repo.dropCollection("sidewalkswithstreetlights")
        repo.createCollection("sidewalkswithstreetlights")
        repo['ferrys.sidewalkswithstreetlights'].insert_many(side_light)
        repo['ferrys.sidewalkswithstreetlights'].metadata({'complete':True})
        print(repo['ferrys.sidewalkswithstreetlights'].metadata())
                
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
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:ferrys#getSidewalksWithStreetlights', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        sidewalk_locations = doc.entity('bod:6aa3bdc3ff5443a98d506812825c250a_0', {'prov:label':'Sidewalk Location Geo Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        streetlight_locations = doc.entity('dat:ferrys#streetlights', {prov.model.PROV_LABEL:'streetlights', prov.model.PROV_TYPE:'ont:DataSet'})
        
        get_uber_lights = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_uber_lights, this_script)

        doc.usage(get_uber_lights, sidewalk_locations, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_uber_lights, streetlight_locations, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        uber_lights = doc.entity('dat:ferrys#sidewalkswithstreetlights', {prov.model.PROV_LABEL: 'Sidewalks with Streetlights', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(uber_lights, this_script)
        doc.wasGeneratedBy(uber_lights, get_uber_lights, endTime)
        doc.wasDerivedFrom(uber_lights, sidewalk_locations, get_uber_lights, get_uber_lights, get_uber_lights)
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
    
    def is_close(sidewalk_coordinate, streetlight_coordinate):
        try:
            return abs(sidewalk_coordinate[0] - streetlight_coordinate[0]) < .0001 and abs(sidewalk_coordinate[1] - streetlight_coordinate[1]) < .0001
        except:
            return False
    
#getSidewalksWithStreetlights.execute(True)
#doc = getSidewalksWithStreetlights.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

