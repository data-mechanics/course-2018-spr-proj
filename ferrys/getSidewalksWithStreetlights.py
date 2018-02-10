import urllib.request
import geojson
import dml
import prov.model
import datetime
import uuid
import json
import copy

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
        
        
        side_light = []
        for sidewalk in projected_sidewalks:
            lights = []
            for coordinate in sidewalk:
                for streetlight in projected_streetlights:
                    if getSidewalksWithStreetlights.is_close(coordinate, streetlight):
                        print("lit")
                        lights += [streetlight]
            
            side_light.append({
                            'sidewalk': sidewalk,
                            'streetlights': lights
                            })         
        

        side_light_cp = copy.deepcopy(side_light)
        repo.dropCollection("sidewalkswithstreetlights")
        repo.createCollection("sidewalkswithstreetlights")
        repo['ferrys.sidewalkswithstreetlights'].insert_many(side_light)
        repo['ferrys.sidewalkswithstreetlights'].metadata({'complete':True})
        print(repo['ferrys.sidewalkswithstreetlights'].metadata())


        with open("../datasets/Sidewalks_With_Streetlights.json", 'w') as file:
                json.dump(side_light_cp, file)
                
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
    
    def is_close(sidewalk_coordinate, streetlight_coordinate):
        try:
            return abs(sidewalk_coordinate[0] - streetlight_coordinate[0]) < .0001 and abs(sidewalk_coordinate[1] - streetlight_coordinate[1]) < .0001
        except:
            return False
    
getSidewalksWithStreetlights.execute()
#doc = example.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

