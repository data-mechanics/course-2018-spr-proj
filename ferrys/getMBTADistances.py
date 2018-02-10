import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy

class getMBTADistances(dml.Algorithm):
    contributor = 'ferrys'
    reads = ['ferrys.mbta', 'ferrys.alc_licenses']
    writes = ['ferrys.mbtadistance']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ferrys', 'ferrys')
        api_key = dml.auth['services']['googlegeocoding']['key']
        
        # mbta
        mbta = repo.ferrys.mbta.find()
        projected_mbta = getMBTADistances.project(mbta, lambda t: (t['attributes']['latitude'], t['attributes']['longitude']))


        # alc
        alc = repo.ferrys.alc_licenses.find()
        projected_alc = getMBTADistances.project(alc, lambda t: (t['Street Number'] + ' ' + t['Street Name'] + ' ' +  str(t['Suffix']) + ' ' + t['City']))


        cache = {}
        mbta_dist = []
        for alc_entry in projected_alc:
            for mbta_entry in projected_mbta:
                alc_address = alc_entry.replace(' ',  '+')
                if alc_address in cache:
                    alc_lat = cache[alc_address][0]
                    alc_long = cache[alc_address][1]
                else:
                    alc_url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + alc_address + 'MA&key='+ api_key
                    response = urllib.request.urlopen(alc_url).read().decode("utf-8")
                    google_json = json.loads(response)
                    try:
                        alc_lat = google_json["results"][0]["geometry"]["location"]['lat']
                        alc_long = google_json["results"][0]["geometry"]["location"]['lng']
                        cache[alc_address] = (alc_lat,alc_long)
                    except IndexError:
                        print("Address not found")
                        alc_lat = 0
                        alc_long = 0
                        cache[alc_address] = (alc_lat,alc_long)

                mbta_dist.append({
                            "alc_coord":(alc_lat, alc_long),
                            "mbta_coord":(mbta_entry[0], mbta_entry[1])
                        })
        mbta_dist_cp = copy.deepcopy(mbta_dist)
        repo.dropCollection('mbtadistance')
        repo.createCollection('mbtadistance')
        repo['ferrys.mbtadistance'].insert_many(mbta_dist)
        repo['ferrys.mbtadistance'].metadata({'complete':True})
        print(repo['ferrys.mbtadistance'].metadata())
        
        with open("../datasets/MBTA_Uber_Distances.json", 'w') as file:
                json.dump(mbta_dist_cp, file)
        
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

getMBTADistances.execute()
#doc = example.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

