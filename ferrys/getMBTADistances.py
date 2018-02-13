import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy

class getMBTADistances(dml.Algorithm):
    '''
    Returns the number of MBTA stops near a specific alcohol license
    '''
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
        projected_alc = getMBTADistances.project(alc, lambda t: (t['License Number'], t['Street Number'] + ' ' + t['Street Name'] + ' ' +  str(t['Suffix']) + ' ' + t['City']))

        if trial:
            projected_mbta = projected_mbta[:10]
            projected_alc = projected_alc[:10]

        cache = {}
        mbta_dist = []
        for alc_entry in projected_alc:
            for mbta_entry in projected_mbta:
                alc_address = alc_entry[1].replace(' ',  '+')
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
                            "alc_license": alc_entry[0],
                            "alc_coord":(alc_lat, alc_long),
                            "mbta_coord":(mbta_entry[0], mbta_entry[1])
                        })

        close_points = getMBTADistances.select(mbta_dist, lambda x: getMBTADistances.is_close((x['alc_coord'][0], x['alc_coord'][1]), (x['mbta_coord'][0], x['mbta_coord'][1])))
        num_mbta_near = getMBTADistances.aggregate_mbta(close_points, lambda x: sum([1 for x in x]))
        print(num_mbta_near)
        repo.dropCollection('mbtadistance')
        repo.createCollection('mbtadistance')
        repo['ferrys.mbtadistance'].insert_many(num_mbta_near)
        repo['ferrys.mbtadistance'].metadata({'complete':True})
        print(repo['ferrys.mbtadistance'].metadata())
        
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

        this_script = doc.agent('alg:ferrys#getMBTADistances', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        licenses = doc.entity('dat:ferrys#alc_licenses', {prov.model.PROV_LABEL:'alc_licenses', prov.model.PROV_TYPE:'ont:DataSet'})
        mbta_stops= doc.entity('dat:ferrys#mbta', {prov.model.PROV_LABEL:'mbta', prov.model.PROV_TYPE:'ont:DataSet'})
        geocode_locations = doc.entity('geocode:json', {'prov:label':'Google Geocode API', prov.model.PROV_TYPE:'ont:DataResource'})


        get_mbta_dist = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_mbta_dist, this_script)

        doc.usage(get_mbta_dist, licenses, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_mbta_dist, mbta_stops, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_mbta_dist, geocode_locations, startTime, None, 
                  {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?address=$&key=$'})

        mbta_dist = doc.entity('dat:ferrys#mbtadistance', {prov.model.PROV_LABEL: 'Alcohol Licenses and MBTA Stop Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(mbta_dist, this_script)
        doc.wasGeneratedBy(mbta_dist, get_mbta_dist, endTime)
        doc.wasDerivedFrom(mbta_dist, licenses, get_mbta_dist, get_mbta_dist, get_mbta_dist)
        doc.wasDerivedFrom(mbta_dist, mbta_stops, get_mbta_dist, get_mbta_dist, get_mbta_dist)
        doc.wasDerivedFrom(mbta_dist, geocode_locations, get_mbta_dist, get_mbta_dist, get_mbta_dist)

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
    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]
    def aggregate_mbta(R, f):
        keys = {r['alc_license'] for r in R}
        return [{key: f([v for v in R if v['alc_license'] == key])} for key in keys]
    def project(R, p):
        return [p(t) for t in R]
    def is_close(mbta_stop, alc_license):
        # around 1.5 miles apart
        return abs(mbta_stop[0] - alc_license[0]) < .015 and abs(mbta_stop[1] - alc_license[1]) < .015


#getMBTADistances.execute(True)
#doc = getMBTADistances.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

