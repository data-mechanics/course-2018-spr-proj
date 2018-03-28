import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from tqdm import tqdm
import rtree
import shapely.geometry

class getClosestMBTAStops(dml.Algorithm):
    '''
        Returns the closest x mbta stops 
    '''
    contributor = 'aoconno8_dmak1112_ferrys'
    reads = ['aoconno8_dmak1112_ferrys.mbta', 'aoconno8_dmak1112_ferrys.alc_licenses']
    writes = ['aoconno8_dmak1112_ferrys.closest_mbta_stops']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        api_key = dml.auth['services']['googlegeocoding']['key']
        
        
        # mbta
        num_mbta_stops = 3
        mbta = repo.aoconno8_dmak1112_ferrys.mbta.find()
        projected_mbta = getClosestMBTAStops.project(mbta, lambda t: (t['attributes']['latitude'], t['attributes']['longitude']))


        # alc
        alc = repo.aoconno8_dmak1112_ferrys.alc_licenses.find()
        projected_alc = getClosestMBTAStops.project(alc, lambda t: (t['License Number'], t['Street Number'] + ' ' + t['Street Name'] + ' ' +  str(t['Suffix']) + ' ' + t['City']))

        if trial:
            projected_mbta = projected_mbta[:10]
            projected_alc = projected_alc[:10]
            
        index = rtree.index.Index()
        for i in tqdm(range(len(projected_mbta))):
            lat = projected_mbta[i][0]
            lon = projected_mbta[i][1]
            index.insert(i, shapely.geometry.Point(lon, lat).bounds)

        cache = {}
        mbta_dist = []
        for alc_entry in tqdm(projected_alc):
            
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
                    continue
                    
            # get x nearest mbta stops from the alc license
            try:
                nearest = index.nearest((alc_long,alc_lat,alc_long,alc_lat), num_results=num_mbta_stops)
            except TypeError:
                pass
            
            mbta_coords = []
            for point in nearest:
                mbta_coords += [projected_mbta[point]]
            
            mbta_dist.append({
                        "alc_license": alc_entry[0],
                        "alc_coord":(alc_lat, alc_long),
                        "mbta_coords":(mbta_coords)
                    })

        print(mbta_dist)
        repo.dropCollection("closest_mbta_stops")
        repo.createCollection("closest_mbta_stops")
        repo['aoconno8_dmak1112_ferrys.closest_mbta_stops'].insert_many(mbta_dist)
        repo['aoconno8_dmak1112_ferrys.closest_mbta_stops'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112_ferrys.closest_mbta_stops'].metadata())
        
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
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('geocode', 'https://maps.googleapis.com/maps/api/geocode')

        this_script = doc.agent('alg:aoconno8_dmak1112_ferrys#getMBTADistances', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        licenses = doc.entity('dat:aoconno8_dmak1112_ferrys#alc_licenses', {prov.model.PROV_LABEL:'alc_licenses', prov.model.PROV_TYPE:'ont:DataSet'})
        mbta_stops = doc.entity('dat:aoconno8_dmak1112_ferrys#mbta', {prov.model.PROV_LABEL:'mbta', prov.model.PROV_TYPE:'ont:DataSet'})
        geocode_locations = doc.entity('geocode:json', {'prov:label':'Google Geocode API', prov.model.PROV_TYPE:'ont:DataResource'})


        get_mbta_dist = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_mbta_dist, this_script)

        doc.usage(get_mbta_dist, licenses, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_mbta_dist, mbta_stops, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_mbta_dist, geocode_locations, startTime, None, 
                  {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?address=$&key=$'})

        mbta_dist = doc.entity('dat:aoconno8_dmak1112_ferrys#mbtadistance', {prov.model.PROV_LABEL: 'Alcohol Licenses and MBTA Stop Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(mbta_dist, this_script)
        doc.wasGeneratedBy(mbta_dist, get_mbta_dist, endTime)
        doc.wasDerivedFrom(mbta_dist, licenses, get_mbta_dist, get_mbta_dist, get_mbta_dist)
        doc.wasDerivedFrom(mbta_dist, mbta_stops, get_mbta_dist, get_mbta_dist, get_mbta_dist)
        doc.wasDerivedFrom(mbta_dist, geocode_locations, get_mbta_dist, get_mbta_dist, get_mbta_dist)

        repo.logout()
        return doc


    def project(R, p):
        return [p(t) for t in R]


getClosestMBTAStops.execute()
#doc = getClosestMBTAStops.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

