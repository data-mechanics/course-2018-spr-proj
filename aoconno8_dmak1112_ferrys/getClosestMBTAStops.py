import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from tqdm import tqdm
import rtree
import shapely.geometry
import numpy as np

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
        print("Getting closest MBTA stops to every alcohol license...")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        api_key = dml.auth['services']['googlegeocoding']['key']
        
        
        # mbta
        num_mbta_stops = 3
        mbta = repo.aoconno8_dmak1112_ferrys.mbta.find()
        projected_mbta = getClosestMBTAStops.project(mbta, lambda t: (t['attributes']['latitude'], t['attributes']['longitude'], t['attributes']['name']))


        # alc
        alc = repo.aoconno8_dmak1112_ferrys.alc_licenses.find()
        projected_alc = getClosestMBTAStops.project(alc, lambda t: (t['License Number'], t['Street Number'] + ' ' + t['Street Name'] + ' ' +  str(t['Suffix']) + ' ' + t['City'], t['Business Name'], t['DBA']))

        if trial:
            # algorithm wasnt working well on trial because the mbta stops
            # and alcohol licenses were so far away from each other
            # so I just picked a small subset of close ones
            projected_alc = [["678", (42.3516079, -71.080906), "Business Name", "Name"]]
            alc_lat = projected_alc[0][1][0]
            alc_long = projected_alc[0][1][1]
            
            projected_mbta = [[42.350067, -71.078068, "Copley"], [42.348227, -71.075493, "Back Bay"], [42.349224, -71.080600, "Ring Rd @ Boylston St"]]
            
        index = rtree.index.Index()
        for i in tqdm(range(len(projected_mbta))):
            lat = projected_mbta[i][0]
            lon = projected_mbta[i][1]
            index.insert(i, shapely.geometry.Point(lon, lat).bounds)

        cache = {}
        mbta_dist = []
        for alc_entry in tqdm(projected_alc):
            if not trial:
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
                        "alc_name": alc_entry[2] if (type(alc_entry[3]) != str and np.isnan(alc_entry[3])) else alc_entry[3],
                        "mbta_coords":(mbta_coords)
                    })

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

        this_script = doc.agent('alg:aoconno8_dmak1112_ferrys#getClosestMBTAStops', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        licenses = doc.entity('dat:aoconno8_dmak1112_ferrys#alc_licenses', {prov.model.PROV_LABEL:'alc_licenses', prov.model.PROV_TYPE:'ont:DataSet'})
        mbta_stops = doc.entity('dat:aoconno8_dmak1112_ferrys#mbta', {prov.model.PROV_LABEL:'mbta', prov.model.PROV_TYPE:'ont:DataSet'})
        geocode_locations = doc.entity('geocode:json', {'prov:label':'Google Geocode API', prov.model.PROV_TYPE:'ont:DataResource'})


        get_mbta_dist = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_mbta_dist, this_script)

        doc.usage(get_mbta_dist, licenses, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_mbta_dist, mbta_stops, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_mbta_dist, geocode_locations, startTime, None, 
                  {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?address=$&key=$'})

        closest_mbta_stops = doc.entity('dat:aoconno8_dmak1112_ferrys#closest_mbta_stops', {prov.model.PROV_LABEL: 'Alcohol Licenses and MBTA Stop Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(closest_mbta_stops, this_script)
        doc.wasGeneratedBy(closest_mbta_stops, get_mbta_dist, endTime)
        doc.wasDerivedFrom(closest_mbta_stops, licenses, get_mbta_dist, get_mbta_dist, get_mbta_dist)
        doc.wasDerivedFrom(closest_mbta_stops, mbta_stops, get_mbta_dist, get_mbta_dist, get_mbta_dist)
        doc.wasDerivedFrom(closest_mbta_stops, geocode_locations, get_mbta_dist, get_mbta_dist, get_mbta_dist)

        repo.logout()
        return doc


    def project(R, p):
        return [p(t) for t in R]


#getClosestMBTAStops.execute()
#doc = getClosestMBTAStops.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

