import geojson
import geoql
import geopy
import dml
import prov.model
import datetime
import uuid
import copy
from geoql import geoql



class getStreetlightsInRadius(dml.Algorithm):
    contributor = 'aoconno8_dmak1112_ferrys'
    reads = ['aoconno8_dmak1112_ferrys.streetlights', 'aoconno8_dmak1112_ferrys.closest_mbta_stops']
    writes = ['aoconno8_dmak1112_ferrys.streetlights_in_radius']

    @staticmethod
    def execute(trial=False):
        def project(R, p):
            return [p(t) for t in R]
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        repo.dropCollection("streetlights_in_radius")
        repo.createCollection("streetlights_in_radius")
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        streetlights_cursor = repo.aoconno8_dmak1112_ferrys.streetlights.find()
        
        if trial:
            alcohol_mbta_stops = repo.aoconno8_dmak1112_ferrys.closest_mbta_stops.find().limit(1)
        else:
            alcohol_mbta_stops = repo.aoconno8_dmak1112_ferrys.closest_mbta_stops.find()
            
        projected_lights = project(streetlights_cursor, lambda t: (t['Long'], t['Lat']))
        
        the_lights = []
        for light in projected_lights:
            the_lights.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": light}})
            
        lights_geojson = {"features": the_lights}
        lights_geoql = geoql.loads(geojson.dumps(lights_geojson))
        
        alcohol_streetlight_list = []
        for alcohol_mbta in alcohol_mbta_stops:
            # make a copy so it doesn't get written over
            temp = copy.deepcopy(lights_geoql)

            alc_coord = alcohol_mbta['alc_coord']
            mbta_coords = alcohol_mbta['mbta_coords']
            
            # get the max radius
            max_radius = 0
            for mbta_coord in mbta_coords:
                radius = geopy.distance.vincenty(alc_coord, mbta_coord).miles
                if radius > max_radius:
                    max_radius = radius
            # get the lights in that radius
            t = lights_geoql.keep_within_radius((alc_coord), radius, 'miles')
            # create a new record with alcohol coordinate, mbta coordinates, and all the streetlights
            alcohol_streetlight_list.append({
                    "alc_coord": alc_coord,
                    "mbta_coords":mbta_coords,
                    "streetlights":t["features"]
                })
            
            lights_geoql = copy.deepcopy(temp)
                
        repo['aoconno8_dmak1112_ferrys.streetlights_in_radius'].insert_many(alcohol_streetlight_list)
        repo['aoconno8_dmak1112_ferrys.streetlights_in_radius'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112_ferrys.streetlights_in_radius'].metadata())

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:aoconno8_dmak1112_ferrys#getSidewalks',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bod:6aa3bdc3ff5443a98d506812825c250a_0',
                              {'prov:label': 'Sidewalk Location Geo Data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'geojson'})
        get_sidewalks = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_sidewalks, this_script)
        doc.usage(get_sidewalks, resource, startTime, None,
                  {
                      prov.model.PROV_TYPE: 'ont:Retrieval'
                  })

        sidewalk_locations = doc.entity('dat:aoconno8_dmak1112_ferrys#sidewalks',
                                        {prov.model.PROV_LABEL: 'sidewalks', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(sidewalk_locations, this_script)
        doc.wasGeneratedBy(sidewalk_locations, get_sidewalks, endTime)
        doc.wasDerivedFrom(sidewalk_locations, resource, get_sidewalks, get_sidewalks, get_sidewalks)

        repo.logout()

        return doc

#getStreetlightsInRadius.execute()
# doc = getStreetlightsInRadius.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof