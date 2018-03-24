import geojson
import geoql
import geoleaflet
import requests
import geopy
import urllib.request
import dml
import prov.model
import datetime
import uuid
import json
import copy
from geoql import geoql


class getStreetsInRadius(dml.Algorithm):
    contributor = 'aoconno8_dmak1112_ferrys'
    reads = ['aoconno8_dmak1112_ferrys.sidewalks', 'aoconno8_dmak1112_ferrys.closest_mbta_stops']
    writes = ['aoconno8_dmak1112_ferrys.streets_in_radius']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        # streetdata = repo.aoconno8_dmak1112_ferrys.sidewalks.find()
        # for i in streetdata:
        #     print(i)
        #     break
        alcohol_mbta_stops = repo.aoconno8_dmak1112_ferrys.closest_mbta_stops.find()



        # streetdata = geojson data of streets,
        # e.g.
        # print(g)
        # get sidewalks as geojson
        roads_cursor = repo.aoconno8_dmak1112_ferrys.roads.find()
        theroads = []
        for roads in roads_cursor:
            # print(roads)
            roads.pop('_id', None)
            theroads.append(roads)
        # roads_geojson = {"features": theroads[:10]}
        roads_geojson = {"features": theroads}

        # print(roads_geojson)
        g = geojson.dumps(roads_geojson)
        g = geoql.loads(g)
        # g = geoql.loads(requests.get(url + 'example_extract.geojson').text, encoding="latin-1")
        # print(g)
        # quit()
        for i in alcohol_mbta_stops:
            alc_coord = i['alc_coord']
            mbta_coords = i['mbta_coords']
            for mbta_coord in mbta_coords:
                temp = copy.deepcopy(g)
                radius = geopy.distance.vincenty(alc_coord, mbta_coord).miles
                print("radius is: ", radius)
                print("alc coord is: ", alc_coord)
                print("mbta coord is: ", mbta_coord)
                t = g.keep_within_radius((alc_coord), radius, 'miles')
                g = copy.deepcopy(temp)
                # break

    # 42.3522913, -71.0452839



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

getStreetsInRadius.execute()
doc = getStreetsInRadius.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof