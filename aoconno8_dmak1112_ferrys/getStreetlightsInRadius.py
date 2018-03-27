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
            alcohol_mbta_stops = repo.aoconno8_dmak1112_ferrys.closest_mbta_stops.find().limit(3)
        else:
            alcohol_mbta_stops = repo.aoconno8_dmak1112_ferrys.closest_mbta_stops.find()
        projected_lights = project(streetlights_cursor, lambda t: (t['Long'], t['Lat']))
        thelights = []
        for light in projected_lights:
            thelights.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": light}})
        lights_geojson = {"features": thelights}
        g = geojson.dumps(lights_geojson)
        g = geoql.loads(g)
        for i in alcohol_mbta_stops:
            alc_coord = i['alc_coord']
            mbta_coords = i['mbta_coords']
            final_dict = {}
            final_dict["alc_coord"] = alc_coord
            mbta = {}
            mbta_coord_index = 1

            #Uncomment this and comment the loop below if you want just one radius for farthest mbta stop
            # mbta_coord = mbta_coords[2]
            # temp = copy.deepcopy(g)
            # radius = geopy.distance.vincenty(alc_coord, mbta_coord).miles
            # t = g.keep_within_radius((alc_coord), radius, 'miles')
            # final_dict["mbta_coord"] = mbta_coord
            # final_dict["streetlights_for_mbta_coord"] = t["features"]
            # g = copy.deepcopy(temp)
            # mbta_coord_index = mbta_coord_index + 1


            for mbta_coord in mbta_coords:
                temp = copy.deepcopy(g)
                radius = geopy.distance.vincenty(alc_coord, mbta_coord).miles
                t = g.keep_within_radius((alc_coord), radius, 'miles')
                final_dict["mbta_coord_" + str(mbta_coord_index)] = mbta_coord
                final_dict["streetlights_for_mbta_coord_" + str(mbta_coord_index)] = t["features"]
                g = copy.deepcopy(temp)
                mbta_coord_index = mbta_coord_index + 1
            alc_coord =''.join((str(e) + " ") for e in alc_coord)
            repo['aoconno8_dmak1112_ferrys.streetlights_in_radius'].insert(final_dict)

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

getStreetlightsInRadius.execute()
# doc = getStreetlightsInRadius.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof