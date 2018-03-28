import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from tqdm import tqdm
import rtree
import shapely.geometry
import osmnx as ox
import networkx as nx
from matplotlib import pyplot as plt
import math
import utm

class getShortestPath(dml.Algorithm):
    '''
        Returns the closest x mbta stops 
    '''
    contributor = 'aoconno8_dmak1112_ferrys'
    reads = ['aoconno8_dmak1112_ferrys.closest_mbta_stops']
    writes = ['aoconno8_dmak1112_ferrys.shortest_path']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        api_key = dml.auth['services']['googlegeocoding']['key']
        
        print("Step 1")
        G = ox.graph_from_place('Boston, Massachusetts, USA', network_type='drive')
        graph_project = ox.project_graph(G)


        print("Step 2")
        # mbta
        num_mbta_stops = 3
        mbta = repo.aoconno8_dmak1112_ferrys.closest_mbta_stops.find()

        projected_mbta = getShortestPath.project(mbta, lambda t: (t['alc_license'], t['alc_coord'], t['mbta_coords']))

        

        if trial:
            projected_mbta = projected_mbta[:10]

        print("Step 3")
        coords = {}
        for i in range(len(projected_mbta)):
            key = projected_mbta[i][0]
            value = (projected_mbta[i][1], projected_mbta[i][2])
            coords[key] = value


        print("Step 4")
        routes = []
        for i in coords:
            license_key = i 
            temp_list = []
            orig_xy = (coords[i][0][0], coords[i][0][1])
            proj_orig_xy = utm.from_latlon(orig_xy[0], orig_xy[1])
            input_orig_xy = (proj_orig_xy[1], proj_orig_xy[0])
            orig_node = ox.get_nearest_node(graph_project, input_orig_xy, return_dist=True, method='euclidean')
            for j in range(len(coords[i][1])):
                target_xy = (coords[i][1][j][0], coords[i][1][j][1])
                proj_target_xy = utm.from_latlon(target_xy[0], target_xy[1])
                input_target_xy = (proj_target_xy[1], proj_target_xy[0])
                target_node = ox.get_nearest_node(graph_project, input_target_xy, return_dist=True, method='euclidean')
                try:
                    temp_route = nx.shortest_path(G=G, source=orig_node[0], target=target_node[0], weight='length')
                except:
                    pass
                temp_list.append(temp_route)

            routes.append({
                "alc_license": license_key,
                "routes": temp_list,
                "alc_coord": orig_xy,
                "mbta_coords": coords[i][1]
                })

        print("All done")
        repo.dropCollection("shortest_path")
        repo.createCollection("shortest_path")
        repo['aoconno8_dmak1112_ferrys.shortest_path'].insert_many(routes)
        repo['aoconno8_dmak1112_ferrys.shortest_path'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112_ferrys.shortest_path'].metadata())
        
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


getShortestPath.execute()
#doc = getClosestMBTAStops.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

