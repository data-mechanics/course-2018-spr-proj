import urllib.request
import dml
import prov.model
import datetime
import uuid
from tqdm import tqdm
import osmnx as ox
import networkx as nx
import utm

class getShortestPath(dml.Algorithm):
    '''
        Gets the shortest path between each alcohol license and its three closest MBTA stops
        and gets all of the streetlights on that path
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
        # format {alc_coord: (x, y) mbta_coords: (x, y), streetlights, [(x, y), (x, y)...]}
        streetlights_in_radius_cursor = repo.aoconno8_dmak1112_ferrys.streetlights_in_radius.find()
        streetlights_in_radius = getShortestPath.project(streetlights_in_radius_cursor, lambda t: t)
        
        print("Step 1")

        G = ox.graph_from_place('Boston, Massachusetts, USA', network_type='drive')
        graph_project = ox.project_graph(G)
            
            
        print("Step 2")
        routes = []
        streetlight_nodes_store = {} # formatted {streetlight_loc: nearest_node}
        
        for streetlights in tqdm(streetlights_in_radius):
            alc_coord = streetlights['alc_coord']
            mbta_coords = streetlights['mbta_coords']
            streetlights_list = streetlights['streetlights']
            
            # get nearest node for alcohol coordinate
            orig_xy = (alc_coord)
            proj_orig_xy = utm.from_latlon(orig_xy[0], orig_xy[1])
            input_orig_xy = (proj_orig_xy[1], proj_orig_xy[0])
            orig_node = ox.get_nearest_node(graph_project, input_orig_xy, return_dist=True, method='euclidean')
            
            streetlight_nodes = {} # formatted {nearest_node: [list_of_streetlights]}
            for streetlight in tqdm(streetlights_list):
                # reformat as tuple so it can be a key in a dictionary
                streetlight_coord = (streetlight['geometry']['coordinates'][0], streetlight['geometry']['coordinates'][1])
                # if the streetlight is already stored, we already have the nearest node
                if streetlight_coord in streetlight_nodes_store:
                    nearest_node = streetlight_nodes_store[streetlight_coord]
                else:
                    # get the nearest node to the streetlight
                    proj_streetlight_coord = utm.from_latlon(streetlight_coord[1], streetlight_coord[0])
                    input_streetlight_coord = (proj_streetlight_coord[1], proj_streetlight_coord[0])
                    nearest_node, distance = ox.get_nearest_node(graph_project, input_streetlight_coord, return_dist=True, method='euclidean')
                    streetlight_nodes_store[streetlight_coord] = nearest_node
                if nearest_node in streetlight_nodes:
                    streetlight_nodes[nearest_node].append(streetlight_coord)
                else:
                    streetlight_nodes[nearest_node] = [streetlight_coord]
                    
            temp_routes = []
            for mbta_coord in tqdm(mbta_coords):
                # get nearest node to the mbta stop
                target_xy = mbta_coord
                proj_target_xy = utm.from_latlon(target_xy[0], target_xy[1])
                input_target_xy = (proj_target_xy[1], proj_target_xy[0])
                target_node = ox.get_nearest_node(graph_project, input_target_xy, return_dist=True, method='euclidean')
                try:
                    route = nx.shortest_path(G=G, source=orig_node[0], target=target_node[0], weight='length')
                except:
                    continue
 
                # determine which lights are on the route
                route_lights = []
                for node in route:
                    if node in streetlight_nodes:
                        route_lights += streetlight_nodes[node]
                
                temp_routes.append({
                    "mbta_coord": mbta_coord, 
                    "route": route, 
                    "streetlights": route_lights
                })
                
            routes.append({
                "alc_coord": orig_xy,
                "mbta_routes": temp_routes
            })
        print(routes)

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

