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
    reads = ['aoconno8_dmak1112_ferrys.streetlights_in_radius']
    writes = ['aoconno8_dmak1112_ferrys.shortest_path']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        print("Getting shortest paths for each alcohol license...")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        # format {alc_coord: (x, y) mbta_coords: (x, y), streetlights, [(x, y), (x, y)...]}
        streetlights_in_radius_cursor = repo.aoconno8_dmak1112_ferrys.streetlights_in_radius.find()
        streetlights_in_radius = getShortestPath.project(streetlights_in_radius_cursor, lambda t: t)
        
        print("Generating graph...")
        if trial:
            streetlights_in_radius = streetlights_in_radius[:1]
            coordinate = streetlights_in_radius[0]['alc_coord']
            G = ox.graph_from_point(coordinate, 1000, network_type='drive')
        else:
            G = ox.graph_from_place('Boston, Massachusetts, USA', network_type='drive')
        
        graph_project = ox.project_graph(G)
            
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
            
            length_streetlight_nodes = {key: len(value) for key, value in streetlight_nodes.items()}
            # gets the "safest" node (i.e. the one with the most streetlights)
            if len(length_streetlight_nodes) > 0:
                safest_node = max(length_streetlight_nodes, key=length_streetlight_nodes.get)
            
            temp_routes = []
            for mbta_coord in tqdm(mbta_coords):
                # get nearest node to the mbta stop
                target_xy = mbta_coord
                proj_target_xy = utm.from_latlon(target_xy[0], target_xy[1])
                input_target_xy = (proj_target_xy[1], proj_target_xy[0])
                target_node = ox.get_nearest_node(graph_project, input_target_xy, return_dist=True, method='euclidean')
                try:
                    # get the shortest path based on distance
                    route = nx.shortest_path(G=G, source=orig_node[0], target=target_node[0], weight='length')
                    route_dist = nx.shortest_path_length(G=G, source=orig_node[0], target=target_node[0], weight='length')
                    
                    # get the shortest path that includes the "safest" node
                    if safest_node:
                        route_to_safe = nx.shortest_path(G=G, source=orig_node[0], target=safest_node, weight='length')
                        route_from_safe = nx.shortest_path(G=G, source=safest_node, target=target_node[0], weight='length')
                        entire_safest_route = route_to_safe[:-1] + route_from_safe
                        
                        route_to_safe_dist = nx.shortest_path_length(G=G, source=orig_node[0], target=safest_node, weight='length')
                        route_from_safe_dist = nx.shortest_path_length(G=G, source=safest_node, target=target_node[0], weight='length')
                        entire_safest_route_dist = route_to_safe_dist + route_from_safe_dist    
                    else:
                        entire_safest_route = []
                        entire_safest_route_dist = 0
                except:
                    continue
 
                # determine how many lights are near each node of the route
                route_lights = []
                entire_safest_route_lights = []
                for node in route:
                    if node in length_streetlight_nodes:
                        route_lights += [(node, length_streetlight_nodes[node])]
                for node in entire_safest_route:
                    if node in length_streetlight_nodes:
                        entire_safest_route_lights += [(node, length_streetlight_nodes[node])]
                
                temp_routes.append({
                    "mbta_coord": mbta_coord, 
                    "route": route, 
                    "route_dist": route_dist,
                    "streetlights": route_lights, 
                    "safest_route": entire_safest_route,
                    "safest_route_dist": entire_safest_route_dist,
                    "safest_route_streetlights": entire_safest_route_lights
                })
                
            routes.append({
                "alc_coord": orig_xy,
                "mbta_routes": temp_routes
            })

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
        doc.add_namespace('osm', 'https://openstreetmap.org/')

        this_script = doc.agent('alg:aoconno8_dmak1112_ferrys#getShortestPath', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        streetlights_radius = doc.entity('dat:aoconno8_dmak1112_ferrys#streetlights_in_radius', {prov.model.PROV_LABEL: 'All streetlights in the radius of each alc license closest MBTA stops', prov.model.PROV_TYPE: 'ont:DataSet'})
        osm = doc.entity('osm:api', {'prov:label':'OpenStreetMap', prov.model.PROV_TYPE:'ont:DataResource'})

        get_shortest_path = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_shortest_path, this_script)

        doc.usage(get_shortest_path, streetlights_radius, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_shortest_path, osm, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        shortest_path = doc.entity('dat:aoconno8_dmak1112_ferrys#shortest_path', {prov.model.PROV_LABEL: 'Shortest Paths Between Alcohol Licenses and MBTA Stop Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(shortest_path, this_script)
        doc.wasGeneratedBy(shortest_path, get_shortest_path, endTime)
        doc.wasDerivedFrom(shortest_path, streetlights_radius, get_shortest_path, get_shortest_path, get_shortest_path)
        doc.wasDerivedFrom(shortest_path, osm, get_shortest_path, get_shortest_path, get_shortest_path)

        repo.logout()
        return doc

    def project(R, p):
        return [p(t) for t in R]


#getShortestPath.execute()
#doc = getShortestPath.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

