import dml
import prov.model
import datetime
import uuid
import numpy as np

class optimization(dml.Algorithm):
    contributor = 'aoconno8_dmak1112_ferrys'
    reads = ['aoconno8_dmak1112_ferrys.shortest_path']
    writes = ['aoconno8_dmak1112_ferrys.optimized_routes']

    @staticmethod
    def execute(trial = False):
        def project(R, p):
            return [p(t) for t in R]
    
        startTime = datetime.datetime.now()
        print("Getting optimal route for each alcohol license...")
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')

        # get sidewalks as geojson
        shortest_paths_cursor = repo.aoconno8_dmak1112_ferrys.shortest_path.find()
        shortest_paths = project(shortest_paths_cursor, lambda t: t)
        
        best_paths = []            
        safest_count = 0
        shortest_count = 0
        for path in shortest_paths:
            alc_coord = path["alc_coord"]
            mbta_routes = path["mbta_routes"]
            
            routes = []
            route_scores = []
            for route_ in mbta_routes:
                mbta_coord = route_["mbta_coord"]
                route = route_["route"]
                route_dist = route_["route_dist"] # meters
                streetlights = route_["streetlights"] # format [(streetlight_node, # lights), ...]
                proj_streetlights = project(streetlights, lambda t: t[1])
                safest_route = route_["safest_route"]
                safest_route_dist = route_["safest_route_dist"]
                safest_route_streetlights = route_["safest_route_streetlights"]
                proj_safest_streetlights = project(safest_route_streetlights, lambda t: t[1])
                                
                # get variance of streetlights for each
                var = np.var(proj_streetlights)
                safest_var = np.var(proj_safest_streetlights)
                
                # get number of streetlights for each
                num_streetlights = np.sum(proj_streetlights)
                num_safest_streetlights = np.sum(proj_safest_streetlights)                
                
                # formula to determine "score" of the route
                # we want the shortest distance so we make it negative
                route_score = (.4*var + .4*num_streetlights + -.1*route_dist)
                
                # since we know this route has the most streetlights, we can make the distance 
                # matter a little less and instead make sure the streetlights are spread out
                safest_route_score = (.45*safest_var + .4*num_safest_streetlights + -.15*safest_route_dist)
                
                route_scores.append(route_score)
                route_scores.append(safest_route_score)
                
                # append route and safest route
                routes.append({
                        "alc_coord": alc_coord,
                        "mbta_route": {
                            "mbta_coord": mbta_coord,
                            "route":route, 
                            "route_dist":route_dist,
                            "streetlights":streetlights
                            }
                        })
                routes.append({
                        "alc_coord": alc_coord,
                        "mbta_route": {
                            "mbta_coord": mbta_coord,
                            "route":safest_route, 
                            "route_dist":safest_route_dist,
                            "streetlights":safest_route_streetlights
                            }
                        })
            
            # choose max score
            if (route_scores):
                index_best = route_scores.index(max(route_scores))
                if index_best % 2 == 1:
                    safest_count += 1
                else:
                    shortest_count += 1
                best_path = routes[index_best]
                best_paths.append(best_path)
                
        print("Picked safest route ", safest_count, " times.")
        print("Picked shortest route ", shortest_count, " times.")

        repo.dropCollection("optimized_routes")
        repo.createCollection("optimized_routes")
        repo['aoconno8_dmak1112_ferrys.optimized_routes'].insert_many(best_paths)
        repo['aoconno8_dmak1112_ferrys.optimized_routes'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112_ferrys.optimized_routes'].metadata())
        
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

        this_script = doc.agent('alg:aoconno8_dmak1112_ferrys#optimization', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        shortest_path = doc.entity('dat:aoconno8_dmak1112_ferrys#shortest_path', {prov.model.PROV_LABEL: 'Shortest Paths Between Alcohol Licenses and MBTA Stop Locations', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_optimized_routes = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_optimized_routes, this_script)

        doc.usage(get_optimized_routes, shortest_path, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        optimized_routes = doc.entity('dat:aoconno8_dmak1112_ferrys#optimized_routes', {prov.model.PROV_LABEL: 'Optimal path between every alcohol licesnse and an MBTA Stop', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(optimized_routes, this_script)
        doc.wasGeneratedBy(optimized_routes, get_optimized_routes, endTime)
        doc.wasDerivedFrom(optimized_routes, shortest_path, get_optimized_routes, get_optimized_routes, get_optimized_routes)
        
        repo.logout()
        return doc
                  
#optimization.execute()
#doc = optimization.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

