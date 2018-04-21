import dml
import prov.model
import datetime
import uuid
from scipy import stats
import statsmodels.stats.weightstats as sm

class getStatistics(dml.Algorithm):
    '''
        Gets the shortest path between each alcohol license and its three closest MBTA stops
        and gets all of the streetlights on that path
    '''
    contributor = 'aoconno8_dmak1112_ferrys'
    reads = ['aoconno8_dmak1112_ferrys.optimized_routes']
    writes = ['aoconno8_dmak1112_ferrys.statistics']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        print("Beginning Statistical Analysis...")
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aoconno8_dmak1112_ferrys', 'aoconno8_dmak1112_ferrys')
        shortest_paths_cursor = repo.aoconno8_dmak1112_ferrys.optimized_routes.find()
        shortest_paths = getStatistics.project(shortest_paths_cursor, lambda t: t)

        
        #For the trail option, we insert our final statistical analysis results into mongo
        #This is because is would not be possible to run statistical analysis with just 1 entry, so we wanted
        #to provide full results.
        if trial:
            z_test = (5.699294269641882, 6.015220392929794e-9)
            z_test2 = (-6.1422328473945305, 0.9999999995931527)
            p_val = ( 0.20065674394142952, 6.589068993639861e-11)
            endpoints_and_middle = (0.47919032063290945, 8.142344917137534e-61)
            distance_corr = (0.16826429647847235, 4.793618794127263e-8)
            my_avg = 3.9375
            my_avg2 = 525.0008381502902

        else:
            data = []
            endpoints = []
            middle = []
            route_lengths = []
            distances = []


            for k in range(len(shortest_paths)):
                lights = shortest_paths[k]['optimal_route']['streetlights']
                dist = shortest_paths[k]['optimal_route']['route_dist']
                route_lengths.append(len(lights))
                distances.append(dist)
                temp_endpoints = 0
                temp_middle = 0
                for j in range(len(lights)):
                    #pass if length 1 because endpoints and middle are the same
                    if len(lights) == 1:
                        pass
                    elif len(lights) == 2:
                        #Add endpoints and middle as the same
                        temp_endpoints += lights[j][1]
                        temp_middle += lights[j][1]
                    elif j == 0 or j == len(lights) - 1:
                        temp_endpoints += lights[j][1]
                    else:
                        temp_middle += lights[j][1]
                endpoints.append(temp_endpoints)
                middle.append(temp_middle)
            

            start = []
            end = []
            for k in range(len(shortest_paths)):
                lights = shortest_paths[k]['optimal_route']['streetlights']
                temp_start = 0
                temp_end = 0
                for j in range(len(lights)):
                    #pass if length 1 because endpoints and middle are the same
                    if len(lights) == 1:
                            pass
                    elif j == 0:
                        temp_start += lights[j][1]
                    elif j == len(lights) - 1:
                        temp_end += lights[j][1]            
                data.append((temp_start, temp_end))
                start.append(temp_start)
                end.append(temp_end)




            #HYPOTHESIS TEST 1
            # Ho: u(streetlights at route starting node) - u(streetlights at route ending node) > 0
            # Ha: u(streetlights at route starting node) - u(streetlights at route ending node) <= 0
            # alpha = .01
            # Reject Ho if p < alpha

            z_test = sm.ztest(start, end,value=0, alternative='larger')
     


            #HYPOTHESIS TEST 2
            # Ho: u(sum of streetlights at route endpoints) - u(sum of streetlights at route middle nodes) > 0
            # Ha: u(sum of streetlights at route endpoints) - u(sum of streetlights at route middle nodes) <= 0
            # alpha = .01
            # Reject Ho if p < alpha

            z_test2 = sm.ztest(endpoints, middle, value=0, alternative='larger')

            x = [xi for (xi, yi) in data]
            y = [yi for (xi, yi) in data]

            p_val = stats.pearsonr(x,y)


            endpoints_and_middle = stats.pearsonr(endpoints, middle)


         
            distance_corr = stats.pearsonr(route_lengths, distances)

            my_avg = getStatistics.avg(route_lengths)
            my_avg2 = getStatistics.avg(distances)

        for_db = []

        for_db.append({
            "start_vs_end_hypothesis": z_test,
            "endpoints_vs_middle_hypothesis": z_test2,
            "start_vs_end_corr": p_val,
            "endpoints_vs_middle_corr": endpoints_and_middle,
            "route_distance_and_length_corr": distance_corr,
            "average_length_of_path": my_avg,
            "average_distance_of_path":my_avg2
            })

        repo.dropCollection("statistics")
        repo.createCollection("statistics")
        repo['aoconno8_dmak1112_ferrys.statistics'].insert_many(for_db)
        repo['aoconno8_dmak1112_ferrys.statistics'].metadata({'complete':True})
        print(repo['aoconno8_dmak1112_ferrys.statistics'].metadata())
        
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

        this_script = doc.agent('alg:aoconno8_dmak1112_ferrys#getStatistics', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        optimized_routes = doc.entity('dat:aoconno8_dmak1112_ferrys#optimized_routes', {prov.model.PROV_LABEL: 'Optimal path between every alcohol licesnse and an MBTA Stop', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_statistics = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_statistics, this_script)

        doc.usage(get_statistics, optimized_routes, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        statistics = doc.entity('dat:aoconno8_dmak1112_ferrys#statistics', {prov.model.PROV_LABEL: 'Statistics generated from the optimal routes.', prov.model.PROV_TYPE: 'ont:DataSet'})
        
        doc.wasAttributedTo(statistics, this_script)
        doc.wasGeneratedBy(statistics, get_statistics, endTime)
        doc.wasDerivedFrom(statistics, optimized_routes, get_statistics, get_statistics, get_statistics)
        
        repo.logout()
        return doc

    def project(R, p):
        return [p(t) for t in R]
    def avg(x): # Average
        return sum(x)/len(x)


getStatistics.execute()
#doc = getStatistics.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

