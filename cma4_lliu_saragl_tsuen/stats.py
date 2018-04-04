import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


google_key = 'AIzaSyCs83Y5ODrwAOEko3-tJbZlNssYw56yd4A'


class stats(dml.Algorithm):
    contributor = 'cma4_lliu_saragl_tsuen'
    reads = ['cma4_lliu_saragl_tsuen.closest']
    writes = ['cma4_lliu_saragl_tsuen.stats']

    @staticmethod
    def execute(trial = True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cma4_lliu_saragl_tsuen', 'cma4_lliu_saragl_tsuen')            

        #url = 'https://data.boston.gov/export/458/2be/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c.json'
        k_means = None
        if trial:
            k_means = repo['cma4_lliu_saragl_tsuen.closest'].aggregate([{'$sample': {'size': 1}}], allowDiskUse=True)
        else:
            k_means = repo['cma4_lliu_saragl_tsuen.closest'].find()


        k_means = list(k_means)

        for i in k_means:
            #print(i)
            url = 'https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&origins='+str(i['stationCoords'][0]) + ',' +str(i['stationCoords'][1]) + '&destinations=' + str(i['coords'][0]) + '%2C' + str(i['coords'][1]) + '&key=' + google_key
            #print(url)
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            #print(r)
            #repo['cma4_lliu_saragl_tsuen.closest'].update_many({'name':i['name']}, {"$set":{'googlemaps': (r['rows'][0]['elements'][0]['distance']['value'], r['rows'][0]['elements'][0]['duration']['value'])}})
            i['googlemaps'] = (r['rows'][0]['elements'][0]['distance']['value'], r['rows'][0]['elements'][0]['duration']['value'])

        #Calculate mean distance and time values
        totalDist = 0
        totalTime = 0
        count = 0
        for i in k_means:
            totalDist += i['googlemaps'][0]
            totalTime += i['googlemaps'][1]
            count += 1

        averages = (totalDist/count, totalTime/count)

        totalLSE_dist = 0
        totalLSE_time = 0
        for i in k_means:
            totalLSE_dist += i['googlemaps'][0] - averages[0]
            totalLSE_time += i['googlemaps'][1] - averages[1]


        #scale stdevs so numbers aren't so small that mongo doesn't accept them - will be scaled up again when needed
        stdev_dist = ((totalLSE_dist / (count - 1)) ** (1/2)) * 10000000
        stdev_time = (totalLSE_time / (count - 1)) ** (1/2) * 10000000

        print(stdev_dist)
        print(stdev_time)

        # add z-score, or how many stdevs away from average, each destination-station pairing's distance and time is. Should be very similar, if not the same
        for i in k_means:
            actualDist = i['googlemaps'][0] 
            actualTime = i['googlemaps'][1]

            zscore_dist = (actualDist - averages[0]) / stdev_dist
            zscore_time = (actualTime - averages[1]) / stdev_time

            i['zscores'] = (zscore_dist, zscore_time)

        print(k_means)

        repo.dropCollection("cma4_lliu_saragl_tsuen.distances")
        repo.createCollection("cma4_lliu_saragl_tsuen.distances")
        repo['cma4_lliu_saragl_tsuen.distances'].insert_many(k_means)
        repo['cma4_lliu_saragl_tsuen.distances'].metadata({'complete':True})
        print(repo['cma4_lliu_saragl_tsuen.distances'].metadata())

        
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
        repo.authenticate('cma4_lliu_saragl_tsuen', 'cma4_lliu_saragl_tsuen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:cma4_lliu_saragl_tsuen#stats', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:stats', {'prov:label':'Final Data with Stats', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_final = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_final, this_script)
        doc.usage(get_final, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        stats = doc.entity('dat:cma4_lliu_saragl_tsuen#stats', {prov.model.PROV_LABEL:'Food Places', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(stats, this_script)
        doc.wasGeneratedBy(stats, get_final, endTime)
        doc.wasDerivedFrom(stats, resource, get_final, get_final, get_final)

        repo.logout()
                  
        return doc
stats.execute()
doc = stats.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
