import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


google_key = 'AIzaSyD_SNKUiwDVf_LjMGyIZxLf9MMaWB2IqH0'


class distances(dml.Algorithm):
    contributor = 'cma4_lliu_saragl_tsuen'
    reads = ['cma4_lliu_saragl_tsuen.closest']
    writes = ['cma4_lliu_saragl_tsuen.distances']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cma4_lliu_saragl_tsuen', 'cma4_lliu_saragl_tsuen')            

        #url = 'https://data.boston.gov/export/458/2be/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c.json'
        k_means = repo['cma4_lliu_saragl_tsuen.closest'].find()
        for i in k_means:
            url = 'https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&origins='+str(i['stationCoords'][0]) + ',' +str(i['stationCoords'][1]) + '&destinations=' + str(i['coords'][0]) + '%2C' + str(i['coords'][0]) + '&key=' + google_key
            #print(url)
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            print(r)

        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cma4_lliu_saragl_tsuen.distances")
        repo.createCollection("cma4_lliu_saragl_tsuen.distances")
        repo['cma4_lliu_saragl_tsuen.distances'].insert_many(r)
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
        doc.add_namespace('food', 'https://data.boston.gov/export/458/2be/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c.json')

        this_script = doc.agent('alg:cma4_lliu_saragl_tsuen#food', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:food', {'prov:label':'Food Locations Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_places = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_places, this_script)
        doc.usage(get_places, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        food = doc.entity('dat:cma4_lliu_saragl_tsuen#food', {prov.model.PROV_LABEL:'Food Places', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(food, this_script)
        doc.wasGeneratedBy(food, get_places, endTime)
        doc.wasDerivedFrom(food, resource, get_places, get_places, get_places)

        repo.logout()
                  
        return doc
#food.PartToParts()
distances.execute()
#doc = distance.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))