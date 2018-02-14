import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geojson

class getNeighborhoods(dml.Algorithm):
    contributor = 'janellc_rstiffel'
    reads = []
    writes = ['janellc_rstiffel.Neighborhoods']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/a6488cfd737b4955bf55b0342c74575b_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        gj = geojson.loads(response)
        r = gj['features']
        # s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Neighborhoods")
        repo.createCollection("Neighborhoods")
        repo['janellc_rstiffel.Neighborhoods'].insert_many(r)
        repo['janellc_rstiffel.Neighborhoods'].metadata({'complete':True})
        print(repo['janellc_rstiffel.Neighborhoods'].metadata())


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
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets')

        this_script = doc.agent('alg:janellc_rstiffel#getNeighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:a6488cfd737b4955bf55b0342c74575b_0', {'prov:label':'Planning Districts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_Neighborhoods = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Neighborhoods, this_script)


        doc.usage(get_Neighborhoods, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        Neighborhoods = doc.entity('dat:janellc_rstiffel#Neighborhoods', {prov.model.PROV_LABEL:'Neighborhoods', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Neighborhoods, this_script)
        doc.wasGeneratedBy(Neighborhoods, get_Neighborhoods, endTime)
        doc.wasDerivedFrom(Neighborhoods, resource, get_Neighborhoods, get_Neighborhoods, get_Neighborhoods)

        repo.logout()
                  
        return doc

#getNeighborhoods.execute()
#doc = getNeighborhoods.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
