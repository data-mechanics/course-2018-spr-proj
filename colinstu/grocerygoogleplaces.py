import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

googlekey = ''
class grocerygoogleplaces(dml.Algorithm):
    contributor = 'colinstu'
    reads = []
    writes = ['colinstu.grocerygoogleplaces']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')

        json_data = open("/Users/colinstuart/Dropbox/CS591-Data-Mechanics/course-2018-spr-proj/auth.json")
        jdata = json.load(json_data)
        url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query=grocery+in+boston&key=' + jdata['googlekey']
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("grocerygoogleplaces")
        repo.createCollection("grocerygoogleplaces")
        repo['colinstu.grocerygoogleplaces'].insert_many(r['results'])
        repo['colinstu.grocerygoogleplaces'].metadata({'complete': True})
        print(repo['colinstu.grocerygoogleplaces'].metadata())

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
        repo.authenticate('colinstu', 'colinstu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://maps.googleapis.com/maps/api/place/textsearch/')

        this_script = doc.agent('alg:colinstu#grocerygoogleplaces',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label': 'Google Places Query for Grocery in Boston',
                                                prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_grocery = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_grocery, this_script)

        doc.usage(get_grocery, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': 'query=grocery+in+boston&key='
                   }
                  )

        grocery = doc.entity('dat:colinstu#grocerygoogleplaces', {prov.model.PROV_LABEL: 'Grocery Stores in Boston (Google Places)',
                                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(grocery, this_script)
        doc.wasGeneratedBy(grocery, get_grocery, endTime)
        doc.wasDerivedFrom(grocery, resource, get_grocery, get_grocery, get_grocery)

        repo.logout()

        return doc


grocerygoogleplaces.execute()
doc = grocerygoogleplaces.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
