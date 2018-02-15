import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


def map(f, R):
    return [t for (k, v) in R for t in f(k, v)]


def reduce(f, R):
    keys = {k for (k, v) in R}
    return [f(k1, [v for (k2, v) in R if k1 == k2]) for k1 in keys]

class combineincomeneighborhood(dml.Algorithm):
    contributor = 'colinstu'
    reads = ['colinstu.HUDincome','colinstu.employeeearnings','colinstu.neighborhood']
    writes = ['colinstu.combineincomeneighborhood']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')

        income = repo['colinstu.HUDincome']
        r = json.loads(income)
        s = json.dumps(r, sort_keys=True, indent=2)



        repo.dropCollection("combineincomeneighborhood")
        repo.createCollection("combineincomeneighborhood")
        repo['colinstu.combineincomeneighborhood'].insert_many(r)
        repo['colinstu.combineincomeneighborhood'].metadata({'complete': True})
        print(repo['colinstu.combineincomeneighborhood'].metadata())

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
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset/active-food-establishment-licenses')

        this_script = doc.agent('alg:colinstu#combineincomeneighborhood',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label': 'Combine Income and Neighborhood Data',
                                                prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_foodlic = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_foodlic, this_script)

        doc.usage(get_foodlic, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type='  # TODO: fix query
                   }
                  )

        foodlic = doc.entity('dat:colinstu#foodlicense', {prov.model.PROV_LABEL: 'Active Food Establishment Licenses',
                                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(foodlic, this_script)
        doc.wasGeneratedBy(foodlic, get_foodlic, endTime)
        doc.wasDerivedFrom(foodlic, resource, get_foodlic, get_foodlic, get_foodlic)

        repo.logout()

        return doc


foodlicense.execute()
doc = foodlicense.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
