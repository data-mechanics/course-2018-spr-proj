import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class employeeearnings(dml.Algorithm):
    contributor = 'colinstu'
    reads = []
    writes = ['colinstu.lost', 'colinstu.found']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')

        url = 'https://data.boston.gov/export/836/8bd/8368bd3d-3633-4927-8355-2a2f9811ab4f.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        response = response.replace("]", "")
        response = response + "]"  # fixes typo in dataset
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("employeeearnings")
        repo.createCollection("employeeearnings")
        repo['colinstu.employeeearnings'].insert_many(r)
        repo['colinstu.employeeearnings'].metadata({'complete': True})
        print(repo['colinstu.employeeearnings'].metadata())

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

        this_script = doc.agent('alg:colinstu#employeeearnings',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label': 'Active Food Establishment Licenses',
                                                prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_earnings = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_earnings, this_script)

        doc.usage(get_earnings, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'  # TODO: fix query
                   }
                  )

        earnings = doc.entity('dat:colinstu#employeeearnings', {prov.model.PROV_LABEL: 'Employee Earnings Report',
                                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(earnings, this_script)
        doc.wasGeneratedBy(earnings, get_earnings, endTime)
        doc.wasDerivedFrom(earnings, resource, get_earnings, get_earnings, get_earnings)

        repo.logout()

        return doc


employeeearnings.execute()
doc = employeeearnings.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
