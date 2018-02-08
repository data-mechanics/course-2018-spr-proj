import urllib.request
import json
import dml
import prov.model
import datetime
import uuid



def csv_to_json(url):
    file = urllib.request.urlopen(url).read().decode("utf-8")  # retrieve file from datamechanics.io
    dict_values = []
    entries = file.split('\n')

    # print(entries[0])
    keys = entries[0].split(',')  # retrieve column names for keys
    print(keys)

    for row in entries[1:-1]:
        values = row.split(',')
        values[-1] = values[-1][:-1]
        dictionary = dict([(keys[i], values[i]) for i in range(len(keys))])
        dict_values.append(dictionary)

    return dict_values


class Businesses (dml.Algorithm):
    contributor = 'janellc_rstiffel'
    reads = []
    writes = ['janellc_rstiffel.medianIncome', 'janellc_rstiffel.bostonTracts']


    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')

        # get csv files associated with income data
        #


        url = 'http://datamechanics.io/data/DBABoston.csv'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        values = csv_to_json(url)
        print(values[3])
        repo.dropCollection("fitBusinesses")
        repo.createCollection("fitBusinesses")
        repo['janellc_rstiffel.fitBusinesses'].insert_many(values)
        print(repo['janellc_rstiffel.fitBusinesses'].metadata())

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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        doc.add_namespace('dba', 'https://www.cityofboston.gov/cityclerk/dbasearch/') # data set form census fact finder

        this_script = doc.agent('alg:janellc_rstiffel#getBusinesses',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dba: ', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_fitBusinesses = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_fitBusinesses, this_script)


        # lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(lost, this_script)
        # doc.wasGeneratedBy(lost, get_lost, endTime)
        # doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)
        #
        # found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(found, this_script)
        # doc.wasGeneratedBy(found, get_found, endTime)
        # doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

# Delete for submission
Businesses.execute()
doc = Businesses.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
