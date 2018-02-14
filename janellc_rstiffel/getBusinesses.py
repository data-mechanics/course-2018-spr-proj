import urllib.request
import json
import dml
import prov.model
import datetime
import uuid



# method to convert csv into json, returns dictionary
def csv_to_json(url):
    file = urllib.request.urlopen(url).read().decode("utf-8")  # retrieve file from datamechanics.io
    dict_values = []
    entries = file.split('\n')

    # print(entries[0])
    keys = entries[0].split(',')  # retrieve column names for keys

    for row in entries[1:-1]:
        values = row.split(',')
        values[-1] = values[-1][:-1]
        dictionary = dict([(keys[i], values[i]) for i in range(len(keys))])
        dict_values.append(dictionary)
    return dict_values


class getBusinesses (dml.Algorithm):
    contributor = 'janellc_rstiffel'
    reads = []
    writes = ['janellc_rstiffel.fitBusinesses']


    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')

        # get csv files from datamechanics.io -- downloaded originally from boston city clerk
        url = 'http://datamechanics.io/data/DBABoston.csv'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        values = csv_to_json(url)

        repo.dropCollection("fitBusinesses")
        repo.createCollection("fitBusinesses")
        repo['janellc_rstiffel.fitBusinesses'].insert_many(values)
        repo['janellc_rstiffel.fitBusinesses'].metadata({'complete':True})
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

        resource = doc.entity('dba: ', {'prov:label':'Doing Business As - City Clerk', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_fitBusinesses = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_fitBusinesses, this_script)
        
        doc.usage(get_fitBusinesses, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )


        fitBusinesses = doc.entity('dat:janellc_rstiffel#fitBusinesses', {prov.model.PROV_LABEL:'Fit Businesses', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fitBusinesses, this_script)
        doc.wasGeneratedBy(fitBusinesses, get_fitBusinesses, endTime)
        doc.wasDerivedFrom(fitBusinesses, resource, get_fitBusinesses, get_fitBusinesses, get_fitBusinesses)


        repo.logout()
                  
        return doc

#Delete for submission
#getBusinesses.execute()
#doc = getBusinesses.provenance()
#print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
