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


class Income (dml.Algorithm):
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

        url = 'http://datamechanics.io/data/census_tracts_list2.csv'
        values = csv_to_json(url)
        print(values[8])
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bostonTracts")
        repo.createCollection("bostonTracts")
        repo['janellc_rstiffel.bostonTracts'].insert_many(values)
        repo['janellc_rstiffel.bostonTracts'].metadata({'complete':True})
        print(repo['janellc_rstiffel.bostonTracts'].metadata())

        url = 'http://datamechanics.io/data/ACS%2012%20YR%20INCOME%20by%20TRACT.csv'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        values = csv_to_json(url)
        print(values[3])
        repo.dropCollection("Income")
        repo.createCollection("Income")
        repo['janellc_rstiffel.Income'].insert_many(values)

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

        doc.add_namespace('ffc', 'https://factfinder.census.gov/faces/tableservices/jsf/pages/') # data set form census fact finder
        doc.add_namespace('cen', 'https://www2.census.gov/geo/docs/maps-data/data/gazetteer/')

        this_script = doc.agent('alg:janellc_rstiffel#getIncome', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        tractResource = doc.entity('ffc:census_tracts_list_25.txt', {'prov:label':'Tract List', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        incomeResource = doc.entity('cen:massgis-data-datalayers-2010-us-census', {'prov:label':'Income Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

        get_bostonTracts = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_Income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_bostonTracts, this_script)
        doc.wasAssociatedWith(get_Income, this_script)

        doc.usage(get_bostonTracts, tractResource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
'ont:Query':''
                  }
                  )
        doc.usage(get_Income, incomeResource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

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

# TAKE THIS OUT IN SUBMISSION
Income.execute()
doc = Income.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
