import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geojson

"""
Retrieves Open Space data as well as Boston Zip code data (important for future joins)

"""


def csv_to_dict(url):
    """
    Helper function to convert CSV file to json
    """
    file = urllib.request.urlopen(url).read().decode("utf-8")  # retrieve file from datamechanics.io
    dict_values = {}
    entries = file.split('\n')[:-1]

    keys = entries[0].split(',')  # retrieve column names for keys

    for row in entries:
        #print(row)
        row = row.split(',')
        #print(row[0], row[1])
        dict_values[row[0]] = row[1][:-2]

    return dict_values


class getOpenSpace(dml.Algorithm):
    contributor = 'janellc_rstiffel'
    reads = []
    writes = ['janellc_rstiffel.openSpace', 'janellc_rstiffel.zipCodes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')

        # Get Open Space data
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        gj = geojson.loads(response)
        r = gj['features']
        # Store in DB
        repo.dropCollection("openSpace")
        repo.createCollection("openSpace")
        repo['janellc_rstiffel.openSpace'].insert_many(r)
        repo['janellc_rstiffel.openSpace'].metadata({'complete':True})
        print(repo['janellc_rstiffel.openSpace'].metadata())

        # Get Zip Code data
        url = 'http://datamechanics.io/data/BostonZip.csv'
        r = csv_to_dict(url)
        # Store Zip Code in DB
        repo.dropCollection("zipCodes")
        repo.createCollection("zipCodes")
        repo['janellc_rstiffel.zipCodes'].insert(r)
        repo['janellc_rstiffel.zipCodes'].metadata({'complete':True})
        print(repo['janellc_rstiffel.zipCodes'].metadata())

        # Logout
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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/janellc_rstiffel/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/janellc_rstiffel/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        # Resource:
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        doc.add_namespace('zip', 'http://datamechanics.io/data/') 
        
        # Agent, entity, activity of open Space
        this_script = doc.agent('alg:janellc_rstiffel#getOpenSpace', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('bod:2868d370c55d4d458d4ae2224ef8cddd_7', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource2 = doc.entity('zip:BostonZip', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        
        get_openSpace = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_zipCodes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_openSpace, this_script)
        doc.wasAssociatedWith(get_zipCodes, this_script)

        doc.usage(get_openSpace, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )
        doc.usage(get_zipCodes, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        openSpace = doc.entity('dat:janellc_rstiffel#openSpace', {prov.model.PROV_LABEL:'Open Space', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(openSpace, this_script)
        doc.wasGeneratedBy(openSpace, get_openSpace, endTime)
        doc.wasDerivedFrom(openSpace, resource, get_openSpace, get_openSpace, get_openSpace)

        zipCodes = doc.entity('dat:janellc_rstiffel#zipCodes', {prov.model.PROV_LABEL:'Boston Zip Codes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(zipCodes, this_script)
        doc.wasGeneratedBy(zipCodes, get_zipCodes, endTime)
        doc.wasDerivedFrom(zipCodes, resource2, get_zipCodes, get_zipCodes, get_zipCodes)

        repo.logout()
                  
        return doc

# getOpenSpace.execute()
# doc = getOpenSpace.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
