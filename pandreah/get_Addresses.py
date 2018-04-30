import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
'''In this script I am gathering data on all existing Addresses in Boston and storing it in a MongoDB collection called propertyA.
This scrip will also produce a provenance document when its provenance() method is called.
Format taken from example file in github.com/Data-Mechanics '''

class get_Addresses(dml.Algorithm):
    contributor = 'pandreah'
    reads = []
    writes = ['pandreah.propertyA']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pandreah', 'pandreah')
        print("did this")
        
        if trial == True:
            url = 'http://datamechanics.io/data/sampleLSAM.json'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            repo.dropCollection("propertyA")
            repo.createCollection("propertyA")
            repo['pandreah.propertyA'].insert_many(r) 
        
        else: 
            url = 'http://datamechanics.io/data/LSALAMMI1.json' #This is where the data is coming from
#           print("hit url1")
            response = urllib.request.urlopen(url).read().decode("utf-8")
#           print("getting json")
            r = json.loads(response)
#           print("got response 0")
            s = json.dumps(r, sort_keys=True, indent=2)
            url1 = 'http://datamechanics.io/data/LSALAMMI2.json'
            response1 = urllib.request.urlopen(url).read().decode("utf-8")
            r1 = json.loads(response1)
#           print("got response 1")
            s1 = json.dumps(r, sort_keys=True, indent=2)
            url2 = 'http://datamechanics.io/data/LSALAMMI1.json'
            response2 = urllib.request.urlopen(url).read().decode("utf-8")
            r2 = json.loads(response2)
#           print("got response 2")
            s2 = json.dumps(r, sort_keys=True, indent=2)
            url3 = 'http://datamechanics.io/data/LSALAMMI1.json'
            response3 = urllib.request.urlopen(url).read().decode("utf-8")
            r3 = json.loads(response3)
#           print("got response 3")
            s3 = json.dumps(r, sort_keys=True, indent=2)
            repo.dropCollection("propertyA")
            repo.createCollection("propertyA")
            repo['pandreah.propertyA'].insert_many(r)                         #This is where the data is being stored
            repo['pandreah.propertyA'].insert_many(r1)
            repo['pandreah.propertyA'].insert_many(r2)
            repo['pandreah.propertyA'].insert_many(r3)
        repo['pandreah.propertyA'].metadata({'complete':True})
        print(repo['pandreah.propertyA'].metadata())

        



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
        repo.authenticate('pandreah', 'pandreah')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:pandreah#propertyA', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_Addresses = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Addresses, this_script)
        doc.usage(get_Addresses, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Property+Addresses&$select=type,OBJECTID,id, X, Y, SAM_ADDRESS_ID, RELATIONSHIP_TYPE, BUILDING_ID, FULL_ADDRESS, STREET_NUMBER, IS_RANGE, RANGE_FROM, RANGE_TO, UNIT, FULL_STREET_NAME, STREET_ID, STREET_PREFIX, STREET_BODY, STREET_SUFFIX_ABBR, STREET_FULL_SUFFIX, STREET_SUFFIX_DIR, STREET_NUMBER_SORT, MAILING_NEIGHBORHOOD, ZIP_CODE, X_COORD, Y_COORD, SAM_STREET_ID, PRECINCT_WARD, PARCEL'
                  }
                  )

        propertyA = doc.entity('dat:pandreah#propertyA', {prov.model.PROV_LABEL:'Property Addresses', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(propertyA, this_script)
        doc.wasGeneratedBy(propertyA, get_Addresses, endTime)
        doc.wasDerivedFrom(propertyA, resource, get_Addresses, get_Addresses, get_Addresses)

        repo.logout()
                  
        return doc

if __name__ == "__main__":
    get_Addresses.execute()
    doc = get_Addresses.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))

