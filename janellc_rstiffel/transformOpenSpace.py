import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geojson

"""
Finds average number of acres of open space for each Boston district.
- Selection for parks, playgrounds, and athletic fields
- projection for key (district), value (total acres) pairs
- Simple calculation for avg.

- Joins with zip code based on District Name
    - Projects at the same time so key is zip code, value is #acres and district name.

"""


class transformOpenSpace(dml.Algorithm):
    contributor = 'janellc_rstiffel'
    reads = ['janellc_rstiffel.openSpace', 'janellc_rstiffel.zipCodes']
    writes = []

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')

        # Get Open Space data
        openSpaceData = repo.janellc_rstiffel.openSpace.find()
        districts = {}
        # Filter for typecode, project key value pairs.
        for row in openSpaceData:
            if (row['properties']['TYPECODE'] == None or row['properties']['ACRES'] == None):
                continue
            if (row['properties']['TYPECODE'] == 3): #TYPECODE 3 means parks, playgrouns, or athletic fields
                district = row['properties']['DISTRICT']
                acres = row['properties']['ACRES']
                
                if district not in districts:                           # Write to dictionary
                    districts[district] = {'ACRES':acres, 'COUNT':1} # Key is the district, value is the number of acres.
                else:
                    districts[district]['ACRES'] += acres
                    districts[district]['COUNT'] += 1
        # Calculating averages here
        for key,value in districts.items():
            districts[key] = value['ACRES'] / value['COUNT']
        #print(districts)

        # Get Zip Code data
        zipCodeData = repo.janellc_rstiffel.zipCodes.find()[0]
        zip_districts = {}
        # Combine the data sets based on District names.
        for key,value in districts.items():
            zip_districts[zipCodeData[key]] = {'District':key, 'Acres':value}
        


        # Store in districtAvgAcres.json
        with open("./transformed_datasets/districtAvgAcres.json", 'w') as outfile:
            json.dump(zip_districts, outfile)


        # Store in DB
        repo.dropCollection("districtAvgAcres")
        repo.createCollection("districtAvgAcres")

        for key,value in zip_districts.items():
            #print({key:value})
            repo['janellc_rstiffel.districtAvgAcres'].insert({key:value})
        repo['janellc_rstiffel.districtAvgAcres'].metadata({'complete':True})
        print(repo['janellc_rstiffel.districtAvgAcres'].metadata())

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

        
        # Agent, entity, activity
        this_script = doc.agent('alg:janellc_rstiffel#transformOpenSpace', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        # Two resources: openSpace and zipCodes
        resource1 = doc.entity('dat:janellc_rstiffel#openSpace', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource2 = doc.entity('dat:janellc_rstiffel#zipCodes', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        transform_openSpace = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transform_openSpace, this_script)

        doc.usage(transform_openSpace, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )
        doc.usage(transform_openSpace, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )

        districtAvgAcres = doc.entity('dat:janellc_rstiffel#districtAvgAcres', {prov.model.PROV_LABEL:'District Avg Results', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(districtAvgAcres, this_script)
        doc.wasGeneratedBy(districtAvgAcres, transform_openSpace, endTime)
        doc.wasDerivedFrom(districtAvgAcres, resource1, transform_openSpace, transform_openSpace, transform_openSpace)
        doc.wasDerivedFrom(districtAvgAcres, resource2, transform_openSpace, transform_openSpace, transform_openSpace)

        repo.logout()
                  
        return doc

transformOpenSpace.execute()
# doc = transformOpenSpace.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
