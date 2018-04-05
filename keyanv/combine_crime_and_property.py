import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class combine_crime_and_property(dml.Algorithm):
    contributor = 'keyanv'
    reads = ['keyanv.crimes', 'keyanv.properties']
    writes = ['keyanv.crime_and_properties']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')

        crimes = repo['keyanv.crimes']
        properties = repo['keyanv.properties']
        projected_crimes = []
        projected_properties = []
        street_info = []

        # project the relevant information from the data
        for crime in crimes.find():
            projected_crimes.append({"Street":crime["STREET"], "Offense":crime["OFFENSE_CODE_GROUP"], "Desc":crime["OFFENSE_DESCRIPTION"]})
        for prop in properties.find():
            projected_properties.append({"Street":prop["ST_NAME"], "Cost":prop["AV_TOTAL"], "Tax":prop["GROSS_TAX"]})

        # perform a union on the two projected sets (only up to 1000 to ensure a reasonable runtime)
        for i in range(100):
            street_info.append({"Street": projected_properties[i]["Street"], "Cost": projected_properties[i]["Cost"], "Tax": projected_properties[i]["Tax"], "Offense":projected_crimes[i]["Offense"],"Desc":projected_crimes[i]["Desc"]})
            
                
        repo.dropCollection("crime_and_properties")
        repo.createCollection("crime_and_properties")
        repo['keyanv.crime_and_properties'].insert_many(street_info)
        repo['keyanv.crime_and_properties'].metadata({'complete':True})
        print(repo['keyanv.crime_and_properties'].metadata())

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
        repo.authenticate('keyanv', 'keyanv')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:keyanv#combine_crime_and_property', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        crime_resource = doc.entity('bdp:12c/b38/12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crime Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        property_resource = doc.entity('bdp:062/fc6/062fc6fa-b5ff-4270-86cf-202225e40858', {'prov:label':'Property Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        combine_crime_and_property = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, { prov.model.PROV_LABEL: "Combination of Crime and Property Data", prov.model.PROV_TYPE: 'ont:Computation'})
        doc.wasAssociatedWith(combine_crime_and_property, this_script)
        doc.usage(combine_crime_and_property, crime_resource, startTime)
        doc.usage(combine_crime_and_property, property_resource, startTime)
        properties = doc.entity('dat:keyanv#crime_and_properties', {prov.model.PROV_LABEL:'Crime and Property Combination', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(properties, this_script)
        doc.wasGeneratedBy(properties, combine_crime_and_property, endTime)
        doc.wasDerivedFrom(properties, crime_resource, combine_crime_and_property, combine_crime_and_property, combine_crime_and_property)
        doc.wasDerivedFrom(properties, property_resource, combine_crime_and_property, combine_crime_and_property, combine_crime_and_property)

        repo.logout()
                  
        return doc

combine_crime_and_property.execute()
doc = combine_crime_and_property.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof