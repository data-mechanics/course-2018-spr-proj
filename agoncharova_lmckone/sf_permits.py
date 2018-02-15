
import urllib.request as ur
import json
import dml
import prov.model
import datetime
import uuid

class sf_permits(dml.Algorithm):
    contributor = 'agoncharova_lmckone'
    reads = []
    writes = ['agoncharova_lmckone.sf_permits']

    def get_and_insert_by_paging():
        '''
        Dynamically gets data via setting the page and the offset.
        '''
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

        repo.dropCollection('sf_permits')
        repo.createCollection('sf_permits')
        sf_coll = repo['agoncharova_lmckone.sf_permits']
        sf_coll.create_index('Permit Number')

        url_str = 'https://data.sfgov.org/resource/vy2q-29it.json?$limit=5000&$offset={0}'
        number_of_pages = 210 + 1# = 1050000 / 5000
        for offset in range(0, number_of_pages):
            url = url_str.format(offset)
            print(url)
            response = ur.urlopen(url)
            data = json.load(response)
            print("Fetched data at offset " + str(offset))
            sf_coll.insert_many(data)
            print("Inserted " + str(len(data)) + " data points from offset " + str(offset))
        sf_coll.metadata({'complete':True})
        
        # get count of data in sf_coll
        print("Inserted " + str(sf_coll.count()) + " of data points for SF permits into collection sf_permits" )            
        repo.logout() 


    @staticmethod
    def execute(trial = False):
        '''Retrieve SF Permit Data'''
        startTime = datetime.datetime.now()        
        
        endTime = datetime.datetime.now()
        sf_permits.get_and_insert_by_paging()

        return { "start": startTime, "end": endTime }

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
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('sfdp', 'https://datasf.org/opendata/')

        this_script = doc.agent('alg:agoncharova_lmckone#sf_permits', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('sfdp:vy2q-29it', {'prov:label':'San Francisco Data Portal Permits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_sf_permits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_sf_permits, this_script)

        doc.usage(get_sf_permits, resource, startTime, None, { prov.model.PROV_TYPE:'ont:Retrieval' })

        sf_permits = doc.entity('dat:agoncharova_lmckone#sf_permits', {prov.model.PROV_LABEL:'San Francisco Permits', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(sf_permits, this_script)
        doc.wasGeneratedBy(sf_permits, get_sf_permits, endTime)
        doc.wasDerivedFrom(sf_permits, resource, get_sf_permits, get_sf_permits, get_sf_permits)
        repo.logout()

        return doc

sf_permits.execute()
sf_permits.provenance()
