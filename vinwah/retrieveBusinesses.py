import urllib
import json
import dml
import prov.model
import datetime
import uuid
import http.client
from tqdm import tqdm


class retrieveBusinesses(dml.Algorithm):
    contributor = 'vinwah'
    reads = []
    writes = ['vinwah.businesses']

    @staticmethod
    def execute(trial = False):
        '''
        Retrieves data from businesses in Boton from Yelp
        '''
        
        startTime = datetime.datetime.now()

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('vinwah', 'vinwah')


        def get_businesses(zipcodes):
            key = ""
            with open('./auth.json') as f:
                key = json.load(f)['yelp_key']
            header = {'authorization': key}
            finalMultiset = []  
            for zipcode in tqdm(zipcodes):
                offset = 0
                result = []
                while(True):
                    params = {'location':'Boston, ' + zipcode, 'limit':50, 'offset':offset}
                    param_string = urllib.parse.urlencode(params)
                    conn = http.client.HTTPSConnection("api.yelp.com")
                    conn.request("GET", "https://api.yelp.com/v3/businesses/search?"+param_string, headers=header)
                    res = conn.getresponse()
                    data = res.read()
                    r = json.loads(data.decode("utf-8"))
                    result = result + r['businesses']
                    offset += 50
                    if((offset >= r['total']) or (offset >= 1000)):
                        break
                finalMultiset = finalMultiset + result
            return finalMultiset

        zipcodes = ['02118','02119','02120','02130','02134','02135',
                    '02445','02446','02447','02467','02108','02114',
                    '02115','02116','02215','02128','02129','02150',
                    '02151','02152','02126','02131','02132','02136',
                    '02109','02110','02111','02113','02121','02122',
                    '02124','02125','02127','02210']

        businessesMultiSet = get_businesses(zipcodes)

        businessesSet = { each['id'] : each for each in businessesMultiSet }.values()

        businesses = []
        for a_dict in businessesSet:
            if a_dict["location"]["city"] == 'Boston':
                businesses.append(a_dict)

        # remove old collection, and create a new one
        repo.dropCollection("businesses")
        repo.createCollection("businesses")

        # insert data into collection 
        repo['vinwah.businesses'].insert_many(businesses)

        repo.logout()

        endTime = datetime.datetime.now()

        print('retrieve businesses finished at:', endTime)

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
        repo.authenticate('vinwah', 'vinwah')


#https://api.yelp.com/v3/businesses/search?location=Boston,MA&limit=50&offset=0
        # set up namespace
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/vinwah#') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/vinwah#') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('ylp', 'https://api.yelp.com/')

        # Set up agent, entities, activity
        this_script = doc.agent('alg:retrieveBusinesses', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('ylp:v3/businesses/search?location=Boston', {'prov:label':'Businesses in Boston', prov.model.PROV_TYPE:'ont:DataResource'})
        get = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        businesses = doc.entity('dat:businesses', {prov.model.PROV_LABEL:'Businesses in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        
        # establish relationships 
        doc.wasAssociatedWith(get, this_script)
        doc.usage(get, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )
        doc.wasAttributedTo(businesses, this_script)
        doc.wasGeneratedBy(businesses, get, endTime)
        doc.wasDerivedFrom(businesses, resource, get, get, get)

        repo.logout()
                  
        return doc










