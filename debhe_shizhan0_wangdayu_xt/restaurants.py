import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io

'''
This is the file that will get the data from the dataset 
parse them and then save it into our database
'''
class restaurants(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = []
    writes = ['debhe_shizhan0_wangdayu_xt.restaurants']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')

        # Going to get the resource from the following link
        url = 'https://data.boston.gov/dataset/5e4182e3-ba1e-4511-88f8-08a70383e1b6/resource/f1e13724-284d-478c-b8bc-ef042aa5b70b/download/licenses.csv'
        
        # parse the data to json file
        response = urllib.request.urlopen(url)
        cr = csv.reader(io.StringIO(response.read().decode('utf-8')), delimiter = ',')
        rs = []
        i = 0
        for row in cr:
            if(i != 0):
                dic = {}
                dic['restaurantName'] = row[0]
                dic['Address'] = row[2]
                dic['city'] = row[3]
                dic['state'] = row[4]
                dic['Descript'] = row[8]
                x,y = row[12].split(",")
                dic['Y'] = x[1:]
                dic['X'] = y[:-1]
                rs.append(dic)
            i = i + 1

        # Connect to database and store the data to the database
        repo.dropCollection("restaurants")
        repo.createCollection("restaurants")
        repo['debhe_shizhan0_wangdayu_xt.restaurants'].insert_many(rs)
        repo['debhe_shizhan0_wangdayu_xt.restaurants'].metadata({'complete':True})
        print(repo['debhe_shizhan0_wangdayu_xt.restaurants'].metadata())

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):


    # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dbg', 'https://data.boston.gov/dataset/5e4182e3-ba1e-4511-88f8-08a70383e1b6/resource/')

        this_script = doc.agent('alg:debhe_shizhan0_wangdayu_xt#restaurants', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dbg:f1e13724-284d-478c-b8bc-ef042aa5b70b/download/licenses', {'prov:label':'Restaurants Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_restaurants = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        #get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_restaurants, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_restaurants, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        restaurants = doc.entity('dat:debhe_shizhan0_wangdayu_xt#restaurants', {prov.model.PROV_LABEL:'pulic School', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(restaurants, this_script)
        doc.wasGeneratedBy(restaurants, get_restaurants, endTime)
        doc.wasDerivedFrom(restaurants, resource, get_restaurants, get_restaurants, get_restaurants)



        repo.logout()
                  
        return doc

#restaurants.execute()
#doc = restaurants.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))