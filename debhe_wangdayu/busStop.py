import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


'''
We are going to get the bus Stop data online, parse them and then save it into the database
'''
class busStop(dml.Algorithm):
    contributor = 'debhe_wangdayu'
    reads = []
    writes = ["debhe_wangdayu.busStop"]

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_wangdayu', 'debhe_wangdayu')

        # Get the data from the following website, parse them as geojson fill and save to data base
        url = 'http://datamechanics.io/data/wuhaoyu_yiran123/MBTA_Bus_Stops.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        s = json.dumps(r, sort_keys=True, indent=2)
        # Create the busStop collection and save the data in this collection
        repo.dropCollection("busStop")
        repo.createCollection("busStop")
        repo['debhe_wangdayu.busStop'].insert_many(r)
        repo['debhe_wangdayu.busStop'].metadata({'complete':True})
        print(repo['debhe_wangdayu.busStop'].metadata())

        repo.logout()
        
        # The geojson data is really hard to use, So we are going to parse it to normal json records
        # Set up the connection to the database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_wangdayu', 'debhe_wangdayu')

        # this is the helper function to get the data records from the tables
        def getCollection(db):
            temp = []
            for i in repo['debhe_wangdayu.' + db].find():
                temp.append(i)
            return temp

        # Get the records and save it into busStop
        busStop = getCollection('busStop')

        # We will save the parsed data into busStop2
        # For each record, we will get the information out of the geojson and
        # save it as normal json file
        busStop2 = []
        for row in busStop:
            dic = {}
            dic['stopName'] = row['properties']['STOP_NAME']
            dic['stopID'] = row['properties']['STOP_ID']
            dic['town'] = row['properties']['TOWN']
            dic['X'] = row['geometry']['coordinates'][0]
            dic['Y'] = row['geometry']['coordinates'][1]
            busStop2.append(dic)

        # save the data back to the collection
        repo.dropCollection("debhe_wangdayu.busStop")
        repo.createCollection("debhe_wangdayu.busStop")

        repo['debhe_wangdayu.busStop'].insert_many(busStop2)
        repo['debhe_wangdayu.busStop'].metadata({'complete':True})
        print(repo['debhe_wangdayu.busStop'].metadata())
        
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
        repo.authenticate('debhe_wangdayu', 'debhe_wangdayu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dmo', 'http://datamechanics.io/data/wuhaoyu_yiran123/')

        this_script = doc.agent('alg:debhe_wangdayu#busStop', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dmo:MBTA_Bus_Stops', {'prov:label':'busStop', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_busStop = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        #get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_busStop, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_busStop, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        busStop = doc.entity('dat:debhe_wangdayu#busStop', {prov.model.PROV_LABEL:'Bus Stop Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(busStop, this_script)
        doc.wasGeneratedBy(busStop, get_busStop, endTime)
        doc.wasDerivedFrom(busStop, resource, get_busStop, get_busStop, get_busStop)

        repo.logout()
                  
        return doc

#busStop.execute()
#doc = busStop.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
