import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class hospital(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = []
    writes = ['alyu_sharontj_yuxiao_yzhang11.hospital']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        url = 'http://datamechanics.io/data/alyu_sharontj_yuxiao_yzhang11/hospitalsgeo.json'

        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        # print(r)
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("hospital") #name of the data link: e.g. station_links
        repo.createCollection("hospital")
        repo['alyu_sharontj_yuxiao_yzhang11.hospital'].insert_many(r)    #insert data into database?
        repo['alyu_sharontj_yuxiao_yzhang11.hospital'].metadata({'complete':True})
        # print(repo['alyu_sharontj_yuxiao_yzhang11.hospital'].metadata())


        # repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

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
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/alyu_sharontj_yuxiao_yzhang11/')
        # http://datamechanics.io/data/alyu_sharontj_yuxiao_yzhang11/hospitalsgeo.json

        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#hospital',
                                {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],
                                 'ont:Extension':'py'})#change to file name
        resource = doc.entity('bdp:hospitalsgeo',
                              {'prov:label':'hospital',
                               prov.model.PROV_TYPE:'ont:DataResource',
                               'ont:Extension':'json'})
        get_TS = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)  #TS= traffic Signals

        doc.wasAssociatedWith(get_TS, this_script)
        doc.usage(get_TS, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        TS = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#hospital',
                        {prov.model.PROV_LABEL:'hospital',
                         prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(TS, this_script)
        doc.wasGeneratedBy(TS, get_TS, endTime)
        doc.wasDerivedFrom(TS, resource, get_TS, get_TS, get_TS)


        repo.logout()

        return doc

# hospital.execute()
# doc = hospital.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
