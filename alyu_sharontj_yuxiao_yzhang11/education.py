import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class education(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = []
    writes = ['alyu_sharontj_yuxiao_yzhang11.education']

    @staticmethod
    def execute(trial=True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        url = 'http://datamechanics.io/data/alyu_sharontj_yuxiao_yzhang11/Colleges_and_Universities.geojson'

        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("education") #name of the data link: e.g. station_links
        repo.createCollection("education")
        repo['alyu_sharontj_yuxiao_yzhang11.education'].insert_many(r['features'])    #insert data into database?
        repo['alyu_sharontj_yuxiao_yzhang11.education'].metadata({'complete': True})


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
        #http://datamechanics.io/data/alyu_sharontj_yuxiao_yzhang11/Colleges_and_Universities.geojson
        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#education',
                                {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],
                                 'ont:Extension': 'py'})#change to file name
        resource = doc.entity('bdp:Colleges_and_Universities',
                              {'prov:label': 'College_University',
                               prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'geojson'})
        get_TS = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)  #TS= traffic Signals

        doc.wasAssociatedWith(get_TS, this_script)
        doc.usage(get_TS, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'}
                  )


        TS = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#education',
                        {prov.model.PROV_LABEL: 'education',
                         prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(TS, this_script)
        doc.wasGeneratedBy(TS, get_TS, endTime)
        doc.wasDerivedFrom(TS, resource, get_TS, get_TS, get_TS)


        repo.logout()

        return doc

# education.execute()
# doc = education.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
