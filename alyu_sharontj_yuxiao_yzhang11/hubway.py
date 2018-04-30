import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv

def csvConvert():
    url = "http://datamechanics.io/data/hubway_stations.csv"

    csvfile = urllib.request.urlopen(url).read().decode("utf-8")

    dict_values = []

    entries = csvfile.split('\n')
    dot_keys = entries[0].split(',')
    dot_keys[-1] = dot_keys[-1][:-1]

    # keys = [key.replace('.', '_') for key in dot_keys]

    for row in entries[1:-1]:
        values = row.split(',')
        values[-1] = values[-1][:-1]
        dictionary = dict([(dot_keys[i], values[i]) for i in range(len(dot_keys))])
        dict_values.append(dictionary)

    return dict_values



class hubway(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = []
    writes = ['alyu_sharontj_yuxiao_yzhang11.hubway']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        dict_values = csvConvert()

        # dict_values = csvConvert()

        repo.dropCollection("hubway")
        repo.createCollection("hubway")
        repo['alyu_sharontj_yuxiao_yzhang11.hubway'].insert_many(dict_values)


        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data')


        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#hubway',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('bdp: hubway_stations',
                              {'prov:label': 'Boston hubway', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})


        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        output =  doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#hubway',
                             {prov.model.PROV_LABEL:'hubway',
                              prov.model.PROV_TYPE:'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, resource, this_run, this_run, this_run)

        repo.logout()

        return doc


# hubway.execute()
# doc = hubway.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
