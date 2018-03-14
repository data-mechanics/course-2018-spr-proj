import urllib.request
import json
from json import loads, dumps
from collections import OrderedDict
import dml
import prov.model
import datetime
import uuid
import pdb
import csv

def convert_roads_csv():
    url = 'http://datamechanics.io/data/alyu_sharontj/Roads%202013.csv'
    csvfile = urllib.request.urlopen(url).read().decode("utf-8")

    dict_values = []

    entries = csvfile.split('\n')
    dot_keys = entries[0].split(',')
    dot_keys[-1] = dot_keys[-1][:-1]

    keys = [key.replace('.', '_') for key in dot_keys]

    for row in entries[1:-1]:
        values = row.split(',')
        values[-1] = values[-1][:-1]
        dictionary = dict([(keys[i], values[i]) for i in range(len(keys))])
        dict_values.append(dictionary)

    return dict_values


class Roads(dml.Algorithm):
    contributor = ''
    reads = []
    writes = ['alyu_sharontj.Roads']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj', 'alyu_sharontj')

        dict_values = convert_roads_csv()

        repo.dropCollection("Roads")
        repo.createCollection("Roads")
        repo['alyu_sharontj.Roads'].insert_many(dict_values)

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
        repo.authenticate('alyu_sharontj', 'alyu_sharontj')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/alyu_sharontj/')
        # doc.add_namespace('hdv', 'https://dataverse.harvard.edu/dataset.xhtml')

        this_script = doc.agent('alg:alyu_sharontj#Roads',
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime,
            { prov.model.PROV_TYPE:'ont:Retrieval'})#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})

        road_input = doc.entity('bdp:Roads%202013',
            { 'prov:label':'Roads', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv', 'ont:Query':'?persistentId=doi:10.7910'})

        output = doc.entity('dat:alyu_sharontj.Roads', {prov.model.PROV_LABEL:'Roads', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAssociatedWith(this_run , this_script)
        doc.used(this_run, road_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, road_input, this_run, this_run, this_run)

        repo.logout()

        return doc

#
# Roads.execute()
# doc = Roads.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
