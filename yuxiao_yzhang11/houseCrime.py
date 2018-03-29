import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv

def csvConvert():
    url = "http://datamechanics.io/data/boston_Zip_Zhvi_AllHomes.csv"

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




class houseCrime(dml.Algorithm):
    contributor = 'yuxiao_yzhang11'
    reads = []
    writes = ['yuxiao_yzhang11.houseCrime']

    @staticmethod
    def execute(trial=True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yuxiao_yzhang11', 'yuxiao_yzhang11')


        house = repo['yuxiao_yzhang11.houseValue']


        match = {
            "$and": [{"State": "MA"}, {"Metro": "Boston"}]
        }

        group = {
            '_id': {"zip:":"$RegionName", 'price': '$2015-02'}
        }

        houseZip = house.aggregate([
            { '$match': match },
            { '$group': group }
        ])

        # for i in houseZip:
        #     i['_id'][0] = "0" + i['_id'][0]

        repo.dropCollection("houseZip")
        repo.createCollection("houseZip")
        repo['yuxiao_yzhang11.houseZip'].insert_many(houseZip)

        # repo.dropCollection("crimeZip")
        # repo.createCollection("crimeZip")
        #
        #
        # repo.dropCollection("houseCrime")
        # repo.createCollection("houseCrime")
        # repo['yuxiao_yzhang11.houseCrime'].insert_many(dict_values)

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
        repo.authenticate('yuxiao_yzhang11', 'yuxiao_yzhang11')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/')


        this_script = doc.agent('alg:yuxiao_yzhang11#houseCrime',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dat:boston_Zip_Zhvi_AllHomes',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',})

        output =  doc.entity('dat:yuxiao_yzhang11.houseCrime', {prov.model.PROV_LABEL:'houseCrime', prov.model.PROV_TYPE:'ont:DataSet'})

        # get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        #
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, resource, this_run, this_run, this_run)

        repo.logout()

        return doc


houseCrime.execute()
doc = houseCrime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
