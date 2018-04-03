import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv




# def csvConvert():
#
#     url = "https://data.boston.gov/dataset/ac9e373a-1303-4563-b28e-29070229fdfe/resource/76771c63-2d95-4095-bf3d-5f22844350d8/download/2013-bostonfireincidentopendata.csv"
#     csvfile = urllib.request.urlopen(url).read().decode("utf-8")
#
#     dict_values = []
#
#     entries = csvfile.split('\n')
#     dot_keys = entries[0].split(',')
#     # dot_keys[-1] = dot_keys[-1][:-1]
#
#     keys = [key.replace('\"', '') for key in dot_keys]
#
#     for row in entries[1:-1]:
#         values = row.split(',')
#         dictionary = dict([(keys[i], values[i].replace('\"','')) for i in range(len(keys))])
#         dict_values.append(dictionary)
#
#     return dict_values



class fire(dml.Algorithm):
    contributor = 'yuxiao_yzhang11'
    reads = []
    writes = ['yuxiao_yzhang11.fire']

    @staticmethod
    def execute(trial=True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yuxiao_yzhang11', 'yuxiao_yzhang11')

        url2013 = 'http://datamechanics.io/data/2013fireincident_anabos2.json'
        response2013 = urllib.request.urlopen(url2013).read().decode("utf-8")
        fire2013 = json.loads(response2013)

        url2014 = 'http://datamechanics.io/data/2014fireincident_anabos2.json'
        response2014 = urllib.request.urlopen(url2014).read().decode("utf-8")
        fire2014 = json.loads(response2014)

        url2015 = 'http://datamechanics.io/data/2015fireincident_anabos2.json'
        response2015 = urllib.request.urlopen(url2015).read().decode("utf-8")
        fire2015 = json.loads(response2015)


        # dict_values = csvConvert()

        repo.dropCollection("fire")
        repo.createCollection("fire")
        repo['yuxiao_yzhang11.fire'].insert_many(fire2013)
        repo['yuxiao_yzhang11.fire'].insert_many(fire2014)
        repo['yuxiao_yzhang11.fire'].insert_many(fire2015)

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
        doc.add_namespace('bdp', 'https://data.boston.gov/export/767/71c/')


        this_script = doc.agent('alg:yuxiao_yzhang11#fire',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dat:2013fireincident_anabos2',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',})

        output =  doc.entity('dat:yuxiao_yzhang11.fire', {prov.model.PROV_LABEL:'fire', prov.model.PROV_TYPE:'ont:DataSet'})

        # get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        #
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)
        # doc.wasAssociatedWith(get_lost, this_script)
        # doc.usage(get_found, resource, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Retrieval',
        #            'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
        #            }
        #           )
        #
        # doc.usage(get_lost, resource, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Retrieval',
        #            'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #            }
        #           )

        # lost = doc.entity('dat:alice_bob#lost',
        #                   {prov.model.PROV_LABEL: 'Animals Lost', prov.model.PROV_TYPE: 'ont:DataSet'})
        # doc.wasAttributedTo(lost, this_script)
        # doc.wasGeneratedBy(lost, get_lost, endTime)
        # doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)
        #
        # found = doc.entity('dat:alice_bob#found',
        #                    {prov.model.PROV_LABEL: 'Animals Found', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, resource, this_run, this_run, this_run)

        repo.logout()

        return doc


fire.execute()
doc = fire.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
