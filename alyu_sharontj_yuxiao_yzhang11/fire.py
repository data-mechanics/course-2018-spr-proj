import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv



class fire(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = []
    writes = ['alyu_sharontj_yuxiao_yzhang11.fire','alyu_sharontj_yuxiao_yzhang11.fire_count']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

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
        repo['alyu_sharontj_yuxiao_yzhang11.fire'].insert_many(fire2013)
        repo['alyu_sharontj_yuxiao_yzhang11.fire'].insert_many(fire2014)
        repo['alyu_sharontj_yuxiao_yzhang11.fire'].insert_many(fire2015)

        repo.dropCollection("fire_count")
        repo.createCollection("fire_count")
        fire = repo['alyu_sharontj_yuxiao_yzhang11.fire']
        group = {
            '_id': "$Zip",
            'count': {'$sum': 1}
        }

        fireCount = fire.aggregate([
            {
                '$group': group
            }
        ])

        repo['alyu_sharontj_yuxiao_yzhang11.fire_count'].insert(fireCount)

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
        doc.add_namespace('bdp', 'https://data.boston.gov/export/767/71c/')


        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#fire',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        # this_script2 = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#fire_count',
        #                         {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource1 = doc.entity('dat:2013fireincident_anabos2',
                              {'prov:label': 'fire1', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        resource2 = doc.entity('dat:2014fireincident_anabos2',
                              {'prov:label': 'fire2', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        resource3 = doc.entity('dat:2015fireincident_anabos2',
                              {'prov:label': 'fire3', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.usage(this_run, resource1, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        output1 =  doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#fire',
                              {prov.model.PROV_LABEL:'fire',
                               prov.model.PROV_TYPE:'ont:DataSet'})
        output2 =  doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#fire_count',
                              {prov.model.PROV_LABEL:'fire_count',
                               prov.model.PROV_TYPE:'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        # doc.wasAssociatedWith(this_run, this_script2)
        doc.used(this_run, resource1, startTime)
        doc.used(this_run, resource2, startTime)
        doc.used(this_run, resource3, startTime)


        doc.wasAttributedTo(output1, this_script)
        doc.wasGeneratedBy(output1, this_run, endTime)
        doc.wasDerivedFrom(output1, resource1, this_run, this_run, this_run)
        doc.wasDerivedFrom(output1, resource2, this_run, this_run, this_run)
        doc.wasDerivedFrom(output1, resource3, this_run, this_run, this_run)

        doc.wasAttributedTo(output2, this_script)
        doc.wasGeneratedBy(output2, this_run, endTime)
        doc.wasDerivedFrom(output2, resource1, this_run, this_run, this_run)
        doc.wasDerivedFrom(output2, resource2, this_run, this_run, this_run)
        doc.wasDerivedFrom(output2, resource3, this_run, this_run, this_run)



        repo.logout()

        return doc


# fire.execute()
# doc = fire.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
