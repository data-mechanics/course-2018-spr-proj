import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv



class garden(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = []
    writes = ['alyu_sharontj_yuxiao_yzhang11.garden',
              'alyu_sharontj_yuxiao_yzhang11.garden_count',
              'alyu_sharontj_yuxiao_yzhang11.garden_new_zip']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        url = 'http://datamechanics.io/data/alyu_sharontj_yuxiao_yzhang11/garden_json.json'
        response_json = urllib.request.urlopen(url).read().decode("utf-8")

        r = json.loads(response_json)



        repo.dropCollection("garden")
        repo.createCollection("garden")
        repo['alyu_sharontj_yuxiao_yzhang11.garden'].insert_many(r)


        garden = repo['alyu_sharontj_yuxiao_yzhang11.garden'].find()

        repo.dropCollection("garden_new_zip")
        repo.createCollection("garden_new_zip")

        for i in garden:
            garden_new_zip = {}
            old_zip = i["zip_code"]
            new_zip = "0" + old_zip
            garden_new_zip["zip"] = new_zip
            garden_new_zip["location"] = i["location"]
            garden_new_zip["site"] = i["site"]
            repo['alyu_sharontj_yuxiao_yzhang11.garden_new_zip'].insert(garden_new_zip)


        repo.dropCollection("garden_count")
        repo.createCollection("garden_count")

        garden_with_new_zip = repo['alyu_sharontj_yuxiao_yzhang11.garden_new_zip']

        group = {
            '_id': "$zip",
            'count': {'$sum': 1}
        }

        gardenCount = garden_with_new_zip.aggregate([
            {
                '$group': group
            }
        ])

        repo['alyu_sharontj_yuxiao_yzhang11.garden_count'].insert(gardenCount)

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
        doc.add_namespace('ont','http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/alyu_sharontj_yuxiao_yzhang11/')

        # url = 'http://datamechanics.io/data/alyu_sharontj_yuxiao_yzhang11/garden_json.json'

        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#garden',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'],
                                 'ont:Extension': 'py'})

        resource = doc.entity('dat:garden_json',
                              {'prov:label': 'garden',
                               prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_garden = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_garden_count = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_garden_new_zip = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_garden, this_script)
        doc.wasAssociatedWith(get_garden_count, this_script)
        doc.wasAssociatedWith(get_garden_new_zip, this_script)

        doc.usage(get_garden, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_garden_count, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_garden_new_zip, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        output1 =  doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#garden',
                              {prov.model.PROV_LABEL:'garden',
                               prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(output1, this_script)
        doc.wasGeneratedBy(output1, get_garden, endTime)
        doc.wasDerivedFrom(output1, resource, get_garden, get_garden, get_garden)



        output2 =  doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#garden_count',
                              {prov.model.PROV_LABEL:'garden_count',
                               prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(output2, this_script)
        doc.wasGeneratedBy(output2, get_garden_count, endTime)
        doc.wasDerivedFrom(output2, resource, get_garden_count, get_garden_count, get_garden_count)

        output3 =  doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#garden_new_zip',
                              {prov.model.PROV_LABEL:'garden_new_zip',
                               prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(output3, this_script)
        doc.wasGeneratedBy(output3, get_garden_new_zip, endTime)
        doc.wasDerivedFrom(output3, resource, get_garden_new_zip, get_garden_new_zip, get_garden_new_zip)

        repo.logout()

        return doc


# garden.execute()
# doc = garden.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
