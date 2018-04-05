import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pprint
import csv



class garden_vs_rent(dml.Algorithm):


    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.garden_count','alyu_sharontj_yuxiao_yzhang11.average_rent_zip']
    writes = ['alyu_sharontj_yuxiao_yzhang11.garden_vs_rent']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        garden_count = repo['alyu_sharontj_yuxiao_yzhang11.garden_count']
        average_rent = repo['alyu_sharontj_yuxiao_yzhang11.average_rent_zip']

        # print("i am here ")
        # s = average_rent.find_one({"Zip": "11111"})
        # print(s == None)
        #pprint.pprint(average_rent.find_one({"Zip": "02169"}))
        repo.dropCollection("garden_vs_rent")
        repo.createCollection("garden_vs_rent")

        garden_cur = garden_count.find()
        rent_cur = average_rent.find()
        for i in rent_cur:
            garden_vs_rent= {}
            rent_zip = i["Zip"]
            # print("rent zip", rent_zip)
            garden_vs_rent["Zip"] = rent_zip
            garden_vs_rent["Average"] = i["Average"]


            if (garden_count.find_one({"_id": rent_zip}) != None):

                garden_vs_rent["garden_count"] = garden_count.find_one({"_id": rent_zip})['count']
            else:
                garden_vs_rent["garden_count"] = 0

            repo['alyu_sharontj_yuxiao_yzhang11.garden_vs_rent'].insert(garden_vs_rent)


        # print("INNN garden_vs_rent")
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


        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#garden_vs_rent',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})


        garden_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#garden_count',
                                  {prov.model.PROV_LABEL: 'garden_count',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})

        rent_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#average_rent_zip',
                                  {prov.model.PROV_LABEL: 'average_rent_zip',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)



        output =  doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#garden_vs_rent',
                             {prov.model.PROV_LABEL:'garden_vs_rent',
                              prov.model.PROV_TYPE:'ont:DataSet'})

        # get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        #
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, garden_input, startTime)
        doc.used(this_run, rent_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, garden_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, rent_input, this_run, this_run, this_run)

        repo.logout()

        return doc


# garden_vs_rent.execute()
# doc = garden_vs_rent.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
