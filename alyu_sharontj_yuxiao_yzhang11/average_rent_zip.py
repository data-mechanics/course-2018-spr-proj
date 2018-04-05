import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import pymongo


class average_rent_zip(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.rental']
    writes = ['alyu_sharontj_yuxiao_yzhang11.average_rent_zip']

    @staticmethod
    def execute(trial=True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        repo.dropCollection("average_rent_zip")
        repo.createCollection("average_rent_zip")

        rental = repo['alyu_sharontj_yuxiao_yzhang11.rental'].find()
        month_keys = ["2017-01","2017-02","2017-03","2017-04","2017-05","2017-06","2017-07","2017-08","2017-09","2017-10","2017-11","2017-12"]

        for i in rental:
            rent_sum = 0
            rental_zip_price = {}
            regionZip = i["RegionName"]
            new_regionZip = "0" + regionZip
            rental_zip_price["Zip"] = new_regionZip
            for m in month_keys:
                month_rent = i[m]
                if month_rent != "":
                    rent_number = float(month_rent)
                    rent_sum += rent_number

            rent_average = rent_sum /12.0   
            rental_zip_price["Average"] = rent_average

            repo['alyu_sharontj_yuxiao_yzhang11.average_rent_zip'].insert(rental_zip_price)


        # repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod

    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.'''
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

        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#average_rent_zip',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})


        resource = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#rental',
                               {'prov:label': 'rental', prov.model.PROV_TYPE: 'ont:DataSet'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        output = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#average_rent_zip',
                            {prov.model.PROV_LABEL: 'average_rent_zip', prov.model.PROV_TYPE: 'ont:DataSet'})

        # get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        #
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)


        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, resource, this_run, this_run, this_run)

        repo.logout()

        return doc

# fire_rental.execute()
# doc = fire_rental.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
