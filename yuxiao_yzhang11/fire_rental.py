import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import pymongo


class fire_rental(dml.Algorithm):
    contributor = 'yuxiao_yzhang11'
    reads = ['yuxiao_yzhang11.fire', 'yuxiao_yzhang11.rental']
    writes = ['yuxiao_yzhang11.fire_rental']

    @staticmethod
    def execute(trial=True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yuxiao_yzhang11', 'yuxiao_yzhang11')

        repo.dropCollection("rental_zip_price")
        repo.createCollection("rental_zip_price")
        # loads fire data
        repo.dropCollection("fire_count")
        repo.createCollection("fire_count")
        fire = repo['yuxiao_yzhang11.fire']
        group = {
            '_id': "$Zip",
            'count': {'$sum': 1}
        }

        fireCount = fire.aggregate([
            {
                '$group': group
            }
        ])

        repo['yuxiao_yzhang11.fire_count'].insert(fireCount)



        # loads rental data


        rental = repo['yuxiao_yzhang11.rental'].find()
        #year_keys= ["2013-03","2013-04","2013-05","2013-06","2013-07","2013-08"]
        rental_sum = 0
        for i in rental:
            rental_zip_price = {}
            regionZip = i["RegionName"]
            new_regionZip = "0" + regionZip
            rental_zip_price["Zip"] = new_regionZip
            rental_zip_price["Average"] = "2246"
            print(rental_zip_price)

            repo['yuxiao_yzhang11.rental_zip_price'].insert(rental_zip_price)


        repo.logout()
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
        repo.authenticate('yuxiao_yzhang11', 'yuxiao_yzhang11')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/export/767/71c/')

        this_script = doc.agent('alg:yuxiao_yzhang11#fire_rental',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource1 = doc.entity('dat: yuxiao_yzhang11#fire',
                               {'prov:label': 'Analyze Boston', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('dat:yuxiao_yzhang11#rental',
                               {'prov:label': 'Zillow', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'csv'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.usage(this_run, resource1, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval', })
        doc.usage(this_run, resource2, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval', })

        output = doc.entity('dat:yuxiao_yzhang11.fire_rental',
                            {prov.model.PROV_LABEL: 'fire_rental', prov.model.PROV_TYPE: 'ont:DataSet'})

        # get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        #
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource1, startTime)
        doc.used(this_run, resource2, startTime)
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
        doc.wasDerivedFrom(output, resource1, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, resource2, this_run, this_run, this_run)

        repo.logout()

        return doc

# fire_rental.execute()
# doc = fire_rental.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
