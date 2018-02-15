import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.geocoders import Nominatim


class crime(dml.Algorithm):
    contributor = 'yuxiao_yzhang11'
    reads = []
    writes = ['yuxiao_yzhang11.crime']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yuxiao_yzhang11', 'yuxiao_yzhang11')

        url1 = 'http://datamechanics.io/data/20127to20158crimeincident1edit.json'
        response1 = urllib.request.urlopen(url1).read().decode("utf-8")
        crime1 = json.loads(response1)

        # for i in crime1:
        #     cor = i['Location'].replace('(', '')
        #     cor = cor.replace(')', '')
        #     # print(cor)
        #     if (cor != "0.0, 0.0"):
        #         geolocator = Nominatim()
        #         location = geolocator.reverse(cor, timeout=None)
        #         try:
        #             zip = location.raw['address']['postcode']
        #             i['zip'] = zip
        #         except KeyError as e:
        #             print("KeyError at cord: ", cor, ", and zip: ", zip, ", and location: ", location)

        url2 = 'http://datamechanics.io/data/20127to20158crimeincident2.json'
        response2 = urllib.request.urlopen(url2).read().decode("utf-8")
        crime2 = json.loads(response2)

        # for i in crime2:
        #     cor = i['Location'].replace('(', '')
        #     cor = cor.replace(')', '')
        #     # print(cor)
        #     if (cor != "0.0, 0.0"):
        #         geolocator = Nominatim()
        #         location = geolocator.reverse(cor, timeout=None)
        #         try:
        #             zip = location.raw['address']['postcode']
        #             i['zip'] = zip
        #         except KeyError as e:
        #             print("KeyError at cord: ", cor, ", and zip: ", zip, ", and location: ", location)

        repo.dropCollection("crime")
        repo.createCollection("crime")
        repo['yuxiao_yzhang11.crime'].insert_many(crime1)
        repo['yuxiao_yzhang11.crime'].insert_many(crime2)


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
        # doc.add_namespace('bdp', 'https://data.boston.gov/dataset/eefad66a-e805-4b35-b170-d26e2028c373/resource/ba5ed0e2-e901-438c-b2e0-4acfc3c452b9/download/')


        this_script = doc.agent('alg:yuxiao_yzhang11#crime',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dat:20127to20158crimeincident1edit',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',})

        output =  doc.entity('dat:yuxiao_yzhang11.crime', {prov.model.PROV_LABEL:'Crime', prov.model.PROV_TYPE:'ont:DataSet'})

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


crime.execute()
doc = crime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
