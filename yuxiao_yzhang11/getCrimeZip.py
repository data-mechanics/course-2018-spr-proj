import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.geocoders import Nominatim


'''

install package geopy

`pip install geopy`

'''


class getCrimeZip(dml.Algorithm):
    contributor = 'yuxiao_yzhang11'
    reads = ['yuxiao_yzhang11.crime']
    writes = ['yuxiao_yzhang11.crimeZip','yuxiao_yzhang11.crimeCor']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yuxiao_yzhang11', 'yuxiao_yzhang11')

        crime = repo['yuxiao_yzhang11.crime']

        group = {
            '_id': "$Location",
            'count': {'$sum': 1}
        }

        crimeCor = crime.aggregate([
            {
                '$group':group
            }
        ])

        repo.dropCollection("crimeCor")
        repo.createCollection("crimeCor")
        repo['yuxiao_yzhang11.crimeCor'].insert_many(crimeCor)

        repo.dropCollection("crimeZip")
        repo.createCollection("crimeZip")

        crime_zips = []
        for i in crimeCor:
            cor = i['_id'].replace('(','')
            cor = cor.replace(')','')
            # print(cor)
            if (cor != "0.0, 0.0"):
                geolocator = Nominatim()
                location = geolocator.reverse(cor, timeout=10)
                try:
                    zip = location.raw['address']['postcode']
                    i['zip'] = zip
                    print(i)
                    crime_zips.append(i)
                    repo['yuxiao_yzhang11.crimeZip'].insert(i)
                except KeyError as e:
                    print("Error: ",e,", at cord: ", cor,", and zip: ",zip, ", and location: ", location)


        # repo['yuxiao_yzhang11.crimeZip'].insert_many(crime_zips)


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


        this_script = doc.agent('alg:yuxiao_yzhang11#getCrimeZip',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dat:20127to20158crimeincident1edit',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',})

        output =  doc.entity('dat:yuxiao_yzhang11.crimeZip', {prov.model.PROV_LABEL:'CrimeZip', prov.model.PROV_TYPE:'ont:DataSet'})

        # get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        #
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)
        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, resource, this_run, this_run, this_run)

        repo.logout()

        return doc


getCrimeZip.execute()
doc = getCrimeZip.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
