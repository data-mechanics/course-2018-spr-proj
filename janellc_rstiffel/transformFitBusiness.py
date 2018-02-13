import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

"""
Finds number of fitness related businesses in each zip code (aggregation)
Joins with district name and average size of park on zip code

"""

class transformfitBusiness(dml.Algorithm):
    contributor = 'janellc_rstiffel'
    reads = ['janellc_rstiffel.obesityData', 'janellc_rstiffel.fitBusinesses']
    writes = ['janellc_rstiffel.obesity_businesses']

    @staticmethod
    def execute(trial = False):
        ' Merge data sets'
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')

        # load collections
        NB = repo['janellc_rstiffel.Neighborhoods'].find({}, {'properties.PD': 1, 'geometry.coordinates': 1, 'properties.Longitude': 1})
        FB = repo['janellc_rstiffel.fitBusinesses'].find()
        hi = repo['janellc_rstiffel.zipCodes'].find()
        data = repo['janellc_rstiffel.districtAvgAcres'].find()



        businesses = {}
        for b in FB:
            # print(b)
            zip = '0' + b['Zipcode\r'] #fix zipcode
            #print(b['Zipcode\r'])
            if zip not in businesses:  # if zipcode not in dictionary add it
                businesses[zip] = {'count': 1}
            else:                               # if its already in dictionary increase count
                businesses[zip]['count'] += 1


        new = {}
        for row in data:
            for key,value in row.items():
                if key=="_id":
                    continue
                for keyb, valueb in businesses.items():
                    if key==keyb:
                        new[key]={'District': value['District'], 'AvgParkSize': value['Acres'], 'fitBusinessCount': valueb['count']}



        with open("./transformed_datasets/zipBusinessPark.json", 'w') as outfile:
            json.dump(new, outfile)

        repo.dropCollection('janellc_rstiffel.zipBusinessPark')
        repo.createCollection('janellc_rstiffel.zipBusinessPark')


        for key,value in new.items():
            repo['janellc_rstiffel.zipBusinessPark'].insert({key:value})
            repo['janellc_rstiffel.zipBusinessPark'].metadata({'complete': True})




    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets')

        this_script = doc.agent('alg:janellc_rstiffel#businessZips',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bod:a6488cfd737b4955bf55b0342c74575b_0',
                              {'prov:label': 'Planning Districts', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'geojson'})
        transformFitBusiness = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transformFitBusiness, this_script)

        doc.usage(transformFitBusiness, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )

        businessZips = doc.entity('dat:janellc_rstiffel#businessZips',
                                   {prov.model.PROV_LABEL: 'business zips', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(businessZips, this_script)
        doc.wasGeneratedBy(businessZips, transformFitBusiness, endTime)
        doc.wasDerivedFrom(businessZips, resource, transformFitBusiness, transformFitBusiness, transformFitBusiness)

        repo.logout()

        return doc

transformfitBusiness.execute()