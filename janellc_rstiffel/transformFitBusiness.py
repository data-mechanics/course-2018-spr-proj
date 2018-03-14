import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

"""
Finds number of fitness related businesses in each zip code,
Finds total acres of open space in each district,
Uses existing avg size of park in a district
Joins on district name,

"""

class transformFitBusiness(dml.Algorithm):
    contributor = 'janellc_rstiffel'
    reads = ['janellc_rstiffel.districtAvgAcres', 'janellc_rstiffel.fitBusinesses', 'janellc_rstiffel.openSpace']
    writes = ['janellc_rstiffel.zipBusinessPark']

    @staticmethod
    def execute(trial = False):
        ' Merge data sets'
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')

        # load collections

        OS = repo['janellc_rstiffel.openSpace'].find({}, {'properties.DISTRICT': 1, 'properties.ACRES': 1})
        FB = repo['janellc_rstiffel.fitBusinesses'].find()
        data = repo['janellc_rstiffel.districtAvgAcres'].find()


        # Create dictionary to count fitness businesses in each zipcode
        businesses = {}
        for b in FB:
            zip = '0' + b['Zipcode\r']  # fix zipcode
            if zip not in businesses:
                businesses[zip] = {'count': 1}
            else:
                businesses[zip]['count'] += 1


        # calculate total acres of open space per district (aggregate)
        district_acres = {}
        for row in OS:
            key = str(row['properties']['DISTRICT'])
            value = row['properties']['ACRES']
            if type(value) != float:
                break
            if key not in district_acres:
                district_acres[key] = {'totalAcres':value}
            else:
                district_acres[key]['totalAcres'] += value

        # print(district_acres)
        # print(businesses)

        # Create final dictionary to join business counts with avg park acres joined on District
        new = {}
        for row in data:
            for key,value in row.items():
                if key=="_id":
                     continue
                for keyb, valueb in businesses.items():
                    if key==keyb:
                        new[key]={'test': 'hi'}
                        new[key]={'District': value['District'], 'AvgParkSize': value['Acres'], 'fitBusinessCount': valueb['count']}


        # join total Acres per district on District name
        for key,value in new.items():
          dis = value['District']
          for key2,value2 in district_acres.items():
              if key2==dis:
                  new[key]['totalParkAcres']=value2['totalAcres']

        with open("./janellc_rstiffel/transformed_datasets/zipBusinessPark.json", 'w') as outfile:
            json.dump(new, outfile)

        repo.dropCollection('janellc_rstiffel.zipBusinessPark')
        repo.createCollection('janellc_rstiffel.zipBusinessPark')


        for key,value in new.items():
            repo['janellc_rstiffel.zipBusinessPark'].insert({key:value})
        
        repo['janellc_rstiffel.zipBusinessPark'].metadata({'complete': True})
        print(repo['janellc_rstiffel.zipBusinessPark'].metadata())



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

        # three resources: openSpace, districtAvgAcres, fitBusinesses
        resource1 = doc.entity('dat:janellc_rstiffel#openSpace',
                               {'prov:label': 'Open Space', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'geojson'})
        resource2 = doc.entity('dat:janellc_rstiffel#districtAvgAcres',
                               {'prov:label': 'Acres per park by district', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})

        resource3 = doc.entity('dat:janellc_rstiffel#fitBusinesses',
                               {'prov:label': 'Fitness Businesses', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})

        transformFitBusiness = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(transformFitBusiness, this_script)
        doc.usage(transformFitBusiness, resource1, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Calculation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(transformFitBusiness, resource2, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Calculation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(transformFitBusiness, resource3, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Calculation',
                   'ont:Query': ''
                   }
                  )
        zipBusinessPark = doc.entity('dat:janellc_rstiffel#zipBusinessPark',
                                   {prov.model.PROV_LABEL: 'park and business data ny zip', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(zipBusinessPark, this_script)
        doc.wasGeneratedBy(zipBusinessPark, transformFitBusiness, endTime)
        doc.wasDerivedFrom(zipBusinessPark, resource1, transformFitBusiness, transformFitBusiness, transformFitBusiness)
        doc.wasDerivedFrom(zipBusinessPark, resource2, transformFitBusiness, transformFitBusiness, transformFitBusiness)
        doc.wasDerivedFrom(zipBusinessPark, resource3, transformFitBusiness, transformFitBusiness, transformFitBusiness)
        

        repo.logout()

        return doc

# transformFitBusiness.execute()
# doc = transformFitBusiness.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
