import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geojson
from math import radians, sqrt, sin, cos, atan2

"""
Counts the number of bike network starting points within 2 miles for each Boston tract, finds medium income as well.
- Joins medium income and tracts
        - Product
        - Selection
- Combines bike data + tract data
        - Distance function used to calculate the distance between Bike geolocation & tract geolocation

"""
def product(R, S):
    return [(t,u) for t in R for u in S]

def geodistance(la1, lon1, la2, lon2):
        la1 = radians(la1)
        lon1 = radians(lon1)
        la2 = radians(la2)
        lon2 = radians(lon2)

        dlon = lon1 - lon2

        EARTH_R = 6372.8

        y = sqrt(
            (cos(la2) * sin(dlon)) ** 2
            + (cos(la1) * sin(la2) - sin(la1) * cos(la2) * cos(dlon)) ** 2
            )
        x = sin(la1) * sin(la2) + cos(la1) * cos(la2) * cos(dlon)
        c = atan2(y, x)
        return EARTH_R * c

class transformBikeNetwork(dml.Algorithm):
    contributor = 'janellc_rstiffel'
    reads = ['janellc_rstiffel.bikeNetwork', 'janellc_rstiffel.Income', 'janellc_rstiffel.bostonTracts']
    writes = ['janellc_rstiffel.bikePerTract']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')

        # Get Open Space data
        incomeData = repo.janellc_rstiffel.Income.find()
        tractsData = repo.janellc_rstiffel.bostonTracts.find()
        bikeData = repo.janellc_rstiffel.bikeNetwork.find()

        # Collect all the starting bike points, store in array.
        bike_points = []
        for row in bikeData:
            #print("\n\nBike data", row['geometry']['coordinates'][0])
            if type(row['geometry']['coordinates'][0][1]) != type(0.0): #Skip over invalid long/lat
                continue
            point = [row['geometry']['coordinates'][0]]
            bike_points += point
        
        # Project income in the form (ID, income)
        income = []
        for row in incomeData:
            income += [(row['GEOID10'], row['median_income'])]

        # Project tracts in the form (ID, lat, long)
        tracts = []
        for row in tractsData:
            tracts += [(row['GEOID'], row['INTPTLAT'], row['INTPTLONG\r'])]
        
        #JOIN:
            # Product in form ((ID, income), (ID, lat, long))
        income_tracts_prod = product(income, tracts)
            # Select in form ((ID, income), (ID, lat, long))
        income_tracts_sel = [t for t in income_tracts_prod if t[0][0] == t[1][0] and t[0][1] != '-'] #filter out the no-income entries
            # Project to get the form ((ID, income, lat, long))
        income_tracts = [(t[0][0], t[0][1], t[1][1], t[1][2]) for t in income_tracts_sel]


        # Count the number of Bike Networks that is wthin 2 mile radius; store in dict
        tract_data = {}
        for tract in income_tracts:
            tract_lat = tract[2]
            tract_long = tract[3]
            num_points = 0
            for bike_point in bike_points:
                #print(bike_point)
                d = geodistance(float(tract_lat), float(tract_long), bike_point[1], bike_point[0])
                if (d < 2.0):
                    num_points += 1
            tract += (num_points,)
            tract_data[tract[0]] = {'avgIncome':tract[1], 'latitutde':tract[2], 'longitude':tract[3], 'numBikeNetworks':tract[4]}
            

        # Store in bikePerTract.json
        with open("./transformed_datasets/bikePerTract.json", 'w') as outfile:
            json.dump(tract_data, outfile)


        # Store in DB
        repo.dropCollection("bikePerTract")
        repo.createCollection("bikePerTract")
        for key,value in tract_data.items():
            repo['janellc_rstiffel.bikePerTract'].insert({key:value})
        repo['janellc_rstiffel.bikePerTract'].metadata({'complete':True})
        print(repo['janellc_rstiffel.bikePerTract'].metadata())


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel', 'janellc_rstiffel')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/janellc_rstiffel/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/janellc_rstiffel/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        
        #Agent, entity, activity
        this_script = doc.agent('alg:janellc_rstiffel#transformBikeNetwork', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:janellc_rstiffel#Income', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('dat:janellc_rstiffel#bostonTracts', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource3 = doc.entity('dat:janellc_rstiffel#bikeNetwork', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        
        transform_bikeNetwork = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transform_bikeNetwork, this_script)

        doc.usage(transform_bikeNetwork, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )
        doc.usage(transform_bikeNetwork, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )
        doc.usage(transform_bikeNetwork, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )

        bikePerTract = doc.entity('dat:janellc_rstiffel#bikePerTract', {prov.model.PROV_LABEL:'Num bike points per boston tract', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikePerTract, this_script)
        doc.wasGeneratedBy(bikePerTract, transform_bikeNetwork, endTime)
        doc.wasDerivedFrom(bikePerTract, resource1, transform_bikeNetwork, transform_bikeNetwork, transform_bikeNetwork)
        doc.wasDerivedFrom(bikePerTract, resource2, transform_bikeNetwork, transform_bikeNetwork, transform_bikeNetwork)
        doc.wasDerivedFrom(bikePerTract, resource3, transform_bikeNetwork, transform_bikeNetwork, transform_bikeNetwork)

        repo.logout()
                  
        return doc

# transformBikeNetwork.execute()
# doc = transformBikeNetwork.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
