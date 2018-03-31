import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import sys
from math import radians, sqrt, sin, cos, atan2
from z3 import *

''' This file will return a model for each district. The model will tell us
the minimum number of night patrols needed in each district such that
every street will be covered. Multiple streets can be covered by a night patrol
if they're less than 1 block away (approx 800 ft. or 0.15 miles). We will use
z3 to find a satisfiable model given these constraints. (similar to edge cover).'''


def geodistance(la1, lon1, la2, lon2):
    ''' helper function to calculate the distance between 2 geolocations '''
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


def findMinSat(streets):
    """ Helper function to find the minimal satisfiable solution given a list of streets
    in the form [[street_name, lat#, long#], [_,_,_] ...]"""
    s=Solver()
    vs = {}

    #Exract points (latitutde and longitude) of each street, create a new z3.Int for each street.
    #Store it in values (vs) dictionaruy in the form {'z3': z3_value,  'street': street_name}
    i=0
    for street in streets:
        vs["c" + str(i)] = {'z3':z3.Int('c'+str(i)), 'street':street[0]}
        s.add(vs["c"+str(i)]['z3'] >= 0)
        i+=1

    # For each street point, calculate its distance to every other street.
    # If the distance between i and j is less than 0.15 (~800 ft.), add a constraint
    # where ci + cj >= 1 (c is the street point).
    for i in range(len(streets)):
        for j in range(i+1,len(streets)):
            s1lat = streets[i][1]
            s1long = streets[i][2]
            s2lat = streets[j][1]
            s2long = streets[j][2]

            dist = geodistance(s1lat,s1long,s2lat,s2long) #Calls geodistance helper function
            if dist < 0.15:
                s.add(vs["c"+str(i)]['z3'] + vs["c"+str(j)]['z3'] >= 1)

    #This value represents the objectifying function (which is the sum of all the edges)
    #st is the statement c = c0 + c1 + c2+ ... + cn as a string. exec(st).
    vs['c' + str(len(vs))] = {'z3': z3.Int('c' + str(len(vs))), 'street': 'Total' } 
    st = "s.add(vs['c" + str(len(vs)-1) + "']['z3']== vs['c" + str(0) + "']['z3']"
    for i in range(1,len(vs)-1):
        st+="+ vs['c" + str(i) + "']['z3']"
    st += ")"
    exec(st)
    
    # Find the minimal value of the objectifying variable
    tot=0
    for i in range(len(vs)):
        s.push()
        s.add(vs['c' + str(len(vs)-1)]['z3'] <= i)
        if str(s.check()) == 'sat':
            tot = i
            break
        s.pop()
    print(s.check())
    return(s.model(), vs)   # Return the model and values              

class findCrimeStats(dml.Algorithm):
    contributor = 'janellc_rstiffel_yash'
    reads = ['janellc_rstiffel_yash.crimeDistricts']
    writes = ['janellc_rstiffel_yash.crimeStats']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')

        # Get the crime data (from yash's project #1)
        crimesData = repo.janellc_rstiffel_yash.crimeDistricts.find()

        # For each district in the crimesData, calculate the minimum number of night patrols
        # so that every street is covered. Multiple streets can be covered if the streets
        # are within a block (approx 800 feet or 0.15 miles). Uses z3 to find a satisfiable
        # solution given the constraints and prints the model (edge cover problem).
        # 1 = there is a patrol stationed at this street
        # 0 = no patrol is stationed at this street
        d_results = {}
        for d in crimesData:
            district = list(d.keys())[1]
            boston_district = d[district]
            streets = []
            #print("District", district)

            for street in boston_district:
                streets.append([street,boston_district[street]['Lat'],boston_district[street]['Long']])

            f = findMinSat(streets) # Calls findMinSat helper function, returns model and street name dict
            (model,vs) = f

            # Process the model, store results in a dictionary in form 
            # {District: {Total: #, streets_results: {street: 0, street:1, etc.}}
            # 0 means no night patrol, 1 means there is a patrol
            street_results = {}
            for entry in model: 
                if vs[str(entry)]['street'] == 'Total':
                    streets_total = int(str(model[entry]))
                street_results[vs[str(entry)]['street']] = int(str(model[entry]))
                #print(vs[str(entry)]['street'],"=", int(str(model[entry])))

            d_results[district] = {'total': streets_total, 'streets_results': street_results}

        #print(d_results)


        repo.dropCollection("crimeStats")
        repo.createCollection("crimeStats")

        for key,value in d_results.items():
             r = {key:value}
             repo['janellc_rstiffel_yash.crimeStats'].insert(r)

        repo['janellc_rstiffel_yash.crimeStats'].metadata({'complete':True})
        print(repo['janellc_rstiffel_yash.crimeStats'].metadata())

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
        repo.authenticate('yash', 'yash')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ybavishi#') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/ybavishi#') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cri', 'https://data.boston.gov/')

        this_script = doc.agent('alg:getCrimeData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('cri:12cb3883-56f5-47de-afa5-3b1cf61b257b', {'prov:label':'Crimes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_prices = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_prices, this_script)

        doc.usage(get_prices, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'api/3/action/datastore_search?offset=$&resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b'
                  }
                  )
        
        prices = doc.entity('dat:crimesData', {prov.model.PROV_LABEL:'Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prices, this_script)
        doc.wasGeneratedBy(prices, get_prices, endTime)
        doc.wasDerivedFrom(prices, resource, get_prices, get_prices, get_prices)

      
        repo.logout()
                  
        return doc

findCrimeStats.execute()
#doc = getCrimeData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
## eof