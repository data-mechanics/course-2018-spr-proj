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
districtNames = {'A1':'Downtown','D14':'Brighton','B2':'Roxbury',\
'E13':'Jamaica Plain','C11':'Dorcester','B3':'Mattapan','D4':'South End',\
'E5':'West Roxbury', 'C6' :'South Boston','E18':'Hyde Park',\
'A7':'East Boston', 'A15':'Charlestown'}

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


def findMinSat(streets, distance_input):
    """ Helper function to find the minimal satisfiable solution given a list of streets
    in the form [[street_name, lat#, long#], [_,_,_] ...]"""
    s=Solver()
    vs = {}
    #Exract points (latitutde and longitude) of each street, create a new z3.Int for each street.
    #Store it in values (vs) dictionaruy in the form {'z3': z3_value,  'street': street_name}
    i=0
    for street in streets:
        vs["c" + str(i)] = {'z3':z3.Int('c'+str(i)), 'street':street[0], 'lat':street[1], 'long':street[2]}
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
            if dist < distance_input:
                s.add(vs["c"+str(i)]['z3'] + vs["c"+str(j)]['z3'] >= 1)
    #This value represents the objectifying function (which is the sum of all the edges)
    #st is the statement c = c0 + c1 + c2+ ... + cn as a string. exec(st).
    the_length = len(vs)
    vs['c' + str(len(vs))] = {'z3': z3.Int('c' + str(len(vs))), 'street': 'Total' } 
    st = "s.add(vs['c" + str(len(vs)-1) + "']['z3']== vs['c" + str(0) + "']['z3']"
    for i in range(1,len(vs)-1):
        st+="+ vs['c" + str(i) + "']['z3']"
    st += ")"
    exec(st)
    s.add(vs['c'+str(the_length)]['z3'] >= 0)
    
    # Find the minimal value of the objectifying variable
    tot=0
    for i in range(len(vs)):
        s.push()
        s.add(vs['c' + str(len(vs)-1)]['z3'] <= i)
        if str(s.check()) == 'sat':
            tot = i
            break
        s.pop()
    if (str(s.check()) == 'unsat'):
        return 'unsat'
    return(s.model(), vs)   # Return the model and values              

class optCrime():

    @staticmethod
    def execute(d_input, distance_input):
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
            district_name = districtNames[district]
            if d_input != district_name:
                continue
            boston_district = d[district]
            streets = []
            
            for street in boston_district:
                streets.append([street,boston_district[street]['Lat'],boston_district[street]['Long']])
            distance_input = distance_input*0.000189393939
            f = findMinSat(streets, distance_input) # Calls findMinSat helper function, returns model and street name dict
            if f == 'unsat':
                return f
            (model,vs) = f

            # Process the model, store results in a dictionary in form 
            # {District: {Total: #, streets_results: {street: 0, street:1, etc.}}
            street_results = {}
            for entry in model: 
                if vs[str(entry)]['street'] == 'Total':
                    streets_total = len(model)-1 - int(str(model[entry]))
                    continue
                street_results[vs[str(entry)]['street']] = {'Patrol?': 'No' if int(str(model[entry])) else 'Yes', 'lat':float(vs[str(entry)]['lat']), 'long': float(vs[str(entry)]['long'])}
                #print(vs[str(entry)]['street'],"=", int(str(model[entry])))

            d_results[district_name] = {'total': streets_total, 'streets_results': street_results}
            break
        repo.logout()
        return d_results
    



#print(optCrime.execute('Dorcester', 800))