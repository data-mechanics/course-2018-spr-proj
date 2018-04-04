import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from vincenty import vincenty

class subway_crime(dml.Algorithm):
    contributor = 'aolzh'
    reads = ['aolzh.NewYorkSubway', 'aolzh.NewYorkCrime']
    writes = ['aolzh.NewYorkSubway_Crime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aolzh', 'aolzh')

        newyorksubway = repo.aolzh.NewYorkSubway
        newyorkcrime = repo.aolzh.NewYorkCrime

        subway = newyorksubway.find()
        crime = newyorkcrime.find()

        nyc_subway_crime = []
        
        for s in subway:
            for c in crime:
                if 'latitude' and 'longitude' in c:
                    location_s = (float(s['the_geom']['coordinates'][1]), float(s['the_geom']['coordinates'][0]))
                    la = c['latitude']
                    lo = c['longitude']
                    location_c = (float(la),float(lo))
                    dis = vincenty(location_s,location_c,miles=True)
                    if dis < 1.0:
                        nyc_subway_crime.append({'subway_name':s['name'],'crime':c['ofns_desc'],'distance':dis})
        repo.dropCollection("NewYorkSubway_Crime")
        repo.createCollection("NewYorkSubway_Crime")
        repo["aolzh.NewYorkSubway_Crime"].insert_many(nyc_subway_crime)
        print("Finished")

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
        repo.authenticate('aolzh', 'aolzh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('nyc', 'https://data.cityofnewyork.us/resource/')

        subway_crime_script = doc.agent('alg:aolzh#subway_crime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        get_newyork_subway_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        newyorksubway = doc.entity('dat:aolzh#NewYorkSubway', {prov.model.PROV_LABEL:'NewYork Subways', prov.model.PROV_TYPE:'ont:DataSet'})
        newyorkcrime = doc.entity('dat:aolzh#NewYorkCrime', {prov.model.PROV_LABEL:'NewYork Crime', prov.model.PROV_TYPE:'ont:DataSet'})
        newyork_subway_crime = doc.entity('dat:aolzh#NewYorkSubway_Crime', {prov.model.PROV_LABEL:'NewYork Subway Crime', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAssociatedWith(get_newyork_subway_crime, subway_crime_script)
        doc.usage(get_newyork_subway_crime, newyorksubway, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        doc.usage(get_newyork_subway_crime, newyorkcrime, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        
        doc.wasAttributedTo(newyork_subway_crime, subway_crime_script)
        doc.wasGeneratedBy(newyork_subway_crime, get_newyork_subway_crime, endTime)
        doc.wasDerivedFrom(newyork_subway_crime, newyorkcrime,get_newyork_subway_crime, get_newyork_subway_crime, get_newyork_subway_crime)
        doc.wasDerivedFrom(newyork_subway_crime, newyorksubway,get_newyork_subway_crime, get_newyork_subway_crime, get_newyork_subway_crime)

       

        repo.logout()
                  
        return doc

subway_crime.execute()
doc = subway_crime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
