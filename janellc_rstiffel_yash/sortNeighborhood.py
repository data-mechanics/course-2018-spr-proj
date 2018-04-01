import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import shapely 
from shapely.geometry import shape, Point
"""
Finds average point (lat, long) for each street in each district where crimes existed.
This is for finding the "middle" of the street - used in findCrimeStats.

Yields the form {DISTRICT: {"street1": {Lat: #, Long: #}, "street2": {Lat: #, Long: #} ...}, DISTRICT2:...}

- Filters out empty entries

"""

def merge_dicts(x, y):
    ''' Helper function to merge 2 dictionaries '''
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z


class sortNeighborhoods(dml.Algorithm):
    contributor = 'janellc_rstiffel_yash'
    reads = ['janellc_rstiffel_yash.neighborhoods']
    writes = ['janellc_rstiffel_yash.idk___']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')

        # Get Crime data and neighborhood data
        crimesData = list(repo.janellc_rstiffel_yash.crimesData.find())
        neighborhoods = list(repo['janellc_rstiffel_yash.neighborhoods'].find())

        n_count = {}
        for polygon in neighborhoods:
            figure = shape(polygon['Polygon'])
            for crime in crimesData:
                # Filters out empty rows
                if (crime['Long'] == None or crime['Lat'] == None):
                    continue
                point = Point(float(crime['Long']), float(crime['Lat']))
                if figure.contains(point):
                    N = polygon['Neighborhood']
                    if N not in n_count:
                        n_count[N] = {'crimes': 1}
                    else:
                        n_count[N]['crimes'] += 1

        print(n_count)

        # Store in DB
        repo.dropCollection("sortedNeighborhoods")
        repo.createCollection("sortedNeighborhoods")

        for key,value in n_count.items():
             r = {key:value}
             repo['janellc_rstiffel_yash.sortedNeighborhoods'].insert(r)
        repo['janellc_rstiffel_yash.sortedNeighborhoods'].metadata({'complete':True})
        print(repo['janellc_rstiffel_yash.sortedNeighborhoods'].metadata())


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
        repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        
        # Agent, entity, activity
        this_script = doc.agent('alg:janellc_rstiffel_yash#transformCrimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # Resource = crimesData
        resource1 = doc.entity('dat:janellc_rstiffel_yash#crimesData', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        #Activity
        transform_crimes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transform_crimes, this_script)

        doc.usage(transform_crimes, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )


        crimesDist = doc.entity('dat:janellc_rstiffel_yash#crimesDistrict', {prov.model.PROV_LABEL:'Avg Loc Streets per District', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimesDist, this_script)
        doc.wasGeneratedBy(crimesDist, transform_crimes, endTime)
        doc.wasDerivedFrom(crimesDist, resource1, transform_crimes, transform_crimes, transform_crimes)

        repo.logout()
                  
        return doc

sortNeighborhoods.execute()
#doc = transformCrimesData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
