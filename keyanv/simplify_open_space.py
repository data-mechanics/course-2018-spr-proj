import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math

class simplify_open_space(dml.Algorithm):
    contributor = 'keyanv'
    reads = ['keyanv.open_spaces']
    writes = ['keyanv.open_spaces_simplified']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')


        open_spaces = repo['keyanv.open_spaces']

        # my code to simplify the open_spaces data by projecting attributes out
        # and transforming the polygonal geojson data using other information to create a
        # rougher, yet simpler representation of the location of the open spaces
        simple_list = []
        for open_space in open_spaces.find():
            # remove open spaces that aren't publically owned by the City of Boston
            if open_space['properties']['OWNERSHIP'] == "City of Boston":
                # calculate the central point of the coordinates
                avg_lat = 0
                avg_long = 0
                count = 0
                for coordinate in open_space['geometry']['coordinates'][0]:
                    if type(coordinate[0]) == list:
                        avg_long = coordinate[0]
                        avg_lat = coordinate[1]
                        radius = open_space['properties']['ShapeSTArea']
                        break

                    else:
                        avg_long += float(coordinate[0])
                        avg_lat += float(coordinate[1])
                        count += 1

                if count != 0:
                    # get the average latitude and longitude
                    avg_lat /= count
                    avg_long /= count

                    # use the count, assume the polygon is a regular n-gon
                    # and find the circumradius of the polygon to get an approximate
                    # sense of how large the space is

                    # Area of regular n-gon in terms of circumradius b is:
                    # A = 1/2(n)(b^2)(sin(2*pi/n)), we can isolate the circumradius b to get
                    # b = sqrt(A*2/n/sin(2*pi/n))
                    radius = math.sqrt(float(open_space['properties']['ShapeSTArea'])*2/count/math.sin(2*math.pi/count))
                    radius = str(radius)

                simple_list.append({
                    'latitude': avg_lat,
                    'longitude': avg_long,
                    'name': open_space['properties']['SITE_NAME'],
                    'approx_radius': radius
                    })

        repo.dropCollection("open_spaces_simplified")
        repo.createCollection("open_spaces_simplified")
        repo['keyanv.open_spaces_simplified'].insert_many(simple_list)
        repo['keyanv.open_spaces_simplified'].metadata({'complete':True})
        print(repo['keyanv.open_spaces_simplified'].metadata())

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
        repo.authenticate('keyanv', 'keyanv')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:keyanv#simplify_open_space', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:2868d370c55d4d458d4ae2224ef8cddd_7', {'prov:label':'Open Space Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_open_spaces_simplified = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
                                                 prov.model.PROV_LABEL: "Simplify the open space data",
                                                 prov.model.PROV_TYPE: 'ont:Computation')
        doc.wasAssociatedWith(get_open_spaces_simplified, this_script)
        doc.usage(get_open_spaces_simplified, resource, startTime)
        open_spaces_simplified = doc.entity('dat:keyanv#open_spaces_simplified', {prov.model.PROV_LABEL:'Simplified version of Open Space Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(open_spaces_simplified, this_script)
        doc.wasGeneratedBy(open_spaces_simplified, get_open_spaces_simplified, endTime)
        doc.wasDerivedFrom(open_spaces_simplified, resource, get_open_spaces_simplified, get_open_spaces_simplified, get_open_spaces_simplified)

        repo.logout()
                  
        return doc

simplify_open_space.execute()
doc = simplify_open_space.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof