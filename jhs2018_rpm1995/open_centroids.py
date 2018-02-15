import dml
import prov.model
import datetime
import uuid
from shapely.geometry import Polygon


class open_centroids(dml.Algorithm):    # We are figuring out the coordinates of the centroid of every open space in
    contributor = 'jhs2018_rpm1995'             # Boston and adding this to the dataset greenspaces
    reads = ['jhs2018_rpm1995.openspaces']
    writes = ['jhs2018_rpm1995.greenspaces']

    @staticmethod
    def extract(cursor, megalist):

        for elements in cursor:         # This function figures out the centroid from the a lsit of coordinates we are
            coordinates = elements['geometry']['coordinates']   # extracting from the openspaces dataset
            coordinates = coordinates[0]
            try:
                polygon = Polygon(coordinates).centroid
                xcoord = polygon.coords.xy[0][0]
                ycoord = polygon.coords.xy[1][0]
                final = [xcoord, ycoord]
                megalist.append({"Type": "openspace", "Location": final})
            except ValueError:
                coordinates = coordinates[0]
                polygon = Polygon(coordinates).centroid
                xcoord = polygon.coords.xy[0][0]
                ycoord = polygon.coords.xy[1][0]
                final = [xcoord, ycoord]
                megalist.append({"Type": "openspace", "Location": final})
            except AssertionError:
                coordinates = coordinates[0]
                polygon = Polygon(coordinates).centroid
                xcoord = polygon.coords.xy[0][0]
                ycoord = polygon.coords.xy[1][0]
                final = [xcoord, ycoord]
                megalist.append({"Type": "openspace", "Location": final})
        return megalist

    @staticmethod
    def execute(trial=False):
        # Retrieve datasets
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jhs2018_rpm1995', 'jhs2018_rpm1995')

        print("Now running open_centroids.py")

        objects = []

        openspaces = repo.jhs2018_rpm1995.openspaces.find()

        objects = open_centroids.extract(openspaces, objects)

        repo.dropCollection("greenspaces")
        repo.createCollection("greenspaces")
        repo['jhs2018_rpm1995.greenspaces'].insert_many(objects)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):

            # Create the provenance document describing everything happening
            # in this script. Each run of the script will generate a new
            # document describing that invocation event.

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jhs2018_rpm1995', 'jhs2018_rpm1995')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet',
        # 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bwod', 'https://boston.opendatasoft.com/explore/dataset/boston-neighborhoods/')  # Boston
        # Wicked Open Data
        doc.add_namespace('ab', 'https://data.boston.gov/dataset/boston-neighborhoods')   # Analyze Boston

        this_script = doc.agent('alg:jhs2018_rpm1995#open_centroids',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

# #######
        resource_openspaces = doc.entity('bwod: openspaces', {'prov:label': 'Boston Hubway Stations',
                                                             prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension':
                                                             'geojson'})

        get_openspaces = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                      {
                                                    prov.model.PROV_LABEL: "Collecting coordinates of Open Spaces in "
                                                                           "Boston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(get_openspaces, this_script)

        doc.usage(get_openspaces, resource_openspaces, startTime)

# #######
        greenobjects = doc.entity('dat:jhs2018_rpm1995_centroids_openspaces',
                                  {prov.model.PROV_LABEL: 'Coordinates of centroids of open spaces in Boston',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(greenobjects, this_script)
        doc.wasGeneratedBy(greenobjects, get_openspaces, endTime)
        doc.wasDerivedFrom(greenobjects, resource_openspaces, get_openspaces, get_openspaces,
                           get_openspaces)

        repo.logout()

        return doc


# combineneighbourhood.execute()
# doc = combineneighbourhood.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
