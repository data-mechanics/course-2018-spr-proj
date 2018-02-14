import dml
import prov.model
import datetime
import uuid


class project_coordinates(dml.Algorithm):
    contributor = 'rpm1995'
    reads = ['rpm1995.hubway',                  # We will combine 3 datasets into dataset greenobjects, which
             'rpm1995.trees',                   # will have just the type of objects (tree, charging station, etc.) and
             'rpm1995.charge']                  # its geographical coordinates
    writes = ['rpm1995.greenobjects']

    @staticmethod
    def extract(cursor, type, megalist):        # This function extracts the type of object we are looking at (eg: tree)
                                                # and its coordinates, and adds them to the list that we will write
        for elements in cursor:                 # into dataset "greenobjects"
            megalist.append({"Type": type, "Location": elements['geometry']['coordinates']})
        return megalist

    @staticmethod
    def execute(trial=False):
        # Retrieve datasets
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rpm1995', 'rpm1995')

        print("Now running project_coordinates.py")

        objects = []

        hubway = repo.rpm1995.hubway.find()
        trees = repo.rpm1995.trees.find()
        charge = repo.rpm1995.charge.find()
        # openspaces = repo.rpm1995.openspaces.find()

        objects = project_coordinates.extract(hubway, "hubway", objects)
        objects = project_coordinates.extract(trees, "tree", objects)
        objects = project_coordinates.extract(charge, "charge", objects)

        repo.dropCollection("greenobjects")
        repo.createCollection("greenobjects")
        repo['rpm1995.greenobjects'].insert_many(objects)

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
        repo.authenticate('rpm1995', 'rpm1995')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet',
        # 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bwod', 'https://boston.opendatasoft.com/explore/dataset/boston-neighborhoods/')  # Boston
        # Wicked Open Data
        doc.add_namespace('ab', 'https://data.boston.gov/dataset/boston-neighborhoods')   # Analyze Boston

        this_script = doc.agent('alg:rpm1995#project_coordinates',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

# #######
        resource_hubway = doc.entity('bwod: hubstations', {'prov:label': 'Boston Hubway Stations',
                                                           prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension':
                                                           'geojson'})

        resource_trees = doc.entity('alg: trees', {'prov:label': 'Trees in Boston', prov.model.PROV_TYPE:
                                                   'ont:DataResource', 'ont:Extension': 'json'})

        resource_charges = doc.entity('bwod: charging', {'prov:label': 'Charging Stations in Boston',
                                                         prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension':
                                                         'json'})

        get_greenobjects = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                          {
                                                    prov.model.PROV_LABEL: "Locations of Hubway, Charging Stations "
                                                                           "and Trees in Boston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(get_greenobjects, this_script)

        doc.usage(get_greenobjects, resource_hubway, startTime)
        doc.usage(get_greenobjects, resource_charges, startTime)
        doc.usage(get_greenobjects, resource_trees, startTime)

# #######
        greenobjects = doc.entity('dat:rpm1995_greenobjects',
                                  {prov.model.PROV_LABEL: 'Coordinates of Environment Friendly Assets in Boston',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(greenobjects, this_script)
        doc.wasGeneratedBy(greenobjects, get_greenobjects, endTime)
        doc.wasDerivedFrom(greenobjects, resource_hubway, get_greenobjects, get_greenobjects,
                           get_greenobjects)
        doc.wasDerivedFrom(greenobjects, resource_trees, get_greenobjects, get_greenobjects,
                           get_greenobjects)
        doc.wasDerivedFrom(greenobjects, resource_charges, get_greenobjects, get_greenobjects,
                           get_greenobjects)

        repo.logout()

        return doc


# combineneighbourhood.execute()
# doc = combineneighbourhood.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
