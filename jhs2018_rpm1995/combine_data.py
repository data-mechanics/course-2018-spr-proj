import dml
import prov.model
import datetime
import uuid
import json
from shapely.geometry import Point, Polygon


class combine_data(dml.Algorithm):
    contributor = 'jhs2018_rpm1995'
    reads = ['jhs2018_rpm1995.neighbourhoods',
             'jhs2018_rpm1995.trees',
             'jhs2018_rpm1995.charge',
             'jhs2018_rpm1995.greenspaces',
             'jhs2018_rpm1995.hubway']
    writes = ['jhs2018_rpm1995.greenneighbourhoods']

    @staticmethod
    def extract(cursor, areacoords):            # This function returns coordinates in database referred to by "cursor"
                                                # that are present within "areacoords"
        container = []
        for i in cursor:
            coords = i['geometry']['coordinates']
            if areacoords.contains(Point(coords)):
                container.append(coords)
        return container

    @staticmethod
    def execute(trial=False):
        # Retrieve datasets
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jhs2018_rpm1995', 'jhs2018_rpm1995')

        print("Now running combine_data.py")

        # insert_me = []

        neighbourhoods = repo.jhs2018_rpm1995.neighbourhoods.find()
        ultimate = []

        for i in neighbourhoods:
            areaname = i['Name']
            try:
                areacoords = Polygon(i['Details'])
            except ValueError:
                areacoords = Polygon(i['Details'][0])
            except AssertionError:
                areacoords = Polygon(i['Details'][0])

            greenspace = repo.jhs2018_rpm1995.greenspaces.find()        # Combining neighbourhoods and open spaces
            coordbit = []
            for j in greenspace:
                type = j['Type']
                coords = j['Location']
                if areacoords.contains(Point(coords)):
                    coordbit.append(coords)

            hubwayobject = repo.jhs2018_rpm1995.hubway.find()           # Combining neighbourhoods and Hubway Stations
            type1 = "Hubway"
            hubwaycoord = combine_data.extract(hubwayobject, areacoords)
            # for k in greenobject:
            #     type1 = "Hubway"
            #     coords = k['geometry']['coordinates']
            #     if areacoords.contains(Point(coords)):
            #         greencoord.append(coords)
            # ultimate.append({"Name": areaname, "Details": [{"Type": type, "Coordinates": coordbit}, {"Type": type1,
            #                                                                                          "Coordinates":
            #                                                                                              greencoord}]})

            chargeobject = repo.jhs2018_rpm1995.charges.find()          # Combining neighbourhoods and Charging Stations
            type2 = "Charge"
            chargecoord = combine_data.extract(chargeobject, areacoords)

            treeobject = repo.jhs2018_rpm1995.trees.find()              # Combining neighbourhoods and Trees
            type3 = "Tree"
            treecoord = combine_data.extract(treeobject, areacoords)

            ultimate.append({"Name": areaname, "Details": [{"Type": type, "Coordinates": coordbit}, {"Type": type1,
                                                                                                     "Coordinates":
                                                                                                         hubwaycoord},
                                                           {"Type": type2, "Coordinates": chargecoord},
                                                           {"Type": type3, "Coordinates": treecoord}]})

        repo.dropCollection("greenneighbourhoods")
        repo.createCollection("greenneighbourhoods")
        repo['jhs2018_rpm1995.greenneighbourhoods'].insert_many(ultimate)

        repo.logout()

        endTime = datetime.datetime.now()

        print(ultimate)

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

        this_script = doc.agent('alg:jhs2018_rpm1995#combine_data',
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

        resource_openspaces = doc.entity('bwod: open spaces', {'prov:label': 'Open Spaces in Boston',
                                                               prov.model.PROV_TYPE: 'ont:DataResource',
                                                               'ont:Extension': 'json'})

        resource_neighbourhoods = doc.entity('bwod: jhs2018_rpm1995neighbourhoods', {'prov:label': 'Boston Neighbourhoods',
                                                                             prov.model.PROV_TYPE: 'ont:DataResource',
                                                                             'ont:Extension': 'json'})

        get_details = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                   {prov.model.PROV_LABEL: "Coordinates of Environmentally Friendly Objects in "
                                                           "Boston Neighbourhoods", prov.model.PROV_TYPE:
                                       'ont:Computation'})

        doc.wasAssociatedWith(get_details, this_script)

        doc.usage(get_details, resource_hubway, startTime)
        doc.usage(get_details, resource_charges, startTime)
        doc.usage(get_details, resource_trees, startTime)
        doc.usage(get_details, resource_openspaces, startTime)
        doc.usage(get_details, resource_neighbourhoods, startTime)
# #######
        greenneighbourhoods = doc.entity('dat:jhs2018_rpm1995_greenneighbourhoods',
                                         {prov.model.PROV_LABEL: 'Environmentally Friendly Assets in Boston '
                                                                 'Neighbourhoods', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(greenneighbourhoods, this_script)
        doc.wasGeneratedBy(greenneighbourhoods, get_details, endTime)
        doc.wasDerivedFrom(greenneighbourhoods, resource_hubway, get_details, get_details,
                           get_details)
        doc.wasDerivedFrom(greenneighbourhoods, resource_trees, get_details, get_details,
                           get_details)
        doc.wasDerivedFrom(greenneighbourhoods, resource_charges, get_details, get_details,
                           get_details)
        doc.wasDerivedFrom(greenneighbourhoods, resource_openspaces, get_details, get_details,
                           get_details)
        doc.wasDerivedFrom(greenneighbourhoods, resource_neighbourhoods, get_details, get_details,
                           get_details)

        repo.logout()

        return doc


# combine_data.execute()
# doc = combine_data.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
