# ####################### Just combining the Coordinates from two datasets; nothing fancy here #########################

import dml
import prov.model
import datetime
import json
import uuid


class Combine_Coordinates(dml.Algorithm):
    contributor = 'jhs2018_rpm1995'
    reads = ['jhs2018_rpm1995.greenobjects',
             'jhs2018_rpm1995.greenspaces']                  # its geographical coordinates
    writes = ['jhs2018_rpm1995.greenassets']

    @staticmethod
    def execute(trial=False):
        # Retrieve datasets
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jhs2018_rpm1995', 'jhs2018_rpm1995')

        print("Now running Combine_Coordinates.py")

        greenobjects = repo.jhs2018_rpm1995.greenobjects.find()
        greenspaces = repo.jhs2018_rpm1995.greenspaces.find()

        repo.dropCollection("greenassets")
        repo.createCollection("greenassets")
        repo['jhs2018_rpm1995.greenassets'].insert_many(greenobjects)
        repo['jhs2018_rpm1995.greenassets'].insert_many(greenspaces)

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

        this_script = doc.agent('alg:jhs2018_rpm1995#Combine_Coordinates',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

# #######
        resource_greenobjects = doc.entity('dat:jhs2018_rpm1995_greenobjects',
                                  {prov.model.PROV_LABEL: 'Coordinates of Environment Friendly Assets in Boston',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})

        resource_greenspaces = doc.entity('dat:jhs2018_rpm1995_greenspaces',
                                 {prov.model.PROV_LABEL: 'Coordinates of Open Spaces in Boston',
                                  prov.model.PROV_TYPE : 'ont:DataSet'})

        get_greenassets = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                          {
                                                    prov.model.PROV_LABEL: "Locations of Green Assets in Boston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(get_greenassets, this_script)

        doc.usage(get_greenassets, resource_greenobjects, startTime)
        doc.usage(get_greenassets, resource_greenspaces, startTime)

# #######
        greenassets = doc.entity('dat:jhs2018_rpm1995_greenassets',
                                 {prov.model.PROV_LABEL: 'Coordinates of All Environment Friendly Assets in Boston',
                                  prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(greenassets, this_script)
        doc.wasGeneratedBy(greenassets, get_greenassets, endTime)
        doc.wasDerivedFrom(greenassets, resource_greenobjects, get_greenassets, get_greenassets, get_greenassets)
        doc.wasDerivedFrom(greenassets, resource_greenspaces, get_greenassets, get_greenassets, get_greenassets)

        repo.logout()

        return doc


# Combine_Coordinates.execute()
# doc = Combine_Coordinates.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
