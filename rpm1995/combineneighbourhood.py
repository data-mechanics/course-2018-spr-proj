import dml
import prov.model
import datetime
import uuid


class combineneighbourhood(dml.Algorithm):
    contributor = 'rpm1995'
    reads = ['rpm1995.neighbourhoodnames',
             'rpm1995.neighbourhoodnonames']
    writes = ['rpm1995.neighbourhoods']

    @staticmethod
    def execute(trial=False):
        # Retrieve datasets
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rpm1995', 'rpm1995')

        print("Now running combineneighbourhood.py")
        # Collecting collections

        nonames = repo.rpm1995.neighbourhoodnonames.find()
        # names = repo.rpm1995.neighbourhoodnames.find()

        cleanneighbourhoods = []             # Will insert this into our new collection

        for tosearch in nonames:
            id = tosearch['fields']['objectid']
            names = repo.rpm1995.neighbourhoodnames.find()
            for searchhere in names:
                if id == searchhere['properties']['OBJECTID']:
                    cleanneighbourhoods.append({
                        "Name": searchhere['properties']['Name'], "Details": tosearch['fields']['geo_shape'][
                            'coordinates'][0]
                    })
                    break

        repo.dropCollection("neighbourhoods")
        repo.createCollection("neighbourhoods")

        repo['rpm1995.neighbourhoods'].insert_many(cleanneighbourhoods)

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

        this_script = doc.agent('alg:rpm1995#combineneighbourhood',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

# #######
        resource_neighbourhoodnonames = doc.entity('bwod: neighbourhoods_no_names', {'prov:label': 'Boston '
                                                                                                 'Neighbourhoods '
                                                                                           'without Names',
                                                                             prov.model.PROV_TYPE: 'ont:DataResource',
                                                                             'ont:Extension': 'geojson'})

        resource_neighbourhoodnames = doc.entity('ab: neighbourhood_names', {'prov:label': 'Boston '
                                                                                            'Neighbourhood Names',
                                                                                            prov.model.PROV_TYPE:
                                                                                            'ont:DataResource',
                                                                                            'ont:Extension': 'json'})

        neighbourhoodfinal = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                          {
                                                    prov.model.PROV_LABEL: "Combine Boston neighbourhood names with "
                                                                           "Locations",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(neighbourhoodfinal, this_script)

        doc.usage(neighbourhoodfinal, resource_neighbourhoodnonames, startTime)
        doc.usage(neighbourhoodfinal, resource_neighbourhoodnames, startTime)

# #######
        neighbourhoods = doc.entity('dat:rpm1995neighbourhoods',
                                    {prov.model.PROV_LABEL: 'Neighbourhoods in Boston',
                                     prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(neighbourhoods, this_script)
        doc.wasGeneratedBy(neighbourhoods, neighbourhoodfinal, endTime)
        doc.wasDerivedFrom(neighbourhoods, resource_neighbourhoodnonames, neighbourhoodfinal, neighbourhoodfinal,
                           neighbourhoodfinal)
        doc.wasDerivedFrom(neighbourhoods, resource_neighbourhoodnames, neighbourhoodfinal, neighbourhoodfinal,
                           neighbourhoodfinal)

        repo.logout()

        return doc


# combineneighbourhood.execute()
# doc = combineneighbourhood.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
