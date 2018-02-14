import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geojson


def fetch_json(url, item):
    print("Downloading " + str(item) + " Dataset from: " + str(url))
    response = urllib.request.urlopen(url).read().decode("utf-8")
    r = json.loads(response)
    return r


def fetch_geojson(url, item):
    print("Downloading " + str(item) + " Dataset from: " + str(url))
    response = urllib.request.urlopen(url).read().decode("utf-8")
    r = geojson.loads(response)
    rdict = dict(r)
    rlist = rdict['features']
    return rlist


class getdata(dml.Algorithm):
    contributor = 'rpm1995'
    reads = []
    writes = ['rpm1995.neighbourhoodnames',
              'rpm1995.neighbourhoodnonames',
              'rpm1995.hubway',
              'rpm1995.trees',
              'rpm1995.charge',
              'rpm1995.openspaces']

    @staticmethod
    def execute(trial=False):
        # Retrieve datasets
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rpm1995', 'rpm1995')

        # This fetches coordinates of Boston neighbourhoods but without the names
        r = fetch_json('https://boston.opendatasoft.com/explore/dataset/boston-neighborhoods/download/?format=json'
                       '&timezone=America/New_York', "Neighbourhoods")
        repo.dropCollection("neighbourhoodnonames")
        repo.createCollection("neighbourhoodnonames")
        repo['rpm1995.neighbourhoodnonames'].insert_many(r)

        # This fetches names of Boston Neighbourhoods but without the coordinates
        r = fetch_geojson('http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0'
                          '.geojson', "Neighbourhoods")
        repo.dropCollection("neighbourhoodnames")
        repo.createCollection("neighbourhoodnames")
        repo['rpm1995.neighbourhoodnames'].insert_many(r)

        # This fetches a dataset with details about Hubway stations in Boston
        r = fetch_geojson('http://bostonopendata-boston.opendata.arcgis.com/datasets'
                          '/ee7474e2a0aa45cbbdfe0b747a5eb032_0.geojson', "Hubway Stations")
        repo.dropCollection("hubway")
        repo.createCollection("hubway")
        repo['rpm1995.hubway'].insert_many(r)

        # This fetches a dataset with details about Trees in Boston
        r = fetch_geojson('http://datamechanics.io/data/Trees%20(1).geojson', "Trees")
        repo.dropCollection("trees")
        repo.createCollection("trees")
        repo['rpm1995.trees'].insert_many(r)

        # This fetches a dataset with details about Charging Stations in Boston
        r = fetch_json('https://boston.opendatasoft.com/explore/dataset/charging-stations/download/?format=json'
                       '&timezone=America/New_York', "Charging Stations")
        repo.dropCollection("charge")
        repo.createCollection("charge")
        repo['rpm1995.charge'].insert_many(r)

        # This fetches a dataset with details about Open Spaces in Boston
        r = fetch_geojson('http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7'
                          '.geojson', "Open Spaces")
        repo.dropCollection("openspaces")
        repo.createCollection("openspaces")
        repo['rpm1995.openspaces'].insert_many(r)
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
        doc.add_namespace('hub', 'http://bostonopendata-boston.opendata.arcgis.com/datasets')  # Boston Open Data
        doc.add_namespace('tree', 'http://datamechanics.io/data')
        doc.add_namespace('charge', 'http://bostonopendata-boston.opendata.arcgis.com/datasets')
        doc.add_namespace('openspace', 'http://bostonopendata-boston.opendata.arcgis.com/datasets')

        this_script = doc.agent('alg:rpm1995#getdata',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

# #######
        resource_neighbourhoodnonames = doc.entity('bwod: neighbourhood_no_names', {'prov:label': 'Boston '
                                                                                    'Neighbourhoods without Names',
                                                                                    prov.model.PROV_TYPE:
                                                                                        'ont:DataResource',
                                                                                    'ont:Extension': 'geojson'})
        get_neighbourhoodnonames = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_neighbourhoodnonames, this_script)
        doc.usage(get_neighbourhoodnonames, resource_neighbourhoodnonames, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        resource_neighbourhoodnames = doc.entity('ab: neighbourhood_names', {'prov:label': 'Boston Neighbourhood Names',
                                                                             prov.model.PROV_TYPE: 'ont:DataResource',
                                                                             'ont:Extension': 'json'})
        get_neighbourhoodnames = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_neighbourhoodnames, this_script)
        doc.usage(get_neighbourhoodnames, resource_neighbourhoodnames, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        resource_hubway = doc.entity('hub: geojson', {'prov:label': 'Hubway stations in Boston',
                                                      prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension':
                                                          'geojson'})
        get_hubway = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hubway, this_script)
        doc.usage(get_hubway, resource_hubway, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        resource_trees = doc.entity('tree: geojson', {'prov:label': 'Trees in Boston',
                                                  prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'geojson'})
        get_trees = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_trees, this_script)
        doc.usage(get_trees, resource_trees, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        resource_charge = doc.entity('charge: json', {'prov:label': 'Charging Stations in Boston',
                                                      prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension':
                                                      'json'})
        get_charge = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_charge, this_script)
        doc.usage(get_charge, resource_charge, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        resource_openspaces = doc.entity('openspace: geojson', {'prov:label': 'Open Spaces in Boston',
                                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                                'ont:Extension':
                                                                'geojson'})
        get_openspaces = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_openspaces, this_script)
        doc.usage(get_openspaces, resource_openspaces, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
# #######
        neighbourhoodnonames = doc.entity('dat:rpm1995neighbourhoodnonames',
                                          {prov.model.PROV_LABEL: 'neighbourhoodnonames',
                                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(neighbourhoodnonames, this_script)
        doc.wasGeneratedBy(neighbourhoodnonames, get_neighbourhoodnonames, endTime)
        doc.wasDerivedFrom(neighbourhoodnonames, resource_neighbourhoodnonames, get_neighbourhoodnonames,
                           get_neighbourhoodnonames, get_neighbourhoodnonames)

        neighbourhoodnames = doc.entity('dat:rpm1995neighbourhoodnames',
                                        {prov.model.PROV_LABEL: 'neighbourhoodnames',
                                         prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(neighbourhoodnames, this_script)
        doc.wasGeneratedBy(neighbourhoodnames, get_neighbourhoodnames, endTime)
        doc.wasDerivedFrom(neighbourhoodnames, resource_neighbourhoodnames, get_neighbourhoodnames,
                           get_neighbourhoodnames, get_neighbourhoodnonames)

        hubway = doc.entity('dat:rpm1995hubway', {prov.model.PROV_LABEL: 'hubway', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(hubway, this_script)
        doc.wasGeneratedBy(hubway, get_hubway, endTime)
        doc.wasDerivedFrom(hubway, resource_hubway, get_hubway, get_hubway, get_hubway)

        trees = doc.entity('dat:rpm1995trees', {prov.model.PROV_LABEL: 'trees', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(trees, this_script)
        doc.wasGeneratedBy(trees, get_trees, endTime)
        doc.wasDerivedFrom(trees, resource_trees, get_trees, get_trees, get_trees)

        charge = doc.entity('dat:rpm1995charge',
                            {prov.model.PROV_LABEL: 'charge', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(charge, this_script)
        doc.wasGeneratedBy(charge, get_charge, endTime)
        doc.wasDerivedFrom(charge, resource_charge, get_charge, get_charge, get_charge)

        openspaces = doc.entity('dat:rpm1995openspaces',
                                {prov.model.PROV_LABEL: 'openspaces', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(openspaces, this_script)
        doc.wasGeneratedBy(openspaces, get_openspaces, endTime)
        doc.wasDerivedFrom(openspaces, resource_openspaces, get_openspaces, get_openspaces, get_openspaces)

        repo.logout()

        return doc


# getdata.execute()
# doc = getdata.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
