
import json
import dml
import prov.model
import datetime
import uuid

class combine_public_utilities(dml.Algorithm):

    contributor = 'keyanv'
    reads = ['keyanv.open_spaces_simplified','keyanv.pools','keyanv.mbta_stops']
    writes = ['keyanv.public_utilities']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')

        # Get the collections
        mbta_stops = repo['keyanv.mbta_stops']
        open_spaces_simplified = repo['keyanv.open_spaces_simplified']
        pools = repo['keyanv.pools']

        # helper function for inserting into the final list
        def insert(utility_type, obj, keys=None):
            ret = {'type': utility_type}
            if utility_type == 'mbta_stop':
                for k in keys:
                    ret[k] = obj['attributes'][k]
                return ret
            elif utility_type == 'pool':
                ret['name'] = obj['properties']['SITE']
                ret['latitude'] = obj['geometry']['coordinates'][1]
                ret['longitude'] = obj['geometry']['coordinates'][0]
                return ret
            else:
                obj['type'] = utility_type
                return obj


        #Get names, neighborhood of all water play parks in Cambridge
        public_utilities_list = []
        for mbta_stop in mbta_stops.find():
            public_utilities_list.append(
                insert('mbta_stop', mbta_stop, ['name', 'longitude', 'latitude','wheelchair_boarding'])
            )

        for open_space in open_spaces_simplified.find():
            public_utilities_list.append(
                insert('open_space', open_space)
            )

        for pool in pools.find():
            public_utilities_list.append(
                insert('pool', pool, ['_'])
            )

        # Create a new collection and insert the result data set
        repo.dropCollection('public_utilities')
        repo.createCollection('public_utilities')
        repo['keyanv.public_utilities'].insert_many(public_utilities_list)


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('mbt', 'https://api-v3.mbta.com/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        this_script = doc.agent('alg:keyanv#combine_public_utilities', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        
        resource_mbta_stops = doc.entity('dat:keyanv#mbta_stops', {'prov:label': 'MBTA Stops', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        resource_open_spaces_simplified = doc.entity('dat:keyanv#open_spaces_simplified', {'prov:label': 'Open Spaces', prov.model.PROV_TYPE: 'ont:DataResource',  'ont:Extension': 'json'})
        resource_pools = doc.entity('dat:keyanv#pools', {'prov:label': 'Pools', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        combine_public_utilities = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, { prov.model.PROV_LABEL: "Combination of All Public Utilities", prov.model.PROV_TYPE: 'ont:Computation'})
        
        doc.wasAssociatedWith(combine_public_utilities, this_script)
        doc.usage(combine_public_utilities, resource_mbta_stops, startTime)
        doc.usage(combine_public_utilities, resource_open_spaces_simplified, startTime)
        doc.usage(combine_public_utilities, resource_pools, startTime)
        public_utilities = doc.entity('dat:keyanv#public_utilities', {prov.model.PROV_LABEL: 'All Public Utilities', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(public_utilities, this_script)
        doc.wasGeneratedBy(public_utilities, combine_public_utilities, endTime)
        doc.wasDerivedFrom(public_utilities, resource_mbta_stops, combine_public_utilities, combine_public_utilities, combine_public_utilities)
        doc.wasDerivedFrom(public_utilities, resource_open_spaces_simplified, combine_public_utilities, combine_public_utilities,  combine_public_utilities)
        doc.wasDerivedFrom(public_utilities, resource_pools, combine_public_utilities,  combine_public_utilities, combine_public_utilities)

        repo.logout()

        return doc


combine_public_utilities.execute()
doc = combine_public_utilities.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))