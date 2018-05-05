import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pprint


class combineneighborhoodpoverty(dml.Algorithm):
    contributor = 'colinstu'
    reads = ['yash.getPovertyRatesData', 'colinstu.neighborhood']
    writes = ['colinstu.combineneighborhoodpoverty']

    def merge(neighborhood, poverty):
        new_collection = []
        for row in neighborhood.find():
            new_dict = {}
            new_dict['city'] = row['properties']['Name']
            new_dict['geometry'] = row['geometry']
            poverty_row = poverty.find_one({'Region': new_dict['city']})
            if poverty_row:
                new_dict['poverty rate'] = poverty_row['Poverty rate']
                new_dict['percent of city impoverished'] = poverty_row["Percent of Boston's\nimpoverished"]
            else:
                new_dict['poverty rate'] = 'N/a'
                new_dict['percent of city impoverished'] = 'N/a'
            new_collection.append(new_dict.copy())
        return new_collection

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:colinstu#combineneighborhoodpoverty',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': 'Combine Neighborhoods & Poverty', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        getearningsbyzip = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(getearningsbyzip, this_script)
        doc.usage(getearningsbyzip, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   }
                  )

        earningsbyzip = doc.entity('dat:colinstu#combineneighborhoodpoverty',
                                   {prov.model.PROV_LABEL: 'Combine Neighborhoods & Poverty',
                                    prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(combineneighborhoodpoverty, this_script)
        doc.wasGeneratedBy(combineneighborhoodpoverty, combineneighborhoodpoverty, endTime)
        doc.wasDerivedFrom(combineneighborhoodpoverty, resource, combineneighborhoodpoverty, combineneighborhoodpoverty, combineneighborhoodpoverty)

        repo.logout()

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')
        repo.dropCollection("combineneighborhoodpoverty")
        repo.createCollection("combineneighborhoodpoverty")
        poverty = repo.yash.povertyRatesData
        neighborhood = repo.colinstu.neighborhood

        collection = combineneighborhoodpoverty.merge(neighborhood,poverty)
        repo['colinstu.combineneighborhoodpoverty'].insert_many(collection)
        repo['colinstu.combineneighborhoodpoverty'].metadata({'complete': True})
        print(repo['colinstu.combineneighborhoodpoverty'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


combineneighborhoodpoverty.execute()
#doc = combineneighborhoodpoverty.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

