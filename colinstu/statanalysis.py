import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import shapely.geometry as sh
import scipy.stats
import pprint as pp

class statAnalysis(dml.Algorithm):
    contributor = 'colinstu'
    reads = ['colinstu.grocerygoogleplaces','colinstu.combineneighborhoodpoverty']
    writes = ['colinstu.statanalysis']

    def count_groceries(grocery_data, neighborhood_data):
        new_dict = {}
        for row in grocery_data.find():
            grocery_point = sh.Point(row['geometry']['location']['lng'],row['geometry']['location']['lat'])
            for row in neighborhood_data.find():
                neighborhood_shape = sh.shape(row['geometry'])
                if neighborhood_shape.contains(grocery_point):
                    if not row['city'] in new_dict:
                        new_dict[row['city']] = 1
                    else:
                        new_dict[row['city']] += 1
        return new_dict

    def corr_data(grocery,neighborhood,trial):
        '''
        Prepares data for correlation coefficient analysis; x-axis is percent of city's impoverished population living
        in that neighborhood and the y-axis is the number of grocery stores in that neighborhood; returns new collection
        containing correlation coefficient and p-value for relationship between percent of city's impoverished living in
        a neighborhood and the number of grocery stores in that neighborhood
        '''
        new_coll = []
        grocery_count = statAnalysis.count_groceries(grocery,neighborhood)
        if trial:
            n = neighborhood.find()[:3]
        else:
            n = neighborhood.find()
        for row in n:
            new_dict = {}
            new_dict['neighborhood'] = row['city']
            if row['percent of city impoverished'] == 'N/a':
                impoverished = 0
            else:
                impoverished = float("{0:.3f}".format(float(row['percent of city impoverished'][:-1]) / 100))
            new_dict['percent impoverished'] = impoverished
            if grocery_count.get(new_dict['neighborhood']) is None:
                new_dict['grocery count'] = 0
            else:
                new_dict['grocery count'] = grocery_count[new_dict['neighborhood']]
            new_coll.append(new_dict.copy())
        x = [row['percent impoverished'] for row in new_coll]
        y = [row['grocery count'] for row in new_coll]
        corr_coef, p_value = scipy.stats.pearsonr(x, y)
        stat_dict = {}
        stat_dict['correlation coefficient'] = corr_coef
        stat_dict['p value'] = p_value
        stat_coll = []
        stat_coll.append(stat_dict.copy())
        return stat_coll






    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')

        repo.dropCollection("statanalysis")
        repo.createCollection("statanalysis")

        grocery = repo.colinstu.grocerygoogleplaces
        neighborhood = repo.colinstu.combineneighborhoodpoverty

        r = statAnalysis.corr_data(grocery, neighborhood, trial)

        repo['colinstu.statanalysis'].insert_many(r)
        repo['colinstu.statanalysis'].metadata({'complete': True})
        print(repo['colinstu.statanalysis'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

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
        doc.add_namespace('gdp', 'https://maps.googleapis.com/maps/api/place/textsearch/')

        this_script = doc.agent('alg:colinstu#statanalysis',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('gdp:wc8w-nujj',
                              {'prov:label': 'Statistical Analysis', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        statanalysis = doc.entity('dat:colinstu#statanalysis',
                                       {prov.model.PROV_LABEL: 'Combined Boston and SF Permit data',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        getstatanalysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getstatanalysis, this_script)
        doc.usage(getstatanalysis, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        getgrocerygoogleplaces = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getgrocerygoogleplaces, this_script)
        doc.usage(getgrocerygoogleplaces, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        getcombineneighborhoodpoverty = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getcombineneighborhoodpoverty, this_script)
        doc.usage(getcombineneighborhoodpoverty, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        doc.wasAttributedTo(statanalysis, this_script)
        doc.wasGeneratedBy(getstatanalysis, getgrocerygoogleplaces, endTime)
        doc.wasDerivedFrom(statanalysis, resource, getgrocerygoogleplaces, getgrocerygoogleplaces, getgrocerygoogleplaces)
        doc.wasDerivedFrom(statanalysis, resource, getcombineneighborhoodpoverty, getcombineneighborhoodpoverty, getcombineneighborhoodpoverty)
        repo.logout()

        return doc


statAnalysis.execute()
doc = statAnalysis.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
