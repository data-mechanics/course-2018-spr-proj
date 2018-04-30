import dml
import prov.model
import numpy as np
import scipy

class calcFloodCor(dml.Algorithm):
    contributor = 'jlove'
    reads = ['jlove.incomeNormalized', 'jlove.percentCovered']
    writes = ['jlove.floodCor', 'jlove.floodPairs']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')

        percent_covered = repo['jlove.percentCovered'].find_one({})
        incomes = repo['jlove.incomeNormalized'].find({})

        pairs = []
        save_pairs = {}

        for income in incomes:
            if income['Neighborhood'] in percent_covered:
                covered = percent_covered[income['Neighborhood']]
                spIncome = income['normalized']
                save_pairs[income['Neighborhood']] = {'covered': covered, 'income': spIncome}
                pairs += [[covered, spIncome]]

        repo.dropCollection('jlove.floodPairs')
        repo.createCollection('jlove.floodPairs')

        repo['jlove.floodPairs'].insert_one(save_pairs)

        pair_arr = np.array(pairs)
        results = scipy.stats.pearsonr(pair_arr[::, 0], pair_arr[::, 1])

        repo.dropCollection("floodCor")
        repo.createCollection("floodCor")

        doc = {'corr_cooef': results[0], 'p_val': results[1]}

        repo['jlove.stats_result'].insert_one(doc)

        repo.logout()

        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/dataset/')

        this_script = doc.agent('alg:jlove#findEvacLocations',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dat:jlove#hydrants',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'geojson'})
        evac_loc = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(evac_loc, this_script)
        doc.usage(this_script, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )

        evacLoc = doc.entity('dat:jlove#evacloc',
                             {prov.model.PROV_LABEL: 'Tentative Locations For Boston Flood Evacuation Centers',
                              prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(evacLoc, this_script)
        doc.wasGeneratedBy(evacLoc, evac_loc, endTime)
        doc.wasDerivedFrom(evacLoc, resource, evac_loc, evac_loc, evac_loc)

        repo.logout()

        return doc
