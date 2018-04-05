import dml
import prov.model
import datetime
import uuid

class stability_score(dml.Algorithm):

    contributor = 'agoncharova_lmckone'
    reads = ['agoncharova_lmckone.boston_tract_counts']
    writes = ['agoncharova_lmckone.stability_score']

    @staticmethod
    def execute(trial=False):
        '''
        Determines a 'housing stability' score for each census tract in Boston based on number of evictions, number of crimes,
        income, and number of businesses.


        Code adapted from https://github.com/Data-Mechanics/course-2017-fal-proj/blob/master/jdbrawn_jliang24_slarbi_tpotye/safetyScore.py
        '''

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

        tract_counts = repo['agoncharova_lmckone.boston_tract_counts']

        score = []
        minCrimes = 99999
        maxCrimes = 0
        minEvictions = 99999
        maxEvictions = 0

        # find mins and maxes
        for entry in tract_counts.find():
            if float(entry['properties']['evictions']) < minEvictions:
                minCrimes = entry['properties']['evictions']
            if float(entry['properties']['evictions']) > maxEvictions:
                maxCrimes = entry['properties']['evictions']
            if float(entry['properties']['crimes']) < minCrimes:
                minCrimes = entry['properties']['crimes']
            if float(entry['properties']['crimes']) > maxCrimes:
                maxCrimes = entry['properties']['crimes']

        eviction_max_minus_min = float(maxEvictions - minEvictions)
        crime_max_minus_min = float(maxCrimes - minCrimes)

        # calculate score
        for entry in tract_counts.find():

            evictionScore = float(entry['properties']['evictions'] - minEvictions) / eviction_max_minus_min
            crimeScore = float(entry['properties']['crimes'] - minCrimes) / crime_max_minus_min

            ## take a look at coefficients used by Desmond in his paper to maybe weight these differently
            stabilityScore = (evictionScore + crimeScore) / 2.0
            # print("entry stabilityScore " + str(stabilityScore) + " crimes " + str(entry['properties']['crimes']) + " eviction " + str(entry['properties']['evictions']))
            score.append({
                'Tract': entry['properties']['GEOID'],
                'stability': stabilityScore,
                'evictionScore': evictionScore,
                'crimeScore': crimeScore,
                'businesses': entry['properties']['businesses']
                })

        repo.dropCollection('stability_score')
        repo.createCollection('stability_score')
        repo['agoncharova_lmckone.stability_score'].insert_many(score)

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        """
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        """

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:agoncharova_lmckone#stability_score',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_stability = doc.entity('dat:agoncharova_lmckone#boston_tract_counts',
                                       {'prov:label': 'Stability Analysis by Census Tract',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        get_stability_score = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_stability_score, this_script)

        doc.usage(get_stability_score, resource_stability, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        stability_score = doc.entity('dat:agoncharova_lmckone#stability_score',
                            {prov.model.PROV_LABEL: 'Stability Score', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(stability_score, this_script)
        doc.wasGeneratedBy(stability_score, get_stability_score, endTime)
        doc.wasDerivedFrom(stability_score, resource_stability, get_stability_score, get_stability_score, get_stability_score)

        repo.logout()

        return doc

# stability_score.execute()
# stability_score.provenance()
