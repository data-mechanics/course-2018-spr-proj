import json
import dml
import uuid
import prov.model
from datetime import datetime
from random import shuffle
from math import sqrt
from geopy.distance import vincenty
import scipy.stats

def join(S, R, s_prefix, r_prefix, s_idx, mcol_s, mcol_r, mcol_new):
    result = []
    for obj_s in S:
        for obj_r in R:
            if obj_s[mcol_s] == obj_r[mcol_r]:
                dicttmp = {"_id": obj_s[s_idx]}
                for key_s in obj_s:
                    if key_s != mcol_s:
                        dicttmp[s_prefix + "_" + key_s] = obj_s[key_s]
                for key_r in obj_r:
                    if key_r != mcol_r:
                        dicttmp[r_prefix + "_" + key_r] = obj_r[key_r]
                dicttmp[mcol_new] = obj_s[mcol_s]
                result.append(dicttmp)
    return result

# Statistic functions derived from class example
def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

# We add additional parameter that specify how many runs required to approximate p-value
def pval(x, y, s):
    c0 = corr(x, y)
    corrs = []
    for _ in range(0, s):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
    return len([c for c in corrs if abs(c) >= abs(c0)])/len(corrs)

# Return distance between two points (Latitude, Longitude)
# The distance is represented in kilometers
def getVDist(lat1, long1, lat2, long2):
    return vincenty((lat1, long1), (lat2, long2)).kilometers


# Finding road safety rating by using numbers of
# surrounding traffic signals and road lights
class CorrelationAccidentDensityLightSignal(dml.Algorithm):
    contributor = 'liwang_pyhsieh'
    reads = ['liwang_pyhsieh.accident_density', 'liwang_pyhsieh.safety_scores']
    writes = ['liwang_pyhsieh.accident_correlation']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.now()

        print("do some stats!")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        # Compute correlation between density of accidents and density of street light/traffic signals
        accident_density = repo['liwang_pyhsieh.accident_density'].find()
        accident_density = [x for x in accident_density]
        safety_scores = [x for x in repo['liwang_pyhsieh.safety_scores'].find()]

        join_density = join(safety_scores, accident_density, "ss", "ad", "_id", "_id", "_id", "_id")
        
        light_density = [x["ss_lights"] for x in join_density]
        signal_density = [x["ss_signals"] for x in join_density]
        accident_density = [x["ad_accident_density"] for x in join_density]

        corr_accident_light = corr(accident_density, light_density)
        corr_accident_signal = corr(accident_density, signal_density)
        corr_light_signal = corr(light_density, signal_density)
        
        if trial == True:
            pval_accident_light = pval(accident_density, light_density, 500)
            pval_accident_signal = pval(accident_density, signal_density, 500)
            pval_light_signal = pval(light_density, signal_density, 500)
        else:
            # pearsonr provides two-tailed values, we're only interested in positive side
            # and the result shows the value on negative side is small enough to omit (<< 1e-10)
            pval_accident_light = scipy.stats.pearsonr(accident_density, light_density)[0]
            pval_accident_signal = scipy.stats.pearsonr(accident_density, signal_density)[0]
            pval_light_signal = scipy.stats.pearsonr(light_density, signal_density)[0]

        accident_correlation = [
            {"_id": "0", "relation": "accident-light", "corr": corr_accident_light, "pval": pval_accident_light},
            {"_id": "1", "relation": "accident-signal", "corr": corr_accident_signal, "pval": pval_accident_signal},
            {"_id": "2", "relation": "light-signal", "corr": corr_light_signal, "pval": pval_light_signal}
        ]

        repo.dropCollection("accident_correlation")
        repo.createCollection("accident_correlation")
        repo["liwang_pyhsieh.accident_correlation"].insert_many(accident_correlation)

        repo.logout()
        endTime = datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        # create document object and define namespaces
        this_script = doc.agent('alg:liwang_pyhsieh#accidentCorrelation',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        # https://data.cityofboston.gov/resource/492y-i77g.json
        resource_accident_denstiy = doc.entity('dat:liwang_pyhsieh#accident_denstiy', {prov.model.PROV_LABEL: 'accident_denstiy', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_accident_denstiy = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_accident_denstiy, this_script)
        doc.usage(get_accident_denstiy, resource_accident_denstiy, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        # https://data.cityofboston.gov/resource/492y-i77g.json
        resource_safety_score = doc.entity('dat:liwang_pyhsieh#safety_scores', {prov.model.PROV_LABEL: 'safety_scores',
                                                                                 prov.model.PROV_TYPE: 'ont:DataSet'})
        get_safety_score = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_safety_score, this_script)
        doc.usage(get_safety_score, resource_safety_score, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        get_accident_correlation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        accident_correlation = doc.entity('dat:liwang_pyhsieh#accident_correlation',
            {prov.model.PROV_LABEL: 'Correlation factors between accident and surrounding facility density', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(accident_correlation, this_script)
        doc.wasGeneratedBy(accident_correlation, get_accident_correlation, endTime)
        doc.wasDerivedFrom(accident_correlation, resource_accident_denstiy, get_accident_correlation, get_accident_correlation, get_accident_correlation)
        doc.wasDerivedFrom(accident_correlation, resource_safety_score, get_accident_correlation, get_accident_correlation, get_accident_correlation)

        repo.logout()
        return doc

if __name__ == "__main__":
    CorrelationAccidentDensityLightSignal.execute()
    doc = CorrelationAccidentDensityLightSignal.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))
