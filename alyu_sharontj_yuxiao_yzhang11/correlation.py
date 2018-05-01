import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

from random import shuffle
from math import sqrt

class correlation(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.education_rent',
             'alyu_sharontj_yuxiao_yzhang11.garden_vs_rent',
             'alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent']
    writes = ['alyu_sharontj_yuxiao_yzhang11.correlation']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')



        fire_hosp_rent = repo['alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent'].find()
        garden_rent = repo['alyu_sharontj_yuxiao_yzhang11.garden_vs_rent'].find()
        edu_rent = repo['alyu_sharontj_yuxiao_yzhang11.education_rent'].find()

        def permute(x):
            shuffled = [xi for xi in x]
            shuffle(shuffled)
            return shuffled

        def avg(x):  # Average
            return sum(x) / len(x)

        def stddev(x):  # Standard deviation.
            m = avg(x)
            return sqrt(sum([(xi - m) ** 2 for xi in x]) / len(x))

        def cov(x, y):  # Covariance.
            return sum([(xi - avg(x)) * (yi - avg(y)) for (xi, yi) in zip(x, y)]) / len(x)

        def corr(x, y):  # Correlation coefficient.
            if stddev(x) * stddev(y) != 0:
                return cov(x, y) / (stddev(x) * stddev(y))

        x1 = []
        y1 = []
        corr_fire_hosp_rent = 0
        for i in fire_hosp_rent:

            x1 += [i["fire/hospital"]]
            y1 += [i["average rent"]]


        corr_fire_hosp_rent = corr(x1, y1)
        # print("corr fire hosp rent", corr_fire_hosp_rent)

        x2 = []
        y2 = []
        for i in garden_rent:
            x2 += [i["garden_count"]]
            y2 += [i["Average"]]

        corr_garden_rent = 0
        # print("garden vs rent")

        corr_garden_rent = corr(x2,y2)
        # print(corr_garden_rent)

        x3 = []
        y3 = []
        corr_edu_rent = 0
        for i in edu_rent:
            x3 += [i["edu_count"]]
            y3 += [i["rent"]]

        corr_edu_rent = corr(x3,y3)
        # print("corr edu rent ", corr_edu_rent)
        # print()

        c_sum = -corr_fire_hosp_rent+corr_edu_rent+corr_garden_rent
        weight_fire_hosp_rent = -corr_fire_hosp_rent/c_sum
        weight_edu_rent = corr_edu_rent/c_sum
        weight_garden_rent = corr_garden_rent/c_sum
        # print()

        edu_rent = {}
        edu_rent["name"] = "edu_rent"
        edu_rent["correlation"] = corr_edu_rent
        edu_rent["weight"] = weight_edu_rent

        repo.dropCollection("correlation") #name of the data link: e.g. station_links
        repo.createCollection("correlation")

        repo['alyu_sharontj_yuxiao_yzhang11.correlation'].insert(edu_rent)

        fire_h_rent = {}
        fire_h_rent["name"] = "fire/hospital_rent"
        fire_h_rent["correlation"] = corr_fire_hosp_rent
        fire_h_rent["weight"] = weight_fire_hosp_rent

        repo['alyu_sharontj_yuxiao_yzhang11.correlation'].insert(fire_h_rent)

        gard_rent = {}
        gard_rent["name"] = "garden_rent"
        gard_rent["correlation"] = corr_garden_rent
        gard_rent["weight"] = weight_garden_rent

        repo['alyu_sharontj_yuxiao_yzhang11.correlation'].insert(gard_rent)


        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''


        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        doc.add_namespace('alg',
                          'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat',
                          'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#correlation',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        fire_h_rent_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#Fire_Hospital_vs_Rent',
                                    {prov.model.PROV_LABEL: 'Fire_Hospital_vs_Rent',
                                     prov.model.PROV_TYPE: 'ont:DataSet'})

        garden_rent_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#garden_vs_rent',
                                      {prov.model.PROV_LABEL: 'garden_vs_rent',
                                       prov.model.PROV_TYPE: 'ont:DataSet'})

        edu_rent_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#education_rent',
                                {prov.model.PROV_LABEL: 'education_rent',
                                 prov.model.PROV_TYPE: 'ont:DataSet'})



        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime,
                                endTime)  # , 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})

        output = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#correlation',
                            {prov.model.PROV_LABEL: 'correlation',
                             prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, fire_h_rent_input, startTime)
        doc.used(this_run,  garden_rent_input, startTime)
        doc.used(this_run, edu_rent_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, fire_h_rent_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, garden_rent_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, edu_rent_input, this_run, this_run, this_run)
        repo.logout()

        return doc

# correlation.execute()
# doc = correlation.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
