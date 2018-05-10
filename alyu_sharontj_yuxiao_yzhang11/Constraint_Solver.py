import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import numpy
import statistics as stats

# from alyu_sharontj_yuxiao_yzhang11.Util.Util import *



class Constraint_Solver(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.garden',
             'alyu_sharontj_yuxiao_yzhang11.education',
             'alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent',
             'alyu_sharontj_yuxiao_yzhang11.average_rent_zip',
             'alyu_sharontj_yuxiao_yzhang11.education_trans_avg',
             'alyu_sharontj_yuxiao_yzhang11.correlation']
    writes = ['alyu_sharontj_yuxiao_yzhang11.Result']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        def union(R, S):
            return R + S

        def difference(R, S):
            return [t for t in R if t not in S]

        def intersect(R, S):
            return [t for t in R if t in S]

        def project(R, p):
            return [p(t) for t in R]

        def select(R, s):
            return [t for t in R if s(t)]

        def product(R, S):
            return [(t, u) for t in R for u in S]

        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k, v) in R if k == key])) for key in keys]

        def map(f, R):
            return [t for (k, v) in R for t in f(k, v)]

        def reduce(f, R):
            keys = {k for (k, v) in R}
            return [f(k1, [v for (k2, v) in R if k1 == k2]) for k1 in keys]


        '''get rent = (zipcode,rent) from db.alyu_sharontj_yuxiao_yzhang11.average_rent_zip'''

        rentinfo = []
        rentdb = repo['alyu_sharontj_yuxiao_yzhang11.average_rent_zip']
        rentcur = rentdb.find()
        for info in rentcur:
            zipcode= info['Zip']
            rent = info['Average']
            rentinfo.append((zipcode, rent))
        rentdict = dict(rentinfo)
        # print("rent info:"+str(rentinfo))


        '''get number of schools = (zipcode,education_count) from db.alyu_sharontj_yuxiao_yzhang11.education_rent'''
        schoolinfo = []
        edudb = repo['alyu_sharontj_yuxiao_yzhang11.education']
        educur = edudb.find()
        for info in educur:
            edu_id = info['properties']['SchoolId']
            if edu_id != "0":
                address = info['properties']['Address']
                edu_zip = address[-5:]
                schoolinfo.append((edu_zip, 1))
        eduinfo = aggregate(schoolinfo, sum)
        edudict = dict(eduinfo)



        '''get fire_hospital = (zipcode,Fire_Hospital_vs_Rent) from db.alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent'''
        fireinfo = []
        fire_hos_db = repo['alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent']
        fire_hos_cur = fire_hos_db.find()
        for info in fire_hos_cur:
            zipcode = info['Zipcode']
            fire_hos_rate = info['fire/hospital']
            fireinfo.append((zipcode, fire_hos_rate))
        firedict = dict(fireinfo)


        '''get number of garden = (zipcode,garden_count) from db.alyu_sharontj_yuxiao_yzhang11.garden_vs_rent'''

        gardeninfo = []
        gardendb = repo['alyu_sharontj_yuxiao_yzhang11.garden_vs_rent']
        gardencur = gardendb.find()
        for info in gardencur:
            zipcode = info['Zip']
            garden_count = info['garden_count']
            # print(str(zipcode)+","+ str(garden_count))
            gardeninfo.append((zipcode, garden_count))
        gardendict = dict(gardeninfo)


        '''get average number of transportation = (zipcode,trans_avg) from db.alyu_sharontj_yuxiao_yzhang11.education_trans_avg'''
        transinfo = []
        transdb = repo['alyu_sharontj_yuxiao_yzhang11.education_trans_avg']
        transcur = transdb.find()
        for info in transcur:
            zipcode = info['zip']
            trans_avg = info['trans_avg']
            transinfo.append((zipcode,trans_avg))
        transdict = dict(transinfo)


        '''find mean, std of each list'''
        def get_boundary(info):
            value_list = list(info.values())
            mean = stats.mean(value_list)
            # print(str(mean))
            std = stats.stdev(value_list)
            # print(str(std))
            low = mean-3*std
            high = mean + 3*std
            return low, high

        zipcode_list = ["02110","02210","02132","02109","02199","02108","02113", "02116","02163","02136","02111","02129", "02114", \
                      "02131", "02118", "02130", "02127", "02135", "02126", "02125", "02215", "02134", "02122", "02128", "02115",\
                      "02124", "02120", "02119", "02121"]



        '''get correlation coefficience'''
        weightinfo = []
        weightinfo.append(('rent', 0.5))
        corrdb = repo['alyu_sharontj_yuxiao_yzhang11.correlation']
        corrcur = corrdb.find()
        for info in corrcur:
            factor = info['name']
            weight = info['weight']
            weightinfo.append((factor,weight))



        weights = []
        weight_rent = dict(weightinfo)
        weight_edu = {"edu_rent": 0.4, "rent": 0.22, "fire/hospital_rent": 0.18, "trans_rent":0.12, "garden_rent": 0.08}
        weight_safety = {"fire/hospital_rent": 0.4, "rent": 0.22, "edu_rent": 0.18, "trans_rent":0.12, "garden_rent": 0.08}
        weight_trans = {"trans_rent": 0.4, "rent": 0.22, "edu_rent": 0.18, "fire/hospital_rent":0.12, "garden_rent": 0.08}
        weight_facility = {"garden_rent": 0.4, "rent": 0.22, "edu_rent": 0.18, "fire/hospital_rent":0.12, "trans_rent": 0.08}
        weights.append(weight_rent)
        weights.append(weight_edu)
        weights.append(weight_safety)
        weights.append(weight_trans)
        weights.append(weight_facility)
        # print(weights)



        def normalize(value, low, high):
            return float((value-low)/(high-low))

        def getscore(z, dict, factor, weightlist):
            if(z in dict.keys()):
                low,high = get_boundary(dict)
                if(dict[z] <= high and  dict[z] >= low):
                    # print("original"+str(dict[z]))
                    n = normalize(dict[z], low, high) * 100
                    # print("normal"+str(n))
                    score2 = n * weightlist[factor]
                else:
                    score2 = 0
            else:
                score2 = 0
            return score2

        results = []
        for zipcode in zipcode_list:
            # print("weightlist" + str(weightlist))
            scorelist = []
            for weightlist in weights:
                # print('rent')
                rentscore = getscore(zipcode, rentdict, 'rent', weightlist)
                # print('edu')
                eduscore = getscore(zipcode, edudict, 'edu_rent', weightlist)
                # print('fire')
                firescore = getscore(zipcode, firedict, 'fire/hospital_rent', weightlist)
                # print('garden')
                gardenscore = getscore(zipcode, gardendict, 'garden_rent', weightlist)

                transscore = getscore(zipcode, transdict, 'trans_rent', weightlist)

                score = rentscore + firescore + eduscore + gardenscore + transscore
                scorelist.append(score)
            results.append((zipcode, scorelist))

        repo.dropCollection("Result")
        repo.createCollection("Result")


        for k, v in results:
            # normV = normalize(v,low,high) * 100
            oneline = {'Zipcode': k, 'rent': v[0],'education': v[1],'safety': v[2],'transportation': v[3],'facility': v[4]}
            # print(oneline)
            repo['alyu_sharontj_yuxiao_yzhang11.Result'].insert_one(oneline)

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}



    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.


        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#Constraint_Solver',
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        rent_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#average_rent_zip',
                                {prov.model.PROV_LABEL:'average_rent_zip',
                                 prov.model.PROV_TYPE:'ont:DataSet'})

        garden_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#garden',
                                  {prov.model.PROV_LABEL:'garden',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        education_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#education',
                                  {prov.model.PROV_LABEL:'education',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        firehospital_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#Fire_Hospital_vs_Rent',
                                  {prov.model.PROV_LABEL: 'Fire_Hospital_vs_Rent',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})

        correlation_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#correlation',
                                  {prov.model.PROV_LABEL: 'correlation',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})

        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})


        output = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#Result',
            { prov.model.PROV_LABEL:'Result', prov.model.PROV_TYPE: 'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, garden_input, startTime)
        doc.used(this_run, education_input, startTime)
        doc.used(this_run, rent_input, startTime)
        doc.used(this_run, firehospital_input, startTime)
        doc.used(this_run, correlation_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, garden_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, education_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, rent_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, firehospital_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, correlation_input, this_run, this_run, this_run)


        repo.logout()


        return doc




# Constraint_Solver.execute()
# doc = Constraint_Solver.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
