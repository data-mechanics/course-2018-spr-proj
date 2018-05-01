import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import numpy

from alyu_sharontj_yuxiao_yzhang11.Util.Util import *



class Fire_Hospital_vs_Rent(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.fire_count', 'alyu_sharontj_yuxiao_yzhang11.hospital','alyu_sharontj_yuxiao_yzhang11.average_rent_zip']
    writes = ['alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        # def union(R, S):
        #     return R + S
        #
        # def difference(R, S):
        #     return [t for t in R if t not in S]
        #
        # def intersect(R, S):
        #     return [t for t in R if t in S]
        #
        # def project(R, p):
        #     return [p(t) for t in R]
        #
        # def select(R, s):
        #     return [t for t in R if s(t)]
        #
        # def product(R, S):
        #     return [(t, u) for t in R for u in S]
        #
        # def aggregate(R, f):
        #     keys = {r[0] for r in R}
        #     return [(key, f([v for (k, v) in R if k == key])) for key in keys]
        #
        # def map(f, R):
        #     return [t for (k, v) in R for t in f(k, v)]
        #
        # def reduce(f, R):
        #     keys = {k for (k, v) in R}
        #     return [f(k1, [v for (k2, v) in R if k1 == k2]) for k1 in keys]


        '''get hospital_count = (zipcode,hospital_count) from db.alyu_sharontj_yuxiao_yzhang11.hospital'''

        hospital_count=[]
        zip1 = []
        hospitalDB=repo['alyu_sharontj_yuxiao_yzhang11.hospital']
        cursor = hospitalDB.find()
        for info in cursor:
            street = info['street']
            tmp = street.split(",")[2].split(" ")[2]
            zip1.append(tmp)

        hospital_count = aggregate(project(zip1,lambda t: (t,1)),sum)


        '''get fire_count = (zipcode,fire_count) from db.alyu_sharontj_yuxiao_yzhang11.fire_count'''

        fire_count=[]
        firecountDB=repo['alyu_sharontj_yuxiao_yzhang11.fire_count']
        cursor = firecountDB.find()
        for info in cursor:
            tmp = (info['_id'], info['count']/3)
            fire_count.append(tmp)


        '''get average_rent = (zipcode,average) from db.alyu_sharontj_yuxiao_yzhang11.average_rent_zip'''

        zip_rent=[]
        zip2 = []
        ziprentDB=repo['alyu_sharontj_yuxiao_yzhang11.average_rent_zip']
        cursor = ziprentDB.find()
        for info in cursor:
            tmp1 = (info['Zip'], info['Average'])
            tmp2 = (info['Zip'])
            zip_rent.append(tmp1)
            zip2.append(tmp2)


        '''combine hospital_count = (zipcode,hospital_count)  with fire_count = (zipcode,fire_count)
                   expected output: (zipcode, fire_count / hospital_count)
               '''

        zipcode_list=["02110","02210","02132","02109","02199","02108","02113", "02116","02163","02136","02111","02129", "02114", \
                      "02131", "02118", "02130", "02127", "02135", "02126", "02125", "02215", "02134", "02122", "02128", "02115", "02124", "02120", "02119", "02121"]


        fire_hospital_1 = project(select(product(fire_count,hospital_count), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1]/t[1][1]))
        fire_hospital_2 =[]
        fire_hospital_3 =[]


        fire_zips = project(fire_count, lambda t: t[0])
        hospital_zips = project(hospital_count, lambda t: t[0])
        differ1 = difference(fire_zips,hospital_zips)
        differ2 = difference(zipcode_list, fire_zips)


        ratio_list=[]
        for i in fire_hospital_1:
            tmp = i[1]
            ratio_list.append(tmp)


        mu = numpy.mean(ratio_list)
        sigma = numpy.std(ratio_list)


        for i in differ1:
            tmp = (i,mu+3*sigma)
            fire_hospital_2.append(tmp)


        for i in differ2:
            tmp = (i,mu-3*sigma)
            fire_hospital_3.append(tmp)


        fire_hospital_ = union(fire_hospital_1,fire_hospital_2)
        fire_hospital = union(fire_hospital_, fire_hospital_3)

        fire_hospital_vs_rent = project(select(product(fire_hospital,zip_rent), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))


        repo.dropCollection("Fire_Hospital_vs_Rent")
        repo.createCollection("Fire_Hospital_vs_Rent")
        for k, v, s in fire_hospital_vs_rent:
            oneline = {'Zipcode': k, 'fire/hospital': v, 'average rent': s}
            repo['alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent'].insert_one(oneline)



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


        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#Fire_Hospital_vs_Rent',
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        hospital_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#hospital',
                                {prov.model.PROV_LABEL:'hospital',
                                 prov.model.PROV_TYPE:'ont:DataSet'})

        fire_count_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#fire_count',
                                  {prov.model.PROV_LABEL:'fire_count',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        rent_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#average_rent_zip',
                                  {prov.model.PROV_LABEL:'average_rent_zip',
                                   prov.model.PROV_TYPE:'ont:DataSet'})


        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})


        output = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#Fire_Hospital_vs_Rent',
            { prov.model.PROV_LABEL:'Fire_Hospital_vs_Rent',
              prov.model.PROV_TYPE: 'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, hospital_input, startTime)
        doc.used(this_run, fire_count_input, startTime)
        doc.used(this_run, rent_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, hospital_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, fire_count_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, rent_input, this_run, this_run, this_run)
        repo.logout()


        return doc




# Fire_Hospital_vs_Rent.execute()
# doc = Fire_Hospital_vs_Rent.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
