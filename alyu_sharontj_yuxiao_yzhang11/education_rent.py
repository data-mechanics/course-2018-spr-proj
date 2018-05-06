import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import re
from alyu_sharontj_yuxiao_yzhang11.Util.Util import *



class education_rent(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.education', 'alyu_sharontj_yuxiao_yzhang11.average_rent_zip'] #read the data of roads and trafficsignals from mongo
    writes = ['alyu_sharontj_yuxiao_yzhang11.education_rent']


    @staticmethod
    def execute(trial=True):
        startTime = datetime.datetime.now()

        '''Set up the database connection.'''
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')


        '''get (schoolid,zipcode) from alyu_sharontj_yuxiao_yzhang11.education'''
        schoolinfo = []
        edudb = repo['alyu_sharontj_yuxiao_yzhang11.education']
        educur = edudb.find()
        for info in educur:
            edu_id= info['properties']['SchoolId']
            if (edu_id != "0"):
                address = info['properties']['Address']
                edu_zip = address[-5: ]
                schoolinfo.append((edu_zip, 1))
        # print(schoolinfo)
        # print(len( schoolinfo))


        rentaldb = repo['alyu_sharontj_yuxiao_yzhang11.average_rent_zip']
        rateinfo = []
        ratecur =rentaldb.find()
        for info in ratecur:
            rate_zip = info['Zip']
            rate_avg = info['Average']
            rateinfo.append((rate_zip, rate_avg))
        # print("len of rentinfo"+str(len(rateinfo)))
        #
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
        #     return [(t,u) for t in R for u in S]
        #
        # def aggregate(R, f):
        #     keys = {r[0] for r in R}
        #     return [(key, f([v for (k,v) in R if k == key])) for key in keys]
        # edu=[(1,3),(1,2),(2,9)]
        # rent=[(1,22),(3,29)]



        edubyzip= aggregate(schoolinfo, sum)
        # print(edubyzip)
        # print(len(edubyzip))
        x = product(edubyzip, rateinfo)
        y = select(x, lambda t: t[0][0] == t[1][0])
        result = project(y, lambda t: (t[0][1], t[1][1]))
        # print(result)

        repo.dropCollection("alyu_sharontj_yuxiao_yzhang11.education_rent")
        repo.createCollection("alyu_sharontj_yuxiao_yzhang11.education_rent")
        for k,v in result:
            oneline={'edu_count': k, 'rent': v}
            repo['alyu_sharontj_yuxiao_yzhang11.education_rent'].insert_one(oneline)


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


        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#education_rent',
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        education_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#education',
                                {prov.model.PROV_LABEL:'education',
                                 prov.model.PROV_TYPE:'ont:DataSet'})

        rent_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#average_rent_zip',
                                  {prov.model.PROV_LABEL:'average_rent_zip',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})


        output = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#education_rent',
            { prov.model.PROV_LABEL:'education_rent', prov.model.PROV_TYPE: 'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, education_input, startTime)
        doc.used(this_run, rent_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, education_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, rent_input, this_run, this_run, this_run)
        repo.logout()


        return doc




# education_rent.execute()
# doc = education_rent.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
