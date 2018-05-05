import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import re
from alyu_sharontj_yuxiao_yzhang11.Util.Util import *



class education_trans_avg(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.education',
             'alyu_sharontj_yuxiao_yzhang11.hubway',
             'alyu_sharontj_yuxiao_yzhang11.MBTA'] #read the data of roads and trafficsignals from mongo
    writes = ['alyu_sharontj_yuxiao_yzhang11.education_trans_avg']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        '''Set up the database connection.'''
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')


        '''get (schoolid,zipcode,latitude,longitute) from alyu_sharontj_yuxiao_yzhang11.education'''
        schoolinfo = []
        edudb = repo['alyu_sharontj_yuxiao_yzhang11.education']
        educur = edudb.find()  #filter not work
        for info in educur:
            school_id = info['properties']['SchoolId']
            if (school_id != "0"):
                address = info['properties']['Address']
                zipcode = address[-5: ]
                Latitude = float(info['properties']['Latitude'])
                Longitude = float(info['properties']['Longitude'])
                schoolinfo.append((school_id, zipcode, (Latitude, Longitude)))
        # print(schoolinfo)



        hubwaydb = repo['alyu_sharontj_yuxiao_yzhang11.hubway']
        hubwayinfo = []
        match = {
            'status': "Existing"
        }
        hubwayExist = hubwaydb.aggregate([
            {
                '$match': match
            }
        ])
        for info in hubwayExist:
            hubway_id = info['id']
            Latitude = float(info['lat'])
            Longitude = float(info['lng'])
            hubwayinfo.append((hubway_id,(Latitude,Longitude)))
        # print(hubwayinfo)

        edu_hub = [(s[0],s[1], h[0], distance(s[2], h[1])) for (s, h) in product(schoolinfo, hubwayinfo)]
        # print(len(edu_hub))

        edu_hub_1 = [ ((s,zip),dis) for (s,zip,h,dis) in edu_hub if dis<0.8]
        # print(len(edu_hub_1))

        edu_hub_count = aggregate(project(edu_hub_1, lambda t: (t[0],1)), sum)



        mbtadb = repo['alyu_sharontj_yuxiao_yzhang11.MBTA']
        mbtainfo = []
        mbtacur = mbtadb.find();

        for info in mbtacur:
            mbta_id = info['stop_id']
            Latitude = float(info['stop_lat'])
            Longitude = float(info['stop_lon'])
            mbtainfo.append((mbta_id, (Latitude, Longitude)))
        # print(mbtainfo)

        edu_mbta = [(s[0], s[1], distance(s[2], h[1])) for (s, h) in product(schoolinfo, mbtainfo)]
        # print(len(edu_mbta))

        edu_mbta_1 = [((s, zip), dis) for (s, zip, dis) in edu_mbta if dis < 0.8]
        # print(len(edu_mbta_1))

        edu_mbta_count = aggregate(project(edu_mbta_1, lambda t: (t[0], 1)), sum)
        # print(edu_mbta_count)

        select_edu_mbta_hub = select(product(edu_hub_count, edu_mbta_count), lambda t: t[0][0][0]==t[1][0][0])
        edu_hub_mbta = [(h[0][1], h[0][0], h[1]+m[1]) for (h,m) in select_edu_mbta_hub]
        # print(edu_hub_mbta)

        zip_edu_trans = project(edu_hub_mbta, lambda t: (t[0], (1, t[2])))
        # print(zip_edu_trans)

        zip_edu_trans_count = aggregate(zip_edu_trans, ADD)
        # print(zip_edu_trans_count)

        zip_edu_trans_avg = [(z, t[0], t[1]/t[0]) for (z,t)in zip_edu_trans_count]
        # print(zip_edu_trans_avg)


        repo.dropCollection("education_trans_avg")
        repo.createCollection("education_trans_avg")
        for i in zip_edu_trans_avg:
            single = {'zip': i[0], 'school_count': i[1], 'trans_avg': i[2]}
            repo['alyu_sharontj_yuxiao_yzhang11.education_trans_avg'].insert_one(single)


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
        # doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        # doc.add_namespace('hdv', 'https://dataverse.harvard.edu/dataset.xhtml')

        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#education_trans_avg',
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        education_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#education',
                                {prov.model.PROV_LABEL:'education',
                                 prov.model.PROV_TYPE:'ont:DataSet'})

        hubway_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#hubway',
                                  {prov.model.PROV_LABEL:'hubway',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        mbta_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#MBTA',
                                  {prov.model.PROV_LABEL: 'MBTA',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})

        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})


        output = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11#education_trans_avg',
            { prov.model.PROV_LABEL:'education_trans_avg', prov.model.PROV_TYPE: 'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, education_input, startTime)
        doc.used(this_run, hubway_input, startTime)
        doc.used(this_run, mbta_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, education_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, hubway_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, mbta_input, this_run, this_run, this_run)

        repo.logout()


        return doc



#
# education_trans_avg.execute()
# doc = education_trans_avg.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
