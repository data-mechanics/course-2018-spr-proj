import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pdb
import re
from alyu_sharontj.Util.Util import *

class SnowEmRoutes_TrafficSignalDensity(dml.Algorithm):
    contributor = 'alyu_sharontj'
    reads = ['alyu_sharontj.SnowEmRoute','alyu_sharontj.TrafficSignal_Density'] #read the data of SnowEmRoute and TrafficSignal_Density from mongo
    writes = ['alyu_sharontj.SnowEmRoutes_TrafficSignalDensity']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        '''Set up the database connection.'''
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj', 'alyu_sharontj') # should probably move this to auth


        '''get (roadname,sig_density) from db.alyu_sharontj.TrafficSignal_Density'''
        density = []
        densityDb = repo['alyu_sharontj.TrafficSignal_Density']
        cursor = densityDb.find()
        for info in cursor:
            tmp = (info['RoadName'], info['TrafficSignal_Density'])
            density.append(tmp)




        '''get SnowEm_Roadname from db.alyu_sharontj.SnowEmRoute
        '''
        SnowEm_Roadname=[]
        SnowEmDb=repo['alyu_sharontj.SnowEmRoute']
        cursor = SnowEmDb.find()
        for info in cursor:
            tmp = info['properties']['FULL_NAME']
            SnowEm_Roadname.append(tmp)



        '''combine SnowEm_Roadname with (roadname,sig_density) 
            expected output: (SnowEm_Roadname,sig_density)
        '''

        result = project(select(product(SnowEm_Roadname, density), lambda t: t[0] == t[1][0]), lambda t: (t[0], t[1][1]))



        repo.dropCollection("SnowEmRoutes_TrafficSignalDensity")
        repo.createCollection("SnowEmRoutes_TrafficSignalDensity")
        for k,v in result:
            oneline={'SnowEmRoadName': k, 'TrafficSignal_Density': v}
            repo['alyu_sharontj.SnowEmRoutes_TrafficSignalDensity'].insert_one(oneline)


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
        repo.authenticate('alyu_sharontj', 'alyu_sharontj')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/alyu_sharontj') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/alyu_sharontj') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        # doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        # doc.add_namespace('hdv', 'https://dataverse.harvard.edu/dataset.xhtml')

        this_script = doc.agent('alg:alyu_sharontj#SnowEmRoutes_TrafficSignalDensity',
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        road_input = doc.entity('dat:alyu_sharontj.SnowEmRoute',
                                {prov.model.PROV_LABEL:'SnowEmRoute',
                                 prov.model.PROV_TYPE:'ont:DataSet'})

        signal_input = doc.entity('dat:alyu_sharontj.TrafficSignal_Density',
                                  {prov.model.PROV_LABEL:'TrafficSignal_Density',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)


        output = doc.entity('dat:alyu_sharontj.SnowEmRoutes_TrafficSignalDensity',
            { prov.model.PROV_LABEL:'SnowEmRoutes_TrafficSignalDensity', prov.model.PROV_TYPE: 'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, road_input, startTime)
        doc.used(this_run, signal_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, road_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, signal_input, this_run, this_run, this_run)
        repo.logout()


        return doc



# SnowEmRoutes_TrafficSignalDensity.execute()
# doc = SnowEmRoutes_TrafficSignalDensity.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
