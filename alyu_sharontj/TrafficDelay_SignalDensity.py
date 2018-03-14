import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pdb
import re
from alyu_sharontj.Util.Util import *

class TrafficDelay_SignalDensity(dml.Algorithm):
    contributor = 'alyu_sharontj'
    reads = ['alyu_sharontj.TrafficJam','alyu_sharontj.TrafficSignal_Density'] #read the data of roads and trafficsignals from mongo
    writes = ['alyu_sharontj.TrafficDelay_SignalDensity']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        '''Set up the database connection.'''
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj', 'alyu_sharontj') # should probably move this to auth

        '''get (roadname,sig_density) from db.alyu_sharontj.TrafficSignal_Density'''
        density=[]
        densityDb=repo['alyu_sharontj.TrafficSignal_Density']
        cursor = densityDb.find()
        for info in cursor:
            tmp = (info['RoadName'], info['TrafficSignal_Density'])

            density.append(tmp)
        # print(len(density))    #4854

        '''get (roadname,avg_delay) from db.alyu_sharontj.TrafficJam
        '''
        road_jam=[]
        jamDb=repo['alyu_sharontj.TrafficJam']
        cursor = jamDb.find()
        for info in cursor:    #get (road name, delay)
            tmp = (info['street'], info['delay'])
            road_jam.append(tmp)

        sum_delay = aggregate(road_jam, sum)    #get (road name, total_delay )
        count_delay = aggregate(road_jam, len)    #get (road name,  num_of_jam)
        x = map(lambda k, v: [(k, v)], sum_delay) + map(lambda k, v: [(k, v)], count_delay)
        avg_delay = reduce(lambda k, v: (k, v[0]/v[1]), x)   #get (road name, average delay time)
        # print(len(avg_delay))   #654



        '''combine (roadname,sig_density) with (roadname, avg_delay) 
            expected output: (roadname, (sig_density, avg_delay))
        '''
        sigden_delay = project(select(product(density,avg_delay), lambda t: t[0][0]==t[1][0]), lambda t: (t[0][0], (t[0][1],t[1][1])))
        #print(len(sigden_delay))  #573



        repo.dropCollection("TrafficDelay_SignalDensity")
        repo.createCollection("TrafficDelay_SignalDensity")
        for k,v in sigden_delay:
            oneline={'RoadName': k, 'Signal_Density': v[0], 'Average_Delay': v[1]}
            repo['alyu_sharontj.TrafficDelay_SignalDensity'].insert_one(oneline)


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

        this_script = doc.agent('alg:alyu_sharontj#TrafficDelay_SignalDensity',
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        density_input = doc.entity('dat:alyu_sharontj.TrafficSignal_Density',
                                {prov.model.PROV_LABEL:'TrafficSignal_Density',
                                 prov.model.PROV_TYPE:'ont:DataSet'})

        jam_input = doc.entity('dat:alyu_sharontj.TrafficJam',
                                  {prov.model.PROV_LABEL:'TrafficJam ',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})


        output = doc.entity('dat:alyu_sharontj.TrafficDelay_SignalDensity',
            { prov.model.PROV_LABEL:'TrafficDelay_SignalDensity', prov.model.PROV_TYPE: 'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, density_input, startTime)
        doc.used(this_run, jam_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, density_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, jam_input, this_run, this_run, this_run)
        repo.logout()


        return doc



# TrafficDelay_SignalDensity.execute()
# doc = TrafficSignal_Density.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
