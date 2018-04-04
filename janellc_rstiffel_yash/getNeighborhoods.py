import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class getNeighborhoods(dml.Algorithm):
    contributor = 'janellc_rstiffel_yash'
    reads = []
    writes = ['janellc_rstiffel_yash.neighborhoods']  # 'medinad.meters'

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')


        # data set borrowed from old team on datamechanics.io
        url = 'http://datamechanics.io/data/jb_rfb_dm_proj2data/bos_neighborhoods_shapes.json'  # 'http://bostonopendata-boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        neighborhoods = list(r)
        neighborhoodList = [{'Neighborhood': x["fields"]["neighborho"], 'Polygon': x["fields"]["geo_shape"]} for x in neighborhoods]

        # print(neighborhoodList[1])
        # print(neighborhoodList[0]['fields']['geo_shape']['coordinates'][0][0])



        repo.dropCollection("janellc_rstiffel_yash.neighborhoods")
        repo.createCollection("janellc_rstiffel_yash.neighborhoods")
        repo['janellc_rstiffel_yash.neighborhoods'].insert_many(neighborhoodList)
        repo['janellc_rstiffel_yash.neighborhoods'].metadata({'complete': True})
        print(repo['janellc_rstiffel_yash.neighborhoods'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        
        # Agent, entity, activity
        this_script = doc.agent('alg:janellc_rstiffel_yash#getNeighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # Resource = crimesData
        resource1 = doc.entity('dat:jb_rfb_dm_proj2data', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        #Activity
        get_neighborhood = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_neighborhood, this_script)

        doc.usage(get_neighborhood, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )


        neighbors = doc.entity('dat:janellc_rstiffel_yash#crimesDistrict', {prov.model.PROV_LABEL:'Avg Loc Streets per District', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighbors, this_script)
        doc.wasGeneratedBy(neighbors, get_neighborhood, endTime)
        doc.wasDerivedFrom(neighbors, resource1, get_neighborhood, get_neighborhood, get_neighborhood)

        repo.logout()
                  
        return doc

#getNeighborhoods.execute()
#doc = getNeighborhoods.provenance()
#print(doc.get_provn())