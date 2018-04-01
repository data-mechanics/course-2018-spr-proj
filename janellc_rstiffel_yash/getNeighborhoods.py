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


        repo.logout()

        return doc

getNeighborhoods.execute()