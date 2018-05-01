import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import random
from math import *
from scipy.cluster.vq import kmeans as km, kmeans2


'''In this script I am clustering the homes that are more than 1KM away but less than 4KM away from their closest Hubway Station.
This way I can kind the mean of the cluster and identify that as a possible location for a new Hubway Station.
This is done by implementing Kmeans.
This scrip will also produce a provenance document when its provenance() method is called.
Format taken from example file in github.com/Data-Mechanics  '''

class kmeans(dml.Algorithm):
    contributor = 'pandreah'
    reads = ['pandreah.popDense']
    writes = ['pandreah.kmeans']

###########################################################################
##      This was taken from the class website.                           ##
###########################################################################
        
    def plus(args):
        p = [0,0]
        for (x,y) in args:
            p[0] += x
            p[1] += y
        return tuple(p)
    
    def dist(p, q):
        (x1,y1) = p
        (x2,y2) = q
        return math.sqrt((x1-x2)**2 + (y1-y2)**2)

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()


        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pandreah', 'pandreah')
        repo.dropCollection("kmeans")
        repo.createCollection("kmeans")

#Getting all of the homes that are at the specified distance.
        h = repo['pandreah.popDense']
        homes = list(h.find())

        
#Selecting the coordinate of each house
        P = []
        for house in homes:
            x1 = house["lat"]
            y1 = house["lng"]
            P.append((x1, y1))

#Generating the means
        mean, labels = kmeans2(P,2)
#        print(mean)
        Kmeans = []

#Going through the means and attaching a house count to them
        for x in range(len(mean)):
            count_h = 0
            Kmeans.append((mean[x][0], mean[x][1]))

            lat = round(float(Kmeans[x][0]), 8)
            lng = round(float(Kmeans[x][1]), 8)

            for house in homes:
                x1 = round(float(house["lat"]), 8)
                y1 = round(float(house["lng"]), 8)

                distance = 6371 * acos(round(cos(radians(90 - lat)) * cos(radians(90 - x1)) + sin(radians(90 - lat)) * sin(radians(90 - x1)) * cos(radians(lng - y1)), 8))

                if distance <= 1:
                    count_h += 1
            
            r = {"mean#": x, "lat": Kmeans[x][0], "lng": Kmeans[x][1], "homes_1KM": count_h}

            repo.pandreah.kmeans.insert_one(r)

                    
        repo['pandreah.kmeans'].metadata({'complete':True})
        print(repo['pandreah.kmeans'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
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
        repo.authenticate('pandreah', 'pandreah')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
 
        this_script = doc.agent('alg:pandreah#kmeans', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat: pandreah#popDense', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'Computation'})
        kmeans = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(kmeans, this_script)
        doc.usage(kmeans, resource, startTime, None,
            {prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:Query':'?type=kmeans&$select=type,latitude,longitude,home_count'
                }
                )
 
        kmeans = doc.entity('dat:pandreah#kmeans', {prov.model.PROV_LABEL:'kmeans ', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(kmeans, this_script)
        doc.wasGeneratedBy(kmeans, kmeans, endTime)
        doc.wasDerivedFrom(kmeans, resource, kmeans, kmeans, kmeans)

        repo.logout()
                  
        return doc
    
if __name__ == "__main__":
    kmeans.execute()
    doc = kmeans.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))
