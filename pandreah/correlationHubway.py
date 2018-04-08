import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
import random

'''This script calculates the Correlation Coefficient between the Home Density (1KM around) and Hubway Stations(3KM around) around Hubway Stations.
This scrip will also produce a provenance document when its provenance() method is called.
Format taken from example file in github.com/Data-Mechanics''' 
#calculations to get correlation for density of homes per hubway to density of hubways relative to hubways
class correlationHubway(dml.Algorithm):
    contributor = 'pandreah'
    reads = ['pandreah.popHubway']
    writes = ['pandreah.correlation']

###########################################################################
##      ALL OF THESE WERE TAKEN FROM THE CLASS WEBSITE.                  ##
###########################################################################
    def permute(x):
        shuffled = [xi for xi in x]
        random.shuffle(shuffled)
        return shuffled

    def avg(x): # Average
        return sum(x)/len(x)
    
    def stddev(x): # Standard deviation.
        m = correlationHubway.avg(x)
        return math.sqrt(sum([(xi-m)**2 for xi in x])/len(x))
    
    def cov(x, y): # Covariance.
        return sum([(xi-correlationHubway.avg(x))*(yi-correlationHubway.avg(y)) for (xi,yi) in zip(x,y)])/len(x)
    
    def corr(x, y): # Correlation coefficient.
        if correlationHubway.stddev(x)*correlationHubway.stddev(y) != 0:
            return correlationHubway.cov(x, y)/(correlationHubway.stddev(x)*correlationHubway.stddev(y))
     
    def p(x, y):
        c0 = correlationHubway.corr(x, y)
        corrs = []
        for k in range(0, 2000):
            y_permuted = correlationHubway.permute(y)
            corrs.append(correlationHubway.corr(x, y_permuted))
        return len([c for c in corrs if abs(c) > c0])/len(corrs)
        
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()


        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pandreah', 'pandreah')

        #Getting the relevant data for these calculations. They were all stored inside popHubway collection.
        d = repo['pandreah.popHubway']
        dataFromHub = list(d.find())

        #I have to separate the different density datas
        houseDensity = []
        hubwayDensity = []
        
        for data in dataFromHub:
            if data["houses_1KM"] != 0:                 #Exclude the Hubway Stations that are in areas for which I don't have address information
                houseDensity.append(data["houses_1KM"])
                hubwayDensity.append(data["hubways_3KM"])
                
        #Getting the averages for both home and hubway density
        average_home_density = correlationHubway.avg(houseDensity)
        average_hubway_density = correlationHubway.avg(hubwayDensity)

        #Getting the correlation and P-value
        correl = correlationHubway.corr(houseDensity, hubwayDensity)
        p_value = correlationHubway.p(houseDensity, hubwayDensity)

        
        calculatedCorrel = {"correlation": correl, "p-value" : p_value, "averageHomes1KM": average_home_density, "averageHubway3KM": average_hubway_density}
        #This will become the MongoDB collection containing the correlation results
              
        repo.dropCollection("correlHubway")
        repo.createCollection("correlHubway")
          
        repo.pandreah.correlHubway.insert_one(calculatedCorrel)
        repo['pandreah.correlHubway'].metadata({'complete':True})
        print(repo['pandreah.correlHubway'].metadata())
          
  
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
 
        this_script = doc.agent('alg:pandreah#correlHubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Computation':'py'})
        resource_popHubway = doc.entity('dat:pandreah#popHubway', {'prov:label':'MongoDB', prov.model.PROV_TYPE:'ont:DataResource'})
        correlationHubway = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(correlationHubway, this_script)
        doc.usage(correlationHubway, resource_popHubway, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=correlationHubway&$select=correlation,p-value, averageHomes1KM, averageHubway3KM'
                  }
                  )
 
        correlHubway = doc.entity('dat:pandreah#calculateCorrelations', {prov.model.PROV_LABEL:'Calculated Correlations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(correlHubway, this_script)
        doc.wasGeneratedBy(correlHubway, correlHubway, endTime)
        doc.wasDerivedFrom(correlHubway, resource_popHubway, correlationHubway, correlationHubway, correlationHubway)

        repo.logout()
                  
        return doc
    
if __name__ == "__main__":
    correlationHubway.execute()
    doc = correlationHubway.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))
