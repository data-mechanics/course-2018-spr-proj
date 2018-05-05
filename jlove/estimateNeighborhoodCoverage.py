import dml
import prov.model
import datetime
import uuid
import shapely.geometry
import numpy
from tqdm import tqdm

class estimateNeighborhoodCoverage(dml.Algorithm):
    contributor = 'jlove'
    reads = ['jlove.hydrantGroups', 'jlove.flood', 'jlove.hydrantCounts']
    writes = ['jlove.percentCovered']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        flood = repo['jlove.flood'].find_one({})
        hydrantCounts = repo['jlove.hydrantCounts'].find_one({})
        hydrantGroups = repo['jlove.hydrantGroups'].find({})

        print(hydrantCounts)

        floodShape = shapely.geometry.shape(flood['features'][0]['geometry'])
        covered = {}
        hgroups = []
        for group in hydrantGroups:
            hgroups += [group]

        hydrantGroups = hgroups

        for group in tqdm(hydrantGroups):
            name = group['metadata']['neighborhood']
            count = 0
            for hydrant in tqdm(group['features']):
                point = shapely.geometry.shape(hydrant['geometry'])
                if floodShape.contains(point):
                    count += 1
            covered[name] = count
        print(covered)
        
        for neighborhood in hydrantCounts.keys():
            if neighborhood == '_id':
                continue
            print(neighborhood)
            temp_covered = covered[neighborhood]
            try:
                covered[neighborhood] = (temp_covered/hydrantCounts[neighborhood]) * 100
            except ZeroDivisionError:
                covered[neighborhood] = 0
        
        repo.dropCollection('percentCovered')
        repo.createCollection('percentCovered')
        
        repo['jlove.percentCovered'].insert_one(covered)
        
        repo.logout()
        
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
        
        
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/dataset/')
        
        
        this_script = doc.agent('alg:jlove#estimateNeighborhoodCoverage', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource1 = doc.entity('dat:jlove#hydrantGroups', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        estimate_coverage = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(estimate_coverage, this_script)
        doc.usage(estimate_coverage, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        resource2 = doc.entity('dat:jlove#flood', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        doc.wasAssociatedWith(estimate_coverage, this_script)
        doc.usage(estimate_coverage, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        resource3 = doc.entity('dat:jlove#hydrantCounts', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.wasAssociatedWith(estimate_coverage, this_script)
        doc.usage(estimate_coverage, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        percent_covered = doc.entity('dat:jlove#percentCovered', {prov.model.PROV_LABEL:'Estimated Percentage of Neighborhood Flooded', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(percent_covered, this_script)
        doc.wasGeneratedBy(percent_covered, estimate_coverage, endTime)
        doc.wasDerivedFrom(percent_covered, resource1, estimate_coverage, estimate_coverage, estimate_coverage)
        doc.wasDerivedFrom(percent_covered, resource2, estimate_coverage, estimate_coverage, estimate_coverage)
        doc.wasDerivedFrom(percent_covered, resource3, estimate_coverage, estimate_coverage, estimate_coverage)
       


        repo.logout()
                  
        return doc