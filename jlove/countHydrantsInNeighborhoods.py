import urllib.request
import dml
import prov.model
import datetime
import uuid
import shapely.geometry
import geojson

class countHydrantsInNeighborhoods(dml.Algorithm):
    contributor = 'jlove'
    reads = ['jlove.hydrants', 'jlove.neighborhoods']
    writes = ['jlove.hydrantCounts', 'jlove.hydrantGroups']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')
        hydrants = repo['jlove.hydrants'].find_one({})
        neighborhoods = repo['jlove.neighborhoods'].find_one({})
        
        #group fire hydrants by neighborhood
        featureCollections = []
        for feature in neighborhoods['features']:
            hydrantsIn = []
            name = feature['properties']['Name']
            shape = shapely.geometry.shape(feature['geometry'])
            for hydrant in hydrants['features']:
                point = shapely.geometry.shape(hydrant['geometry'])
                if shape.contains(point):
                    hydrantsIn += [hydrant]
            
            featureCollection = geojson.FeatureCollection(hydrantsIn)
            featureCollection['metadata'] = {'neighborhood': name}
            featureCollections += [featureCollection]
        
        
        repo.dropCollection('hydrantGroups')
        repo.createCollection('hydrantGroups')
        repo['jlove.hydrantGroups'].insert_many(featureCollections)
        repo['jlove.hydrantGroups'].metadata({'complete':True})
        
        
        #Count hydrants in each neighborhood
        hydrantGroups = repo['jlove.hydrantGroups'].find({})
        
        counts = {}
        
        for group in hydrantGroups:
            neighborhood = group['metadata']['neighborhood']
            count = len(group['features'])
            counts[neighborhood] = count
        
        repo.dropCollection('hydrantCounts')
        repo.createCollection('hydrantCounts')
        
        repo['jlove.hydrantCounts'].insert_one(counts)
        repo['jlove.hydrantCounts'].metadata({'complete':True})
                
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
        
        
        this_script = doc.agent('alg:jlove#countHydrantsInNeighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource1 = doc.entity('dat:jlove#hydrants', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        count_hydrants = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(count_hydrants, this_script)
        doc.usage(count_hydrants, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        resource2 = doc.entity('dat:jlove#neighborhoods', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        doc.wasAssociatedWith(count_hydrants, this_script)
        doc.usage(count_hydrants, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        resource3 = doc.entity('dat:jlove#hydrantGroups', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        doc.wasAssociatedWith(count_hydrants, this_script)
        doc.usage(count_hydrants, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        hydrant_groups = doc.entity('dat:jlove#hydrantGroups', {prov.model.PROV_LABEL:'Boston Fire Hydrants Grouped by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hydrant_groups, this_script)
        doc.wasGeneratedBy(hydrant_groups, count_hydrants, endTime)
        doc.wasDerivedFrom(hydrant_groups, resource1, count_hydrants, count_hydrants, count_hydrants)
        doc.wasDerivedFrom(hydrant_groups, resource2, count_hydrants, count_hydrants, count_hydrants)
        
        hydrant_counts = doc.entity('dat:jlove#hydrantCounts', {prov.model.PROV_LABEL:'Number of Fire Hydrants Per Boston Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hydrant_counts, this_script)
        doc.wasGeneratedBy(hydrant_counts, count_hydrants, endTime)
        doc.wasDerivedFrom(hydrant_counts, resource3, count_hydrants, count_hydrants, count_hydrants)


        repo.logout()
                  
        return doc