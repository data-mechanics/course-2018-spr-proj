#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  5 22:32:14 2018

@author: jlove
"""

import dml
import prov.model
import datetime
import uuid
import shapely.geometry
import numpy as np
import geojson
import scipy.stats

class findAverageElevationByNeighborhood(dml.Algorithm):
    contributor = 'jlove'
    reads = ['jlove.contours', 'jlove.neighborhoods', 'jlove.incomes']
    writes = ['jlove.contour_neighborhoods', 'jlove.avg_elev', 'jlove.stats_result']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jlove', 'jlove')


        
        neighborhoods = repo['jlove.neighborhoods'].find_one({})
        contours = repo['jlove.contours'].find({})
        
        neighborhood_overlap = {}
        
        for contour in contours:
            for neighborhood in neighborhoods['features']:
                nShape = shapely.geometry.shape(neighborhood['geometry'])
                cShape = shapely.geometry.shape(contour['geometry'])
                name = neighborhood['properties']['Name']
                overlap = nShape.intersection(cShape)
                
                if overlap.length == 0:
                    continue
                else:
                    geo = geojson.LineString(overlap.coords)
                    feature = geojson.Feature(name, geo, {})
                    feature['properties']['neighborhood'] = name
                    feature['properties']['elev'] = contour['properties']['ELEV']
                    feature['properties']['length'] = overlap.length
                    if neighborhood in neighborhood_overlap:
                        neighborhood_overlap[neighborhood] += [feature]
                    else:
                        neighborhood_overlap[neighborhood] = [feature]
        
        to_save = []
        for key in neighborhood_overlap:
            collection = geojson.FeatureCollection(neighborhood_overlap[key])
            doc = {'neighborhood': key, 'geo': collection}
            to_save += [doc]
        
        repo.dropCollection('jlove.contour_neighborhoods')
        repo.createCollection('jlove.contour_neighborhoods')
        repo['jlove.contour_neighborhoods'].insert_many(to_save)
        
        
        
        #Now calculate 
        
        contours = repo['jlove.contour_neighborhoods'].find()
        
        avg_elevation = {}
        
        for contour in contours:
            nh = contour['neighborhood']
            features = contour['geo']['features']
            weighted_elev = []
            for feature in features:
                weighted_elev += [feature['properties']['length'] * feature['properties']['elev']]
            
            avg_elevation[nh] = np.average(weighted_elev)
            
        
        repo.dropCollection('jlove.avg_elev')
        repo.createCollection('jlove.avg_elev')
        repo['jlove.avg_elev'].insert_one(avg_elevation)
        
        
        contour_nh = repo['jlove.avg_elev'].find_one({})
        incomes = repo['jlove.incomes'].find({})
        
        pairs = []
        for income in incomes:
            pairs[income['Neighborhood']] = {'income': income['Median Household Income']}
            pairs += [income['Median Household Income'], [contour_nh[income['Neighborhood']]]]
        
        
        pair_arr = np.array(pairs)
        results = scipy.stats.pearsonr(pair_arr[::,0], pair_arr[::,1])
        
        doc = {'corr_cooef': results[0], 'p_val': results[1]}
        
        repo.dropCollectio('jlove.stats_result')
        repo.createCollectio('jlove.stats_result')
        
        repo['jlove.stats_result'].insert_one(doc)

        
            
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
        
        
        this_script = doc.agent('alg:jlove#findAverageElevationByNeighborhood', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource1 = doc.entity('dat:jlove#contours', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        neighborhood_contours = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(neighborhood_contours, this_script)
        doc.usage(neighborhood_contours, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        resource2 = doc.entity('dat:jlove#neighborhoods', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        doc.wasAssociatedWith(neighborhood_contours, this_script)
        doc.usage(neighborhood_contours, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        resource3 = doc.entity('dat:jlove#contour_neighborhoods', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        doc.wasAssociatedWith(neighborhood_contours, this_script)
        doc.usage(neighborhood_contours, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        
        resource4 = doc.entity('dat:jlove#incomes', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.wasAssociatedWith(neighborhood_contours, this_script)
        doc.usage(neighborhood_contours, resource4, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        
        resource5 = doc.entity('dat:jlove#avg_elev', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.wasAssociatedWith(neighborhood_contours, this_script)
        doc.usage(neighborhood_contours, resource5, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        contour_neighborhoods = doc.entity('dat:jlove#contour_neighborhoods', {prov.model.PROV_LABEL:'Boston 1 ft. Countour Lines Grouped by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(contour_neighborhoods, this_script)
        doc.wasGeneratedBy(contour_neighborhoods,neighborhood_contours, endTime)
        doc.wasDerivedFrom(contour_neighborhoods, resource1, neighborhood_contours, neighborhood_contours, neighborhood_contours)
        doc.wasDerivedFrom(contour_neighborhoods, resource2, neighborhood_contours, neighborhood_contours, neighborhood_contours)
        
        avg_elev = doc.entity('dat:jlove#avg_elev', {prov.model.PROV_LABEL:'Boston Average Elevation by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(avg_elev, this_script)
        doc.wasGeneratedBy(avg_elev, neighborhood_contours, endTime)
        doc.wasDerivedFrom(avg_elev, resource3, neighborhood_contours, neighborhood_contours, neighborhood_contours)
        
        stat_result = doc.entity('dat:jlove#stats_result', {prov.model.PROV_LABEL:'Results of Statistic Analysis', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(stat_result, this_script)
        doc.wasGeneratedBy(stat_result, neighborhood_contours, endTime)
        doc.wasDerivedFrom(stat_result, resource4, neighborhood_contours, neighborhood_contours, neighborhood_contours)
        doc.wasDerivedFrom(stat_result, resource5, neighborhood_contours, neighborhood_contours, neighborhood_contours)


        repo.logout()
                  
        return doc