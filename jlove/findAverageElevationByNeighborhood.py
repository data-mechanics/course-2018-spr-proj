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

class findAverageElevationByNeighborhood(dml.Algorithm):
    contributor = 'jlove'
    reads = ['jlove.contours', 'jlove.neighborhoods']
    writes = ['jlove.contour_neighborhoods', 'jlove.avg_elev']
    
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
                weighted_elev += [feature['properties']['elev'] * feature['properties']['elev']]
            
            avg_elevation[nh] = np.average(weighted_elev)
            
        print(avg_elevation)
        
        repo.dropCollection('jlove.avg_elev')
        repo.createCollection('jlove.avg_elev')
        repo['jlove.avg_elev'].insert_many(to_save)
            

        
            
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
        
        
        this_script = doc.agent('alg:jlove#normalizeIncomeData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource1 = doc.entity('dat:jlove#income', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        normalize_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(normalize_income, this_script)
        doc.usage(normalize_income, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        resource2 = doc.entity('dat:jlove#neighborhoods', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        doc.wasAssociatedWith(normalize_income, this_script)
        doc.usage(normalize_income, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        resource3 = doc.entity('dat:jlove#incomeNormalized', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.wasAssociatedWith(normalize_income, this_script)
        doc.usage(normalize_income, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        income_normalized = doc.entity('dat:jlove#incomeNormalized', {prov.model.PROV_LABEL:'Normalized Boston Median Household Income by Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income_normalized, this_script)
        doc.wasGeneratedBy(income_normalized, normalize_income, endTime)
        doc.wasDerivedFrom(income_normalized, resource1, normalize_income, normalize_income, normalize_income)
        
        nh_income = doc.entity('dat:jlove#nhWithIncome', {prov.model.PROV_LABEL:'Boston Neighborhood Borders with Information About Median Household Income', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(nh_income, this_script)
        doc.wasGeneratedBy(nh_income, normalize_income, endTime)
        doc.wasDerivedFrom(nh_income, resource3, normalize_income, normalize_income, normalize_income)
        doc.wasDerivedFrom(nh_income, resource2, normalize_income, normalize_income, normalize_income)


        repo.logout()
                  
        return doc