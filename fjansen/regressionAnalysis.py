import pprint
import datetime
import dml
import prov.model
import uuid
import pandas as pd
import numpy as np
import statsmodels.api as sm


### Algorithm 1

class regressionAnalysis(dml.Algorithm):
    contributor = 'fjansen'
    reads = ['fjansen.OTP_by_line', 'fjansen.stops', 'fjansen.stopsVsLines']
    writes = ['fjansen.regressionAnalysis']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('fjansen', 'fjansen')

        # Read data from mongo
        mbta_db = repo['fjansen.OTP_by_line']
        stops_db = repo['fjansen.stops']
        stopsVsLines_db = repo['fjansen.stopsVsLines']

        # Read data into pandas
        print("Loading OTP Data")
        otpData = mbta_db.find_one()
        del otpData['_id']
        otp_by_line = pd.DataFrame.from_dict(otpData)
        otp_by_line = otp_by_line.transpose()
        otp_by_line[otp_by_line['Peak Service'] == ''] = np.nan
        otp_by_line[otp_by_line['Off-Peak Service'] == ''] = np.nan

        print("Loading Stops Data")
        stopsData = stops_db.find_one()
        del stopsData['_id']
        stops = pd.DataFrame.from_dict(stopsData)

        print("Loading Stops By Line Data")
        stop_by_line_data = stopsVsLines_db.find_one()
        del stop_by_line_data['_id']
        stop_by_line = pd.DataFrame([(key, x) for key, val in stop_by_line_data.items() for x in val],
                                    columns=['Name', 'Values'])
        stop_by_line.columns = ['Route', 'Stop']
        stop_by_line = stop_by_line.set_index('Stop')

        print("Joining Stops By Line with Stops")
        stop_route_neighborhood = stop_by_line.join(stops.set_index('stop_id'), how='left')
        stop_route_neighborhood = stop_route_neighborhood[stop_route_neighborhood['neighborhood'].notnull()]
        stop_route_neighborhood['stop_id'] = stop_route_neighborhood.index
        merged = pd.merge(otp_by_line, stop_route_neighborhood, left_index=True, right_on='Route', how='right')
        merged_stop = merged[merged['Peak Service'].notnull()]

        print("Creating dummy data")
        stop_dummy_city = pd.get_dummies(merged_stop['city'])
        stop_dummy_neighborhood = pd.get_dummies(merged_stop['neighborhood'])
        merged_dummy_city = merged_stop.join(stop_dummy_city)
        merged_dummy_city_final = merged_dummy_city.groupby('Route').max()
        x_cols = merged_dummy_city_final.columns[8:]
        y_cols = 'Off-Peak Service'

        print("Creating regression model")
        model = sm.GLM(merged_dummy_city_final[y_cols], merged_dummy_city_final[x_cols], family=sm.families.Gaussian())
        results = model.fit()
        resultsKeys = results.params.keys()

        coefficients = {}
        for key in resultsKeys:
            coefficients[key] = results.params[key]

        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(coefficients)

        print("Saving coefficients")

        repo.dropCollection('regressionAnalysis')
        repo.createCollection('regressionAnalysis')
        repo['fjansen.regressionAnalysis'].insert_one(coefficients)

        print("Done!")
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client = dml.pymongo.MongoClient()
        repo = client.repo

        ##########################################################

        ## Namespaces
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts in / format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets in / format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log#')  # The event log.

        ## Agents
        this_script = doc.agent('alg:fjansen#regressionAnalysis',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        ## Activities
        get_regressionAnalysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        ## Entitites
        # Data Source
        resource1 = doc.entity('dat:otp_by_line.json',
                               {'prov:label': 'Trickle', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        resource2 = doc.entity('dat:stops.json', {'prov:label': 'Stops', prov.model.PROV_TYPE: 'ont:DataResource',
                                                  'ont:Extension': 'json'})
        resource3 = doc.entity('dat:stops_vs_lines.json',
                               {'prov:label': 'Stops vs Lines', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        # Data Generated
        regressionAnalysis = doc.entity('dat:fjansen#regressionAnalysis', {prov.model.PROV_LABEL: 'Regression Analysis',
                                                                           prov.model.PROV_TYPE: 'ont:DataSet'})

        ############################################################

        ## wasAssociatedWith
        doc.wasAssociatedWith(get_regressionAnalysis, this_script)

        ## used
        doc.usage(get_regressionAnalysis, resource1, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', })
        doc.usage(get_regressionAnalysis, resource2, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', })
        doc.usage(get_regressionAnalysis, resource3, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', })

        ## wasGeneratedBy
        doc.wasGeneratedBy(regressionAnalysis, get_regressionAnalysis, endTime)

        ## wasAttributedTo
        doc.wasAttributedTo(regressionAnalysis, this_script)

        ## wasDerivedFrom
        doc.wasDerivedFrom(regressionAnalysis, resource1, get_regressionAnalysis, get_regressionAnalysis,
                           get_regressionAnalysis)
        doc.wasDerivedFrom(regressionAnalysis, resource2, get_regressionAnalysis, get_regressionAnalysis,
                           get_regressionAnalysis)
        doc.wasDerivedFrom(regressionAnalysis, resource3, get_regressionAnalysis, get_regressionAnalysis,
                           get_regressionAnalysis)

        ############################################################

        repo.logout()

        return doc
