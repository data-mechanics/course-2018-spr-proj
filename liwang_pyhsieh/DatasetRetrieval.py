import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import random

def format_time(str):
    res = str
    if res[:2][1] == ":":
        res = "0" + res
    if res[:2] == "00":
        res = "12" + res[2:]
    return res

def time_validity(str):
    hr = int(str[:2])
    min = int(str[3:5])
    return 0 <= hr < 24 and 0 <= min < 60

class DatasetRetrieval(dml.Algorithm):
    contributor = 'liwang_pyhsieh'
    reads = []
    writes = ['liwang_pyhsieh.crash_2015', 'liwang_pyhsieh.hospitals', 'liwang_pyhsieh.police_stations',
              'liwang_pyhsieh.street_lights', 'liwang_pyhsieh.traffic_signals']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        # Crash_2015
        url = 'http://datamechanics.io/data/liwang_pyhsieh/crash_2015.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        r_c = []
        # Data cleaning
        for item in r:
            item["Crash Time"] = format_time(item["Crash Time"])
            if item["X Coordinate"] is not None and item["Y Coordinate"] is not None and time_validity(item["Crash Time"]):
                r_c.append(item)
        if trial:
            r_c = random.sample(r_c, 300)

        repo.dropCollection("crash_2015")
        repo.createCollection("crash_2015")
        repo['liwang_pyhsieh.crash_2015'].insert_many(r_c)
        repo['liwang_pyhsieh.crash_2015'].metadata({'complete': True})

        # Hospitals
        url = 'http://datamechanics.io/data/liwang_pyhsieh/hospitals.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("hospitals")
        repo.createCollection("hospitals")
        repo['liwang_pyhsieh.hospitals'].insert_many(r)
        repo['liwang_pyhsieh.hospitals'].metadata({'complete': True})

        # Police_station
        url = 'http://datamechanics.io/data/liwang_pyhsieh/Boston_Police_Stations.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("police_stations")
        repo.createCollection("police_stations")
        repo['liwang_pyhsieh.police_stations'].insert_many(r)
        repo['liwang_pyhsieh.police_stations'].metadata({'complete': True})

        # Street_lights
        url = 'http://datamechanics.io/data/liwang_pyhsieh/street_lights.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("street_lights")
        repo.createCollection("street_lights")
        repo['liwang_pyhsieh.street_lights'].insert_many(r)
        repo['liwang_pyhsieh.street_lights'].metadata({'complete': True})

        # Traffic Signals
        url = 'https://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("traffic_signals")
        repo.createCollection("traffic_signals")
        repo['liwang_pyhsieh.traffic_signals'].insert_many(r["features"])
        repo['liwang_pyhsieh.traffic_signals'].metadata({'complete': True})

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
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/')
        doc.add_namespace('mcp', 'https://services.massdot.state.ma.us/crashportal/')
        doc.add_namespace('bag', 'http://bostonopendata-boston.opendata.arcgis.com/')

        this_script = doc.agent('alg:liwang_pyhsieh/#datast-retrieval', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        crash_2015_resource = doc.entity('mcp:crash-2015', {'prov:label': '2015 Massachusetts Crash Report', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        hospitals_resource = doc.entity('bdp:hospitals', {'prov:label': 'Boston hospital information', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        police_stations_resource = doc.entity('bdp:police-stations', {'prov:label': 'Boston police station information', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        street_lights_resource = doc.entity('bag:street-lights', {'prov:label': 'Boston street light locations', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        traffic_signals_resource = doc.entity('bdp:traffic-signals', {'prov:label': 'Boston traffic signal locations', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})

        get_crash_2015 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_hospitals = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_police_stations = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_street_lights = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_traffic_signals = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_crash_2015, this_script)
        doc.wasAssociatedWith(get_hospitals, this_script)
        doc.wasAssociatedWith(get_police_stations, this_script)
        doc.wasAssociatedWith(get_street_lights, this_script)
        doc.wasAssociatedWith(get_traffic_signals, this_script)

        doc.usage(get_crash_2015, crash_2015_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', })
        doc.usage(get_hospitals, hospitals_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', })
        doc.usage(get_police_stations, police_stations_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', })
        doc.usage(get_street_lights, street_lights_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', })
        doc.usage(get_traffic_signals, traffic_signals_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', })

        crash_2015 = doc.entity('dat:liwang_pyhsieh#crash_2015',
                                {prov.model.PROV_LABEL: '2015 Massachusetts Crash Report',
                                 prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crash_2015, this_script)
        doc.wasGeneratedBy(crash_2015, get_crash_2015, endTime)
        doc.wasDerivedFrom(crash_2015, crash_2015_resource, crash_2015, crash_2015, crash_2015)

        hospitals = doc.entity('dat:liwang_pyhsieh#hospitals',
                               {prov.model.PROV_LABEL: 'Boston hospital information',
                                prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(hospitals, this_script)
        doc.wasGeneratedBy(hospitals, get_hospitals, endTime)
        doc.wasDerivedFrom(hospitals, hospitals_resource, hospitals, hospitals, hospitals)

        police_stations = doc.entity('dat:liwang_pyhsieh#police_stations',
                                     {prov.model.PROV_LABEL: 'Boston police station information',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(police_stations, this_script)
        doc.wasGeneratedBy(police_stations, get_police_stations, endTime)
        doc.wasDerivedFrom(police_stations, police_stations_resource, police_stations, police_stations, police_stations)

        street_lights = doc.entity('dat:liwang_pyhsieh#street_lights',
                                   {prov.model.PROV_LABEL: 'Boston street light locations',
                                    prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(street_lights, this_script)
        doc.wasGeneratedBy(street_lights, get_street_lights, endTime)
        doc.wasDerivedFrom(street_lights, street_lights_resource, street_lights, street_lights, street_lights)

        traffic_signals = doc.entity('dat:liwang_pyhsieh#traffic_signals',
                                     {prov.model.PROV_LABEL: 'Boston traffic signals locations',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(traffic_signals, this_script)
        doc.wasGeneratedBy(traffic_signals, get_traffic_signals, endTime)
        doc.wasDerivedFrom(traffic_signals, traffic_signals_resource, traffic_signals, traffic_signals, traffic_signals)

        repo.logout()

        return doc


if __name__ == "__main__":
    DatasetRetrieval.execute()
    doc = DatasetRetrieval.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
