import urllib.request
import json
import geojson
import dml
import prov.model
import datetime
import uuid


class HUDincomelimits(dml.Algorithm):
    contributor = 'colinstu'
    reads = []
    writes = ['colinstu.HUDincome']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')
        url = 'https://mapc-admin.carto.com/api/v2/sql?q=SELECT%20a.seq_id%2Ca.muni_id%2Ca.municipal%2Ca.countyname%2Ca.areaname%2Ca.fy_year%2Ca.median%2Ca.il_50_1%2Ca.il_50_2%2Ca.il_50_3%2Ca.il_50_4%2Ca.il_50_5%2Ca.il_50_6%2Ca.il_50_7%2Ca.il_50_8%2Ca.il_30_1%2Ca.il_30_2%2Ca.il_30_3%2Ca.il_30_4%2Ca.il_30_5%2Ca.il_30_6%2Ca.il_30_7%2Ca.il_30_8%2Ca.il_80_1%2Ca.il_80_2%2Ca.il_80_3%2Ca.il_80_4%2Ca.il_80_5%2Ca.il_80_6%2Ca.il_80_7%2Ca.il_80_8%2C%20b.the_geom%2C%20b.the_geom_webmercator%20%20FROM%20hous_section8_income_limits_by_year_m%20a%20%20INNER%20JOIN%20ma_municipalities%20b%20ON%20a.muni_id%20%3D%20b.muni_id%20WHERE%20a.fy_year%20IN%20(%272017%27)&format=geojson&filename=hous_section8_income_limits_by_year_m'
        response = urllib.request.urlopen(url).read().decode("utf-8")

        r = geojson.loads(response)
        s = geojson.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("HUDincome")
        repo.createCollection("HUDincome")
        repo['colinstu.HUDincome'].insert_many(r)  # TODO: fix insert many
        repo['colinstu.HUDincome'].metadata({'complete': True})
        print(repo['colinstu.HUDincome'].metadata())

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
        repo.authenticate('colinstu', 'colinstu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset/active-food-establishment-licenses')

        this_script = doc.agent('alg:colinstu#HUDincomelimits',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label': 'HUD Income Limits',
                                                prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_HUDincome = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_HUDincome, this_script)

        doc.usage(get_HUDincome, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'  # TODO: fix query
                   }
                  )

        HUDincome = doc.entity('dat:colinstu#HUDincomelimits', {prov.model.PROV_LABEL: 'Active Food Establishment Licenses',
                                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(HUDincome, this_script)
        doc.wasGeneratedBy(HUDincome, get_HUDincome, endTime)
        doc.wasDerivedFrom(HUDincome, resource, get_HUDincome, get_HUDincome, get_HUDincome)

        repo.logout()

        return doc


HUDincomelimits.execute()
doc = HUDincomelimits.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
