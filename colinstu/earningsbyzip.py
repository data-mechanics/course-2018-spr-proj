import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class eaningsbyzip(dml.Algorithm):
    contributor = 'colinstu'
    reads = []
    writes = ['colinstu.earningsbyzip']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')
        repo.dropCollection("earningsbyzip")
        repo.createCollection("earningsbyzip")
        earnings = repo['colinstu.employeeearnings'].find()
        def zipearnings(earnings):
            zipearnings = {}
            for document in earnings:
                data = dict(document)
                zipearnings[data['POSTAL']] = data['TOTAL EARNINGS'] # creates dict of all earnings by zip code
            return zipearnings

        zipearnings = zipearnings(earnings)
        s = json.dumps(zipearnings, sort_keys=True, indent=2)
        r = json.loads(s)
        # test below
        r = dict((k, float(v[1:].replace(',',''))) for k, v in r.items()) # converts string income values to float

        def inboston(zip):
            url = 'http://datamechanics.io/data/colinstu/bostonzips.csv'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            bostonzip = response.split('\r\n')
            newdict = {}
            for k, v in zip.items():
                if k in bostonzip:
                    newdict[k] = v
            return newdict

        bosincome = inboston(r)

        def average(incomes):
            avg = {}
            for k,v in incomes.items():
                if isinstance(v, float):
                    avg[k] = v
                else:
                    avg[k] = sum(v)/len(v)
            return avg

        avgincome = average(bosincome)


        bostonzip = repo['colinstu.bostonzip'].find()
        def zipconvert(bostonzip, avgincome):
            newdict = {}
            newdict['income'] = avgincome
            newdict['geometry'] = {}
            for document in bostonzip:
                data = dict(document)
                curzip = data['properties']['ZIP5']
                if curzip in avgincome.keys():
                    newdict['geometry'][curzip] = data['geometry']
            return newdict
        bostonzip = zipconvert(bostonzip, avgincome)

        repo['colinstu.zipearnings'].insert_many([bostonzip])
        repo['colinstu.zipearnings'].metadata({'complete': True})
        print(repo['colinstu.zipearnings'].metadata())

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

        this_script = doc.agent('alg:colinstu#earningsbyzip',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': 'Average Earnings by Zipcode', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        getearningsbyzip = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(getearningsbyzip, this_script)
        doc.usage(getearningsbyzip, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   }
                  )

        earningsbyzip = doc.entity('dat:colinstu#earningsbyzip',
                          {prov.model.PROV_LABEL: 'Average Earnings by Zipcode', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(earningsbyzip, this_script)
        doc.wasGeneratedBy(earningsbyzip, getearningsbyzip, endTime)
        doc.wasDerivedFrom(earningsbyzip, resource, getearningsbyzip, getearningsbyzip, getearningsbyzip)

        repo.logout()


eaningsbyzip.execute()
doc = eaningsbyzip.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
