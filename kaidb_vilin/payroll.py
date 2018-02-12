
import urllib.request
import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid

class payroll(dml.Algorithm):
    contributor = 'kaidb_vilin'
    reads = []
    writes = ['kaidb_vilin.payroll']
    DEBUG = True

    @staticmethod
    def clean_data(df):
        """
        - @args: df -- dataframe containing csv formated boston payroll data. 
        - @returns  -- cleaned and formated Boston payroll dataframe.  
        Boston Payroll Data has missing values to indicate an absence of a payment. 
        it also represents paychecks with $DOLLAR_AMOUNT. This function pluts the 
        data in a usable formate by replacing NAANs with 0 and $x with x. It then sorts the idnex
        by descending total_pay

        """
        ################# clean data #####################
        # NAN in this case means no money was given
        data = df.fillna(0)
        # Convert dollars to numeric/float
        pay_info = ['REGULAR', 'RETRO', 'OTHER',
                    'OVERTIME', 'INJURED', 'DETAIL', 
                    'QUINN/EDUCATION INCENTIVE', 
                    'TOTAL EARNINGS']
        for col in pay_info:
            data[col]  = data[col].replace('[\$,]', '', regex=True).astype(float).fillna(0)

        # Sort by earnings (descending )
        return data.sort_values("TOTAL EARNINGS", ascending=False)
        ################# End Clean Data ################



    @staticmethod
    def execute(trial = False, custom_url=None):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        # authenticate db for user 'kaidb_vilin'
        repo.authenticate('kaidb_vilin', 'kaidb_vilin')

        if custom_url:
            # You can pass a custom url
            payroll_url = custom_url
        payroll_url = "https://data.boston.gov/dataset/418983dc-7cae-42bb-88e4-d56f5adcf869/resource/8368bd3d-3633-4927-8355-2a2f9811ab4f/download/employee-earnings-report-2016.csv"

        # retrieve data using pandas 
        data = pd.read_csv(payroll_url, encoding = "ISO-8859-1")
        # puts data in useable format 
        data = payroll.clean_data(data)

        # Obj transformation: 
        #  df --> string (json formated) --> json 
        r = json.loads(data.to_json( orient='index'))
        # dump json. 
        # Keys should already be sorted

        s = json.dumps(r, sort_keys=True, indent=2)
        
        repo.dropCollection("payroll")
        repo.createCollection("payroll")

        if payroll.DEBUG:
            print(type(r))
            assert isinstance(r, dict)

        repo['kaidb_vilin.payroll'].insert_many( [dict(r)])

        repo['kaidb_vilin.payroll'].metadata({'complete':True})
        print(repo['kaidb_vilin.payroll'].metadata())

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
        repo.authenticate(contributor, contributor)
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

# comment this out for submission. 
payroll.execute()
#doc = example.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
