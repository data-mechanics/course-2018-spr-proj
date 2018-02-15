
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
    DEBUG = False

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
        r = json.loads(data.to_json( orient='records'))
        # dump json. 
        # Keys should already be sorted

        s = json.dumps(r, sort_keys=True, indent=2)
        
        repo.dropCollection("payroll")
        repo.createCollection("payroll")

        #if payroll.DEBUG:
         #   print(type(r))
          #  assert isinstance(r, dict)

        repo['kaidb_vilin.payroll'].insert_many( r)

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
        repo.authenticate('kaidb_vilin', 'kaidb_vilin')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dbd', 'https://data.boston.gov/dataset/')
        doc.add_namespace('rc', 'employee-earnings-report-2016')

        this_script = doc.agent('alg:kaidb_vilin#payroll', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('dbd:rc', {'prov:label':'rc', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_payroll = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_payroll, this_script)
        
        # How to retrieve the .csv
        doc.usage(get_payroll, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'418983dc-7cae-42bb-88e4-d56f5adcf869/resource/8368bd3d-3633-4927-8355-2a2f9811ab4f/download/employee-earnings-report-2016.csv'
                  }
                  )
     

        payroll = doc.entity('dat:kaidb_vilin#payroll', {prov.model.PROV_LABEL:'Payroll', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(payroll, this_script)
        doc.wasGeneratedBy(payroll, get_payroll, endTime)
        doc.wasDerivedFrom(payroll, resource, get_payroll, get_payroll, get_payroll)
        repo.logout()
                  
        return doc

# comment this out for submission. 
payroll.execute()
doc = payroll.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
