import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from vincenty import vincenty

class subway_hospital(dml.Algorithm):
    contributor = 'aolzh'
    reads = ['aolzh.NewYorkSubway', 'aolzh.NewYorkHospitals','aolzh.NewYorkSchool', 'aolzh.NewYorkLibrary']
    writes = ['aolzh.NewYorkSubway_Hospitals_School_Library']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aolzh', 'aolzh')

        newyorksubway = repo.aolzh.NewYorkSubway
        newyorkhospital = repo.aolzh.NewYorkHospitals
        newyorkschool = repo.aolzh.NewYorkSchool
        newyorklibrary = repo.aolzh.NewYorkLibrary

        subway = newyorksubway.find()
        hospital = newyorkhospital.find()
        school = newyorkschool.find()
        library = newyorklibrary.find()

        nyc_subway_hospital_school_library = []
        
        for s in subway:
            dis_1 = 100
            dis_2 = 100
            dis_3 = 100
            for h in hospital:
                location_s = (float(s['the_geom']['coordinates'][1]), float(s['the_geom']['coordinates'][0]))
                location_h = (float(h['location_1']['latitude']),float(h['location_1']['longitude']))
                if vincenty(location_s,location_h,miles=True) < dis_1:
                    dis_1 = vincenty(location_s,location_h,miles=True)
                    name_1 = h['facility_name']
            for sc in school:
                location_s = (float(s['the_geom']['coordinates'][1]), float(s['the_geom']['coordinates'][0]))
                location_sc = (float(sc['the_geom']['coordinates'][1]), float(sc['the_geom']['coordinates'][0]))
                if vincenty(location_s,location_sc,miles=True) < dis_1:
                    dis_2 = vincenty(location_s,location_sc,miles=True)
                    name_2 = sc['name']
            for l in library:
                location_s = (float(s['the_geom']['coordinates'][1]), float(s['the_geom']['coordinates'][0]))
                location_l = (float(l['the_geom']['coordinates'][1]), float(l['the_geom']['coordinates'][0]))
                if vincenty(location_s,location_l,miles=True) < dis_1:
                    dis_3 = vincenty(location_s,location_l,miles=True)
                    name_3 = l['name']
            dis = dis_1+dis_2+dis_3
            nyc_subway_hospital_school_library.append({'subway_name':s['name'],'hospital':name_1,'school':name_2,'library':name_3,'distance':dis})
        repo.dropCollection("NewYorkSubway_Hospitals_School_Library")
        repo.createCollection("NewYorkSubway_Hospitals_School_Library")
        repo["aolzh.NewYorkSubway_Hospitals_School_Library"].insert_many(nyc_subway_hospital_school_library)
        print("Finished")

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
        repo.authenticate('aolzh', 'aolzh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('nyc', 'https://data.cityofnewyork.us/resource/')

        subway_hospital_school_library_script = doc.agent('alg:aolzh#subway_hospital_school_library', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        get_newyork_subway_hospital_school_library = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        newyorksubway = doc.entity('dat:aolzh#NewYorkSubway', {prov.model.PROV_LABEL:'NewYork Subways', prov.model.PROV_TYPE:'ont:DataSet'})
        newyorkhospital = doc.entity('dat:aolzh#NewYorkHospital', {prov.model.PROV_LABEL:'NewYork Hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        newyorkschool = doc.entity('dat:aolzh#NewYorkSchool', {prov.model.PROV_LABEL:'NewYork School', prov.model.PROV_TYPE:'ont:DataSet'})
        newyorklibrary = doc.entity('dat:aolzh#NewYorkLibrary', {prov.model.PROV_LABEL:'NewYork Library', prov.model.PROV_TYPE:'ont:DataSet'})
        newyork_subway_hospital_school_library = doc.entity('dat:aolzh#NewYorkSubway_Hospitals_School_Library', {prov.model.PROV_LABEL:'NewYork Subway Hospital School Library', prov.model.PROV_TYPE:'ont:DataSet'})

        
        doc.wasAttributedTo(newyork_subway_hospital_school_library, subway_hospital_school_library_script)
        doc.wasGeneratedBy(newyork_subway_hospital_school_library, get_newyork_subway_hospital_school_library, endTime)
        doc.wasDerivedFrom(newyorksubway, newyorkhospital,get_newyork_subway_hospital_school_library, get_newyork_subway_hospital_school_library, get_newyork_subway_hospital_school_library)

       

        repo.logout()
                  
        return doc

subway_hospital.execute()
doc = subway_hospital.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
