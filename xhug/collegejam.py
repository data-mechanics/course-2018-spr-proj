import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class collegejam(dml.Algorithm):
	contributor = 'xhug'
	reads = ['xhug.trafficejam','xhug.bostoncolleges']
	writes = ['xhug.collegeandjam']


def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]



	@staticmethod
	def execute(trial = False):


		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.autenticate("xhug", "xhug")
		jam = repo.xhug.trafficejam
		colleges = repo.xhug.bostoncolleges

		'''cleanse the data to contain only the streets of jam
		'''

		jamlocations = project(jam, lambda v: (v[street]))

		def getschooladdress(school):
			if 'Address' in school:
				temp = school['properties']['Address'].split(",")
				return temp[0]


		collegelocations = project(colleges, getschooladdress())

		college_jam = []

		for i in jamlocations:
			for j in collegelocations:
				if i in j:
					college_jam.append(i)


		

		repo.dropCollection("collegeandjam")
		repo.createCollection("collegeandjam")
		repo['xhug.collegeandjam'].insert(college_jam)
		repo.logout()
		endTime = datetime.datetime.now()
		return {"start": startTime, "end": endTime}

		@staticmethod
		def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
			client = dml.pymongo.MongoClient()
			repo = client.repo
			repo.authenticate("xhug", "xhug")
			doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
			doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
			doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
			doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
			doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
			this_script = doc.agent('alg:xhug#collegejam', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
			resource_jam = doc.entity('dat:xhug#trafficejam', {'prov:label':'JAMDATA', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
			resource_schools = doc.entity('dat:xhug#bostoncolleges', {'prov:label':'SCHOOLSDATA', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
			get_collegeandjam = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'jamandschools', prov.model.PROV_TYPE:'ont:DataSet'})
			doc.wasAssociatedWith(get_collegeandjam, this_script)
			doc.usage(get_collegeandjam, resource_schools, startTime,None, {prov.model.PROV_TYPE: 'ont:Computation'})
			doc.usage(get_collegeandjam, resource_jam, startTime,None, {prov.model.PROV_TYPE: 'ont:Computation'})
			JS = doc.entity('dat:xhug#collegeandjam', {prov.model.PROV_LABEL:'JAMSCHOOL', prov.model.PROV_TYPE:'ont:DataSet'})
			doc.wasAttributedTo(JS, this_script)
			doc.wasGeneratedBy(JS, get_collegeandjam, endTime)
			doc.wasDerivedFrom(JS, resource_jam, get_collegeandjam, get_collegeandjam, get_collegeandjam)
			doc.wasDerivedFrom(JS, resource_schools, get_collegeandjam, get_collegeandjam, get_collegeandjam)

			repo.logout()
			return doc
