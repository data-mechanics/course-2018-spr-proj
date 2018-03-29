import dml
import prov.model
import datetime
import uuid
import gpxpy.geo

class getDistUniToCrimeCenter(dml.Algorithm):

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]

    def product(R, S):
        return [(t, u) for t in R for u in S]

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]

    def dist(p, q):
        return gpxpy.geo.haversine_distance(p[0], p[1], q[0], q[1])


    contributor = 'vinwah'
    reads = ['vinwah.universities', 'vinwah.crimeCenters']
    writes = ['vinwah.distUniToCrimeCenter']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('vinwah', 'vinwah')

        universities = repo['vinwah.universities']
        crimeCenters = repo['vinwah.crimeCenters']

        #project universities
        p = lambda x: (x['properties']['SchoolId'] ,(x['geometry']["coordinates"][1], x['geometry']["coordinates"][0]))
        U = getDistUniToCrimeCenter.project(universities.find(), p)

        #project crime centers
        p = lambda x: (x['lat'] , x['long'])
        C = getDistUniToCrimeCenter.project(crimeCenters.find(), p)

        # get distance from every university to every crime center
        UCDs = [(u, c, getDistUniToCrimeCenter.dist(u[1],c)) for (u,c) in getDistUniToCrimeCenter.product(U,C)]

        #project before aggregation
        UDs = getDistUniToCrimeCenter.project(UCDs, lambda x: (x[0],x[2]))

        #aggregate the distance that correspondes to shortest distance t crime center
        UD = getDistUniToCrimeCenter.aggregate(UDs, min)

        # match them university min distance with a crime center
        UC = [(u, c) for ((u,c,d), (u2,d2)) in getDistUniToCrimeCenter.product(UCDs, UD) if u==u2 and d==d2]

        # format for MongoDB
        data = []
        for uc in UC:
            data.append({'uid':uc[0][0], 'coordinates': uc[0][1], 'crimeCenter':uc[1] })

        repo.dropCollection('distUniToCrimeCenter')
        repo.createCollection('distUniToCrimeCenter')
        repo['vinwah.distUniToCrimeCenter'].insert_many(data)

        repo.logout()
        endTime = datetime.datetime.now()

        print('getDistUniToCrimeCenter finished at:', endTime)

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        """
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        """

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('vinwah', 'vinwah')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/vinwah#')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/vinwah#')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:getDistUniToCrimeCenter', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resourceU = doc.entity('dat:universities', {'prov:label': 'Universities in Boston', prov.model.PROV_TYPE: 'ont:DataSet'})
        resourceC = doc.entity('dat:crimeCenters', {'prov:label': 'Crime Centers in Boston', prov.model.PROV_TYPE: 'ont:DataSet'})

        get = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get, this_script)

        doc.usage(get, resourceU, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get, resourceC, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        enti = doc.entity('dat:getDistUniToCrimeCenter', {prov.model.PROV_LABEL: 'Crime Center Closest to Universities', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(enti, this_script)
        doc.wasGeneratedBy(enti, get, endTime)
        doc.wasDerivedFrom(enti, resourceU, get, get, get)
        doc.wasDerivedFrom(enti, resourceC, get, get, get)


        repo.logout()

        return doc

