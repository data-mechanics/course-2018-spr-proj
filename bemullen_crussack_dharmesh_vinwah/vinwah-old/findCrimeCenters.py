import dml
import prov.model
import datetime
import uuid
import gpxpy.geo
import random

class findCrimeCenters(dml.Algorithm):

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

    def plus(args):
        p = [0,0]
        for (x,y) in args:
            p[0] += x
            p[1] += y
        return tuple(p)

    def scale(p, c):
        (x,y) = p
        return (x/c, y/c)

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]


    contributor = 'vinwah'
    reads = ['vinwah.crimes', 'vinwah.businesses', 'vinwah.universities', 'vinwah.busStops', 'vinwah.properties']
    writes = ['vinwah.crimeCenters']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('vinwah', 'vinwah')

        M = [(random.uniform(42.3,42.34), random.uniform(-71.09,-71.07)) for _ in range(5)]

        crimes = repo['vinwah.crimes']

        # project crimes to be on form (lat, long)
        p = lambda x: (x['Lat'], x['Long'])
        P = findCrimeCenters.project(crimes.find(), p)

        # select only points that has location
        s = lambda x: (None not in x)
        P = findCrimeCenters.select(P, s)

        # project crimes to be on form (lat, long)
        p = lambda x: (float(x[0]), float(x[1]))
        P = findCrimeCenters.project(P, p)

        # select all points that are located in boston
        s = lambda x: (42<x[0]<42.4 and -71.2 < x[1] < -70.8)
        P = findCrimeCenters.select(P, s)

        for _ in range(6):
            MPD = [(m, p, findCrimeCenters.dist(m,p)) for (m, p) in findCrimeCenters.product(M, P)]
            PDs = [(p, findCrimeCenters.dist(m,p)) for (m, p, d) in MPD]
            PD = findCrimeCenters.aggregate(PDs, min)
            MP = [(m, p) for ((m,p,d), (p2,d2)) in findCrimeCenters.product(MPD, PD) if p==p2 and d==d2]
            MT = findCrimeCenters.aggregate(MP, findCrimeCenters.plus)
            M1 = [(m, 1) for (m, _) in MP]
            MC = findCrimeCenters.aggregate(M1, sum)
            M = [findCrimeCenters.scale(t,c) for ((m,t),(m2,c)) in findCrimeCenters.product(MT, MC) if m == m2]


        # format for MongoDB
        data = []
        for m in M:
            data.append({'lat': m[0], 'long': m[1]})

        repo.dropCollection('crimeCenters')
        repo.createCollection('crimeCenters')
        repo['vinwah.crimeCenters'].insert_many(data)

        repo.logout()
        endTime = datetime.datetime.now()
        print('findCrimeCenters finished at:', endTime)
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

        this_script = doc.agent('alg:findCrimeCenters', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dat:crimes', {'prov:label': 'Crime Incident Reports', prov.model.PROV_TYPE: 'ont:DataSet'})

        get = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get, this_script)

        doc.usage(get, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        enti = doc.entity('dat:crimeCenters', {prov.model.PROV_LABEL: 'Crime Centers in Boston', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(enti, this_script)
        doc.wasGeneratedBy(enti, get, endTime)
        doc.wasDerivedFrom(enti, resource, get, get, get)

        repo.logout()

        return doc

