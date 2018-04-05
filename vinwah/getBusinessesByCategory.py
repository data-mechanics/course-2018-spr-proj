import dml
import prov.model
import datetime
import uuid
import gpxpy.geo
import random

class getBusinessesByCategory(dml.Algorithm):

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]

    def product(R, S):
        return [(t, u) for t in R for u in S]

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]


    contributor = 'vinwah'
    reads = ['vinwah.businesses']
    writes = ['vinwah.businessesByCategory']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('vinwah', 'vinwah')

        businesses = repo['vinwah.businesses']

        # 1) find representative categories
        #projection. format of businessesCategories is [[json]]
        p = lambda x: (x['categories'])
        businessesCategories = getBusinessesByCategory.project(businesses.find(), p)
                
        #make histogram over categories
        categories_hist = {}
        for categories in businessesCategories:
            for category in categories:
                if category['title'] in categories_hist:
                    categories_hist[category['title']] += 1
                else:
                    categories_hist[category['title']] = 1

        del businessesCategories
        #find the 10 highest scors
        top10scors = []
        for key in categories_hist:
            top10scors.append(categories_hist[key])

        top10scors = sorted(top10scors)[-10:]

        # find the 10 categories associated with highest scors
        top10categories = []
        for key in categories_hist:
            if categories_hist[key] in top10scors:
                top10categories.append(key)


        # 2) project businesses into (lat, long, representable category)
        #    and remove all businesses that does not conform to a category

        #project only needed data
        p = lambda x: (x['coordinates']['latitude'], x['coordinates']['longitude'], x['categories'])
        B = getBusinessesByCategory.project(businesses.find(), p)

        #Flatten the data and select category to associate business with
        for i in range(len(B)):
            temp = []
            for c in B[i][2]:
                temp.append(c['title'])
            cat = ""
            for c in temp:
                if c in top10categories:
                    cat = c
                    break
            B[i] = (B[i][0], B[i][1], cat)

        # select non-null data 
        s = lambda x: (x[2] != "")
        B = getBusinessesByCategory.select(B, s)


        # grup by category
        data = []
        for c in top10categories:
            s = lambda x: x[2] == c
            P = getBusinessesByCategory.select(B, s)
            temp = []
            for p in P:
                temp.append({'lat':p[0], 'long':p[1]})
            data.append({c: temp})


        repo.dropCollection('businessesByCategory')
        repo.createCollection('businessesByCategory')
        repo['vinwah.businessesByCategory'].insert_many(data)

        repo.logout()
        endTime = datetime.datetime.now()

        print('getBusinessesByCategory finished at:', endTime)

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

        this_script = doc.agent('alg:getBusinessesByCategory', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dat:businesses', {'prov:label': 'Businesses in Boston', prov.model.PROV_TYPE: 'ont:DataSet'})

        get = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get, this_script)

        doc.usage(get, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        enti = doc.entity('dat:businessesByCategory', {prov.model.PROV_LABEL: 'Location of Businesses in Boston by top 10 categories', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(enti, this_script)
        doc.wasGeneratedBy(enti, get, endTime)
        doc.wasDerivedFrom(enti, resource, get, get, get)

        repo.logout()

        return doc

