import json
import dml
import prov.model
import datetime
import uuid
import bson.code
import pymongo

class RestaurantRatingsAndInspections_Boston(dml.Algorithm):

    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.RestaurantRatingsAndInspections_Boston']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        new_collection_name = 'RestaurantRatingsAndInspections_Boston'

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')

        repo.dropCollection('bstc_semina.'+new_collection_name)
        repo.createCollection('bstc_semina.'+new_collection_name)

        collection_yelp = repo.bstc_semina.getBostonYelpRestaurantData
        cursor_yelp = collection_yelp.find({})
        collection_inspections = repo.bstc_semina.getBostonRestaurantLicenseData
        cursor_inspections = collection_inspections.find({})

        mapperInspections = bson.code.Code("""
            function() {
                var vs = {
                    violation_level: this.ViolLevel,
                    num_violations: 1
                };
                emit(this.businessName, vs);
            }
            """)
        reducer = bson.code.Code("""
            function(k, vs) {
                var total = 0;
                var num_violations = 0;

                for (var i = 0; i < vs.length; i++) {
                    //total += vs[i].violation_level.length;
                    num_violations += 5;                        // for some reason this isn't working, reducer doesn't seem to be called.
                }
                return {num_violations: num_violations, total_violation_severity: total};
            }
            """)
        finalizer = bson.code.Code("""
            function(k, reduced_v) {
                reduced_v.ave_violation_severity = reduced_v.num_violations;
                return reduced_v;
            }
            """)

        repo.bstc_semina.getBostonRestaurantLicenseData.map_reduce(mapperInspections, reducer, 'bstc_semina.'+new_collection_name, finalize = finalizer)

        # # merge on restaurant name
        # mapperYelp = bson.code.Code("""
        #     function() {
        #         var vs = {
        #             ratings: this.ratings,
        #             review_count: this.review_count,
        #             categories: this.categories,
        #             location: this.location,
        #             coordinates: this.coordinates
        #         };
        #         emit(this.businesses[0].name, vs)
        #     }
        #     """)
        # mapperInspections = bson.code.Code("""
        #     function() {
        #         var vs = {
        #             violation_level: this.ave_violation,
        #
        #         };
        #     }
        #     """)

        repo.logout()

        endTime = datetime.datetime.now()

        return ({'start':startTime, 'end':endTime})

    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alice_bob', 'alice_bob')
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

RestaurantRatingsAndInspections_Boston.execute()
