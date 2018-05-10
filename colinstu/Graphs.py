import mpld3
import matplotlib.pyplot as plt
import json
import dml
import prov.model
import datetime
import uuid


class graphs(dml.Algorithm):
    contributor = 'colinstu'
    reads = ['colinstu.statanalysis']
    writes = []

    def makeGraphs(stats):
        for row in stats.find():
            x = row['x']
            y = row['y']
            z = row['z']

        fig, ax = plt.subplots()
        points = ax.plot(x, y, 'o', color='b', mec='k', ms=15, mew=1, alpha=.6)
        ax.set_xlabel("Percent of Boston's Impoverished Living in Neighborhood", size=15)
        ax.set_ylabel('Number of Grocery Stores', size=15)
        ax.set_title('Relationship Between Number of Grocery Stores and Poverty Rate', size=20)
        ax.grid(True, alpha=0.3)

        labels = []
        for i in range(len(z)):
            label = z[i]
            labels.append(label)
        tooltip = mpld3.plugins.PointLabelTooltip(points[0], labels)
        mpld3.plugins.connect(fig, tooltip)
        mpld3.show()

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('colinstu', 'colinstu')

        stats = repo.colinstu.statanalysis
        graphs.makeGraphs(stats)

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

        this_script = doc.agent('alg:colinstu#graphs',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('gdp:graphs',
                              {'prov:label': 'Graph of Relationship Between Number of Grocery Stores and Poverty Rate',
                               prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        graph = doc.entity('dat:colinstu#graphs',
                                       {prov.model.PROV_LABEL: 'Created Graph',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        getstatanalysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getstatanalysis, this_script)
        doc.usage(getstatanalysis, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})


        doc.wasAttributedTo(graph, this_script)
        doc.wasGeneratedBy(graph, getstatanalysis, endTime)
        doc.wasDerivedFrom(graph, resource, getstatanalysis)

        repo.logout()

        return doc

graphs.execute()
doc = graphs.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))





