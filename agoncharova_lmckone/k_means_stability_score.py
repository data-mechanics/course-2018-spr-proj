from scipy.cluster.vq import kmeans,vq
import matplotlib.pyplot as plt
from pylab import plot,show
from numpy import array
import prov.model
import datetime
import uuid
import dml

class k_means_stability_score(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = ['agoncharova_lmckone.stability_score']
	writes = ['agoncharova_lmckone.stability_score_kmeans']

	@staticmethod
	def graph_clusters(data, centroids):
		'''
		Uses matplotlib to print the data (numpy array)
		and the centroids
		'''
		print("graphing data")
		plt.scatter(data[:, 0], data[:, 1])
		plt.scatter(centroids[:, 0], centroids[:, 1], c='r')
		plt.show()

	@staticmethod
	def cluster_by_evictions_stability(all_data):
		num_clusters = 5

		pre_numpy_arr = [] # [stability', 'evictionScore']
		for item in all_data:
			pre_numpy_arr.append([item['stability'], item['evictionScore']])
		numpy_arr = array(pre_numpy_arr)

		centroids, distortion = kmeans(numpy_arr, num_clusters)
		print("ran cluster_by_evictions_stability with " + str(num_clusters) + " clusters")
		# k_means_stability_score.graph_clusters(numpy_arr, centroids)
		return 0

	@staticmethod
	def cluster_by_crime_stability(all_data):
		num_clusters = 5

		pre_numpy_arr = [] # [stability', 'crimeScore']
		for item in all_data:
			pre_numpy_arr.append([item['stability'], item['crimeScore']])
		numpy_arr = array(pre_numpy_arr)

		centroids, distortion = kmeans(numpy_arr, num_clusters)
		print("ran cluster_by_crime_stability with " + str(num_clusters) + " clusters")
		# graph_clusters
		# k_means_stability_score.graph_clusters(numpy_arr, centroids)
		return 0

	@staticmethod
	def cluster_by_crime_evictions_stability(all_data):
		num_clusters = 11
		# scipy wants data of type array([[x1, x2], [x1, x2], [x1, x2]])
		pre_numpy_arr = [] # [stability', 'evictionScore', 'crimeScore']
		for item in all_data:
			pre_numpy_arr.append([item['stability'], item['evictionScore'], item['crimeScore']])
		numpy_arr = array(pre_numpy_arr)

		# https://docs.scipy.org/doc/scipy/reference/generated/scipy.cluster.vq.whiten.html#scipy.cluster.vq.whiten
		# TODO: maybe try to 'whiten'?

		centroids, distortion = kmeans(numpy_arr, num_clusters)
		print("ran cluster_by_crime_evictions_stability with " + str(num_clusters) + " clusters")
		# assign each sample to a cluster		
		# k_means_stability_score.graph_clusters(numpy_arr, centroids)
		return 0

	@staticmethod
	def get_stability_scores_from_repo():
		'''
		Gets and returns the data from the stability_score collection.
		'''
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		stability_score_collection = repo['agoncharova_lmckone.stability_score']

		stability_scores_cursor = stability_score_collection.find()
		stability_scores_arr = []
		for stability_score_object in stability_scores_cursor:
			stability_scores_arr.append(stability_score_object)
		print("in k_means_stability_score.py, got all data from stability_score collection")
		repo.logout()
		return stability_scores_arr

	@staticmethod
	def execute(trial=False):
		'''
		Run k-means algorithm on the previously computed stability scores.
		We can later put this on the map to see the distribution and
		physical distance of the neighborhoods with low scores.

		We use scipy library for the kmeans algorithm.
		May need to install: matplotlib, numpy, scipy.
		'''		
		this = k_means_stability_score

		startTime = datetime.datetime.now()
		stability_scores = this.get_stability_scores_from_repo()

		# ran k-means algorithm on various combinations of data
		# possible to see visual output via uncommenting `graph_clusters`
		# function calls in each of the methods below
		this.cluster_by_crime_evictions_stability(stability_scores)
		this.cluster_by_crime_stability(stability_scores)
		this.cluster_by_evictions_stability(stability_scores)

		return {"start": startTime, "end":  datetime.datetime.now()}

	@staticmethod
	def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont',
		                'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

		this_agent = doc.agent('alg:agoncharova_lmckone#stability_score_kmeans',
		                      	{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

		this_entity = doc.entity('dat:agoncharova_lmckone#stability_score_k_means',
                            {prov.model.PROV_LABEL: 'Stability Score Clusters', prov.model.PROV_TYPE: 'ont:DataSet'})
		
		stability_score_resource = doc.entity('dat:agoncharova_lmckone#stability_score',
		                  {prov.model.PROV_LABEL: 'Stability Score', prov.model.PROV_TYPE: 'ont:DataSet'})

		get_clusters_stability_score = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

		doc.usage(get_clusters_stability_score, stability_score_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
		
		doc.wasAssociatedWith(get_clusters_stability_score, this_agent)
		doc.wasAttributedTo(this_entity, this_agent)
		doc.wasGeneratedBy(this_entity, get_clusters_stability_score, endTime)
		doc.wasDerivedFrom(this_entity, stability_score_resource, get_clusters_stability_score, get_clusters_stability_score, get_clusters_stability_score)

		repo.logout()

		return doc

# k_means_stability_score.execute()
# k_means_stability_score.provenance()
