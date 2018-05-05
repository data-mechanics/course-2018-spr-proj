from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
import time
import os

start = time.time()

spark = SparkSession.builder.appName('kmeans').getOrCreate()
start2 = time.time()
# Load and parse the data
dataset = spark.read.parquet(
    os.path.join('/Volumes', 'Shared', 'cs591', '311-service-requests.parquet'))
dataset = dataset.na.drop()

features = ['Latitude', 'Longitude']
assembler = VectorAssembler(
    inputCols=[x for x in dataset.columns if x in features],
    outputCol='features')

assembler.transform(dataset)

kmeans = KMeans().setK(20).setMaxIter(300).setInitSteps(10)
pipeline = Pipeline(stages=[assembler, kmeans])
model = pipeline.fit(dataset)
# model = kmeans.fit(df)

# Make predictions
predictions = model.transform(dataset)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# Shows the result.
centers = model.stages[1].clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

spark.stop()

end = time.time()
print(end - start2)
print(end - start)
