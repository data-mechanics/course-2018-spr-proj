from pymongo import MongoClient
import numpy as np
from mpmath import mp, mpf
import shapely.geometry

def find_closest_centroids(samples, centroids):
    closest_centroids = []
    for sample in samples:
        distances = []
        i = 0
        for centroid in centroids:
            distance = np.sqrt(((sample[0] - centroid[0]) ** 2) + ((sample[1] - centroid[1]) ** 2))
            distances += [(distance, i)]
            i += 1
        closest_centroids += [min(distances)[1]]

    return np.array(closest_centroids)


def get_centroids(samples, clusters):
    sample_nums = {}
    for cluster in clusters:
        sample_nums[cluster] = None

    sums = [np.array([mpf(0), mpf(0)]) for _ in range(len(sample_nums.keys()))]
    numbers = [0 for _ in range(len(sample_nums.keys()))]

    for i in range(len(samples)):
        numbers[clusters[i]] += 1
        sums[clusters[i]][0] += samples[i][0]
        sums[clusters[i]][1] += samples[i][1]


    for i in range(len(sums)):
        sums[i] = sums[i] / numbers[i]


    return np.array(sums)


class kmeans:
    def __init__(self, number):
        self.number = number
        self.client = MongoClient(username="jlove", password="jlove", authSource="repo")
        self.db = self.client['repo']

    def run(self):
        mp.dps = 30
        hydrants = self.db['jlove.hydrants'].find_one({})
        points = []
        for hydrant in hydrants['features']:
            point = shapely.geometry.shape(hydrant['geometry'])
            points += [[mpf(point.x), mpf(point.y)]]

        points = np.array(points)
        shuffled = points.copy()
        np.random.shuffle(shuffled)
        centroids = np.array(shuffled[:self.number])
        iterations = 30
        for i in range(iterations):
            clusters = find_closest_centroids(points, centroids)
            centroids = get_centroids(points, clusters)

        json_centroids = []
        for centroid in centroids:
            json_centroids += [{'lng': float(centroid[0]), 'lat': float(centroid[1])}]

        return json_centroids


