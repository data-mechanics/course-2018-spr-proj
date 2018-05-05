# Project 3

### Team:

*rikenm@bu.edu* <br />
*bm181354@bu.edu*



## Summary:

Hubway is a bicycle sharing company where guest and subscribed users can lend a bike for certain amount of time. Our main goal was motivated by the question what if Hubway would like to add more station in the city of Boston. To obtained this objective, the clustering algorithm like hierarchal and k-mean were used to place the new bike stations.


## Programming language:
Python and MongoDB

## Dataset

The following publicly available dataset  were used to help determine the most optimal coordinates to place the new stations in the greater Boston and city of Boston.

Hubway stations and Hubway Trip dataset

1. [Hubway Trip data] https://s3.amazonaws.com/hubway-data/index.html

2. [Hubway Station data]  https://s3.amazonaws.com/hubway-data/Hubway_Stations_as_of_July_2017.csv




## Methodology:
1. After retrieving the data, we basically, at first, did hierarchal clustering based on the distance. We decided 20 clusters seem appropriate. From the first dataset, we calculated how many times each station is visited in period of three month.
2. We stored this value in an array as popularity for each station. User would select how many stations to add by sliding the slider in our webpage.
3. We  then decided to put station on the cluster with the highest average popularity. After this we use k-mean to figure out the exact coordinates to put our stations.

<img src="https://github.com/bm181354/course-2018-spr-proj/bm181354_rikenm/img/dendogram.png" width="350"/>

*Fig 1.0: Dendrogram of Hierarchical clustering
The figure above shows how the algorithm choices points to accumulate.  Slowly the number of cluster decreases and at the end we would have one cluster. The algorithm was stopped before this point. 20 clusters seem decent point to stop.*


**Note: Least significant stops changes as we determine how many clusters to have. If we have one cluster then least sig would be the one with the least popularity of all the data.**




## Dependency:

```python
 pip install pandas
 pip install scipy
 pip install numpy
```

## To Run:

```python
python execute.py bm181354_rikenm
```
Our algorithm’s data would be saved as **“bm181354_rikenm.solutionLeastPopularStationsdb”** inside db **“repo”** and collection **“solutionLeastPopularStationsdb”**


D3 and web services, which was created from flask/python were used. We stored all our result from our algorithm into Mongodb. This result will be obtained by our web service which sends this data to front end in D3 from our Flask app when user slides the slider.

### Run Flask:
```python
python3 visualization/app.py
```

