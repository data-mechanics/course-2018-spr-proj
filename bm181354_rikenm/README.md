# Project 2

### Team:

*rikenm@bu.edu* <br />
*bm181354@bu.edu*


## The purpose of the project

Our idea is to located the least significant Hubway station and eliminate it. We used clustering algorithm called hierarchal clustering to cluster station based on their location and popularity. If multiple bike stations of same type, as type determined by our clustering algorithm, are located in the same area in high intensity then we would remove this stop as  least significant one.

## Programming language:
Python and MongoDB

## Dataset
Hubway stations and Hubway Trip dataset


## Methodology:
1. Cluster data based on their distance, and popularity using Linkage Clustering. ( metric would be: **( Haversine_Distance(a, b)+ difference_in_popularity(a, b)**) where a and b are stops.
2. Once data is clustered. We label each point based on their cluster.
3. iterate over this labelled data now to find the point which is the least popular and also has the most neighbors with the same label as point in consideration in some threshold sphere(instead of circle as 3d vector(lat,long,poularity)).
4. mark this as a least significant stop.

**Note: Least significant stops changes as we determine how many clusters to have. If we have one cluster then least sig would be the one with the least popularity of all the data.**

In this project we extracted 4 data sets from Hubway, a bicycle station company. Data acquired had start and end trip location, station ID, and station location.  Hence, we had three dimension of data(x=long, y=lat, z=popularity of a stop). For popularity we just counted total occurrence of stationID. We clustered our data using average linkage clustering. Linkage clustering is a hierarchal clustering where two similar objects are clustered in the same cluster at first then these clustered are joined iteratively until we hit the root node. We can stop algorithm to get a desired cluster. The cluster that seemed to work was when we had 20 clusters. We used this clustering instead of K mean because there are lots of option how to do the clustering. For example: Single, Average, Complete. For Cluster analysis, we did not find many useful test hence, we just created a correlation table for our data.
One useful thing about hierarchal clustering is that you can see the dneundogram diagram to a analysis the cluster.

<img src="/img/20clusters.jpeg" width="350"/>

## To Run:

```python
python execute.py bm181354_rikenm
```
Our algorithm’s data would be saved as “bm181354_rikenm.solutionLeastPopularStationsdb” inside db “repo” and collection “solutionLeastPopularStationsdb”
