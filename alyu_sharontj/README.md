# Team Member
1. Ailing Yu (alyu@bu.edu)
2. Jin Tang (sharontj@bu.edu)


# The Purpose of Project
Evaluating the reasonability of current arrangements of traffic signals in Boston area.

# Programming Languages
1. Python
2. MongoDB

# Dataset
1. All Road Segments in Boston, MA\
https://dataverse.harvard.edu/dataverse/geographical_infrastructure_2017

2. Traffic Signals\
http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0

3. Snow Emergency Route\
https://data.boston.gov/dataset/snow-emergency-routes

4. Traffic Jam\
http://datamechanics.io/data/alyu_sharontj/TrafficJam.json

5. Uber traffic Movement\
http://datamechanics.io/data/alyu_sharontj/boston_taz.json


# Data Transformation
We performed several transformations to product three new data set.

1. TrafficSignal_Density\
We processed Traffic Signal Dataset to get the signal density of each street by using selection, MapReduce, projection.

2. SnowEmRoutes_TrafficSignalDensity\
We processed Traffic Signal Density Dataset and Snow Emergency Route to get the traffic signal density of each snow emergency route by using selection, production, and projection.

3. TrafficDelay_SignalDensity\
We processed Traffic Signal Density Dataset and traffic jam data to find the relation between traffic signal density and traffic jam.


# To Run this Project
```
python3 execute.py alyu_sharontj
```




