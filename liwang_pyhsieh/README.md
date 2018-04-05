# CS591 Project: Car-Accident-Related Infrastructure Analysis

## Objective: 
Crashing accidents happen everyday in the world. In this project, we look into the crashing accidents that happen especially at night, and find if the lack of lights would result in the tragedy. We also look for the locations of hospitals and police stations to evaluate if the positioning corresponds to where the accidents happen the most. 

## Project #1: 
In this part of our project, we retrieved dataset from various portal website, and trying to make some transformation based on relational algeabra paradigm.

### Dataset Source
1.Traffic Signals:  http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0

2. Street Lights:  https://data.boston.gov/dataset/streetlight-locations/resource/c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5

3. Hospitals:  https://data.boston.gov/dataset/hospital-locations/resource/6222085d-ee88-45c6-ae40-0c7464620d64

4. Police Stations:  https://data.boston.gov/dataset/boston-police-stations/resource/0b2be5cb-89c6-4328-93be-c54ba723f8db

5. Crash accidents in 2015:  http://datamechanics.io/data/liwang_pyhsieh/crash_2015.json

### Transformation 
We conduct these transformations on the original dataset, to get the position and possible 'hotspots' for car accidents in Boston during nighttime. Then we generate additional data related to these spatial positions, such as average distance to the nearest facilities within the coverage of a hotspot, or numbers of lamps and signals in surrounding area.

1. Numbers of street lights and traffic signals near the accident spot within the given range.
2. Using K-means algorithm to find out 'hotspots' where accidents happen frequently.
3. Calculate the average distance of nearest hospital and police station within each cluster previously found from (2).

## Project #2
In this project, we try to solve constraint-satisfaction problem, and to conduct statistical analysis based on our work in Project #1.

In Project #1, we've found the number of lights and signals near to each accident spot. We want to know whether there's any relationship between frequency of accidents and amount of surrounding traffic facilities. Since we only analyze on accidents happening during nighttime, it's reasonable that accident frequency could be reduced if there's enough lighting or signals in an area. So that we can observe their relationships by computing correlation-coefficient. If we find there's negative relationship between accident frequency and number of nearby lights or signals, we can conclude that these facilities may help reduce car accidents. If not (with negative, or no apparent relation), then there may be other factors should be taken into consideration, such as traffic flow or weather condition.

### Constraint-Satisfaction Problem Solving

### Statistical Analysis
First we define that 'accident density' is the amount of other accidents near to the accident spot,then we compute the correlation-coefficient along with approximated p-value between accident density and number of nearby street lights and traffic signals of all accident spots.

## Library Dependency
These are libraries may required other than python basic libraries and ones for ```dml```:
```
numpy
pandas
pyproj
geopy
```
