## CS591 Project 01

## Objective: 
Crashing accidents happen everyday in the world. In this project, we look into the crashing accidents that happen especially at night, and find if the lack of lights would result in the tragedy. We also look for the locations of hospitals and police stations to evaluate if the positioning corresponds to where the accidents happen the most. 

## Datasets: 
1.Traffic Signals:  http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0

2. Street Lights:  https://data.boston.gov/dataset/streetlight-locations/resource/c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5

3. Hospitals:  https://data.boston.gov/dataset/hospital-locations/resource/6222085d-ee88-45c6-ae40-0c7464620d64

4. Police Stations:  https://data.boston.gov/dataset/boston-police-stations/resource/0b2be5cb-89c6-4328-93be-c54ba723f8db

5. Crash accidents in 2015:  http://datamechanics.io/data/liwang_pyhsieh/crash_2015.json

## Transformation 
1. Define a relation between the street lights and traffic signals. 
2. K-mean to find the clustering of where most of the accidents happen
3. Calculate the average distance with each clustering to the hospitals and police stations.