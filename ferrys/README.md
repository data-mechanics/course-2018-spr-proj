# Project 1
## Justification
Going to an establishment that allows alcohol consumption on premises can be fun, but getting home can be dangerous. Even if you are not driving, you can encounter difficulty getting to your destination by public transportation or by a driving service. This project hopes to uncover walking routes from the location of an alcohol-serving location to a public transportation stop which will be the most well-lit by streetlights. Additionally, I want to look at the impact that Uber could have on improving safety. Factors could include dropping their patrons off in well lit areas, making sure to be active when public transportation shuts down at night, or being active in areas where there are a lot of alcohol licenses provided.  

## Datasets

* [Sidewalk Inventory](https://data.boston.gov/dataset/sidewalk-inventory)
* [Alcohol Licenses](https://data.boston.gov/dataset/all-section-12-alcohol-licenses)
* [Streetlight Locations](https://data.boston.gov/dataset/streetlight-locations)
* [MBTA Developer V3 API Portal](https://api-v3.mbta.com)
* [Uber Movement](https://movement.uber.com/cities)

## Transformations

* getMBTADistances - Combines MBTA stops and alcohol licences and determines how many public transportation stops are geographically close to each location of an alcohol license. 
* getUberLights - Combines Uber and streetlights and determines which Uber boundaries have 1 or more streetlights in the destination.
* getSidewalksWithStreetlights - Combines sidewalks and streetlights and determines which streetlights are on which sidewalks.

## Requirements

* Python 3.6
* Extra Libraries
    * pandas
    * geojson
    * shapely
    * rtree
    * spatialindex
* [Google Geocode API Key](https://developers.google.com/maps/documentation/geocoding/get-api-key)
    * Credentials are pulled from the auth.json file in the format ['services']['googlegeocoding']['key']
* [MBTA Developer V3 API Portal Key](https://api-v3.mbta.com)
    * Credentials are pulled from the auth.json file in the format ['services']['mbtadeveloperportal']['key']
    