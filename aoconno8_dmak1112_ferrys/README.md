# Project 2
## Justification
Going to an establishment that allows alcohol consumption on premises can be fun, but getting home can be dangerous. Even if you are not driving, you can encounter difficulty getting to your destination by public transportation or by a driving service. This project hopes to uncover walking routes from the location of an alcohol-serving location to a public transportation stop which will be the most well-lit by streetlights. Additionally, I want to look at the impact that Uber could have on improving safety. Factors could include dropping their patrons off in well lit areas, making sure to be active when public transportation shuts down at night, or being active in areas where there are a lot of alcohol licenses provided.  

## Datasets

* [Alcohol Licenses](https://data.boston.gov/dataset/all-section-12-alcohol-licenses)
* [Streetlight Locations](https://data.boston.gov/dataset/streetlight-locations)
* [MBTA Developer V3 API Portal](https://api-v3.mbta.com)
* [OpenStreetMap](https://www.openstreetmap.org) (Accessed via [OSMnx](https://github.com/gboeing/osmnx))

## Transformations

* getClosestMBTAStops - Combines MBTA stops and alcohol licences and determines which public transportation stops are geographically close to each location of an alcohol license. Currently limits to 3.
* getStreetlightsInRadius - Gets all of the streetlights near each alcohol license using the distance to the farthest "closest" MBTA stop as the radius.

## Optimizations

* getShortestPath - Gets the shortest path and the "safest" path from each alcohol license  to its 3 closest MBTA stops. The "safest" path is the path that includes the node with the most streetlights.
* optimization - Takes the 6 paths generated from each alcohol license and scores them based on variance of streetlights on the path, number of streetlights, and distance. It weights the distance of the "safest" path slightly less since we know that it is "safer" than the shortest path.

## Requirements

* Python 3.6
* Extra Libraries
    * pandas
    * geojson
    * shapely
    * rtree
    * spatialindex
    * tqdm
    * osmnx
    * networkx
    * utm
    * geoql
    * geopy
* [Google Geocode API Key](https://developers.google.com/maps/documentation/geocoding/get-api-key)
    * Credentials are pulled from the auth.json file in the format ['services']['googlegeocoding']['key']
* [MBTA Developer V3 API Portal Key](https://api-v3.mbta.com)
    * Credentials are pulled from the auth.json file in the format ['services']['mbtadeveloperportal']['key']
    