# Project 2
## Justification
Going to an establishment that allows alcohol consumption on the premises can be fun, but getting home can be dangerous. Even if you are not driving, you can encounter difficulty getting to your destination by public transportation or by a driving service. In Project #1, we incorporated datasets inlcuding Boston streetlight data, MBTA stops, and alcohol licenses, and performed transformations combining these datasets with the goal of taking the preliminary steps towards uncovering walking routes from the location of an alcohol-serving location to a public transportation stop which will be the most well-lit by streetlights.

In Project #2, we take this idea a step further by performing optimization techniques to calculate routes from locations with alcohol licenses to MBTA stops. We first calculate both the shortest paths and the safest paths from each alcohol license to its 3 closest MBTA stops, where the safest path is the path that includes the node with the most streetlights. Then, we perform an optimization technique that scores all six of the paths based on a number of factors including distance, number of streetlights, and variance of streetlights along the path. By finding the highest scored path, we hope to be able to provide civilians with routes to nearby MBTA stops that provide a reasonable combination of convenience and safety.

We also perform various statistical analyses to test two hypotheses and to check for correlation, and from these statistical analyses we were able to glean valuable information about the distribution of streetlights, for example that the mean number of streetlights near locations with alcohol licenses is greater than the mean number of streetlights near MBTA stops. More details about our results can be found below in the Statistical Analysis section.


## Datasets

* [Alcohol Licenses](https://data.boston.gov/dataset/all-section-12-alcohol-licenses)
* [Streetlight Locations](https://data.boston.gov/dataset/streetlight-locations)
* [MBTA Developer V3 API Portal](https://api-v3.mbta.com)
* [OpenStreetMap](https://www.openstreetmap.org) (Accessed via [OSMnx](https://github.com/gboeing/osmnx))

## Transformations

* getClosestMBTAStops - Combines MBTA stops and alcohol licenses and determines which public transportation stops are geographically close to each location of an alcohol license. Currently limits to 3.
* getStreetlightsInRadius - Gets all of the streetlights near each alcohol license using the distance to the farthest "closest" MBTA stop as the radius.

## Optimizations

* getShortestPath - Gets the shortest path and the "safest" path from each alcohol license  to its 3 closest MBTA stops. The "safest" path is the path that includes the node with the most streetlights.
* optimization - Takes the 6 paths generated from each alcohol license and scores them based on variance of streetlights on the path, number of streetlights, and distance. It weights the distance of the "safest" path slightly less since we know that it is "safer" than the shortest path.

## Statistical Analyses

For our Statistical Analyses, we tested two hypotheses and checked for correlation:

The first hypothesis that we tested was to see if the mean number of streetlights at the starting node along the route from alcohol licenses to MBTA stops is greater than the mean number of streetlights at the ending node of the paths:
HO: u(starting node) - u(ending node) > 0
HA: u(starting node) - u(ending node) <= 0
alpha = .01
We reject the null hypothesis if p < alpha.
Results: z = 5.70, p < .0001

With a p value of <.01, we are able to reject our null hypothesis and conclude that there is sufficient evidence at alpha = .01 that the mean number of streetlights at the ending node of a route is >= the mean number of streetlights at the starting node.


The second hypothesis we tested was to see if the mean number of streetlights at the start and end nodes of a route was greater than the mean number of streetlights at all of the middle nodes in the route. 
HO: u(start+end nodes) - u(middle nodes) > 0
HA: u(start+end nodes) - u(middle nodes) <= 0
alpha = .01
We reject the null hypothesis if p < alpha
Z = -6.14, p = .9998

With a p value that is much greater than .01, we are unable to reject the null hypothesis, meaning that there is not sufficient evidence at alpha = .01 that the mean number of streetlights at the starting and ending  nodes in a route is less than or equal to the mean number of streetlights at all middle nodes in the route.

Finally, we ran three tests to ccheck for correlation:
i.) Streetlights at starting node vs ending node, r= .2007
ii.) Steetlights at ending nodes vs middle nodes, r=.4792
iii.) Number of nodes in a route vs total distance of route, r = .1683

All three tests for correlation did not yield convincing results, and none of these tests show strong correlation between the factors.

Finally, we checked to see the average number of nodes in a route and the average distance of a route. 
We found that the average number of nodes was 3.9375, and the average distance was 525.008 meters.


## Trial Method

To provide a quick way to test our transformations, we provide a Trial method that runs our algorithms on a subset of the data. Our trial method takes approximately 20 seconds to complete. It runs through the entire algorithm with a pre-picked coordinate of an alcohol license location and 3 pre-picked MBTA stop coordinates. It does not run through the statistical analysis since the trial algorithm only outputs one route, but we append the results of the statistical analysis on the entire collection of optimized routes instead.

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
    
  
    
