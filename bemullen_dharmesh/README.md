# CS 591 L1 Project 1 

Authors: Brooke Mullen <bemullen@bu.edu>, Dharmesh Tarapore <dharmesh@bu.edu>

## The Impact of College Students on Boston's residents

Move in week is universally chaotic and a generally unpleasant experience for both, college students
and local residents. 

In this project, we attempt to isolate the most important factors that affect the residents' quality
of life. With this information, we can model the problem as a relatively straightforward optimization
problem with constraints.

To that end, the datasets we are using are:

1. <strong><a href="https://data.boston.gov/dataset/cityscore">CityScores</a></strong>: This provides a consolidated metric that reflects residents' satisfaction with the city's efforts. We use the average of a collection of these values as an approximate indicator of how painful move in week really is.

2. <strong><a href="https://data.boston.gov/dataset/code-enforcement-building-and-property-violations/resource/90ed3816-5e70-443c-803d-9a71f44470be">Code Enforcement Building and Property Violations</a></strong>: This dataset contains a list of violations issued by the city and may be indexed by location, date ranges, etc. We use this to compute a weighted average over months/years to see if there is any relation between move in week and the number of violations (normalized for the obvious rise in enforcements spurred by the influx of students to Boston post move in week).

3. <strong><a href="https://data.boston.gov/dataset/311-service-requests/resource/2968e2c0-d479-49ba-a884-4ef523ada3c0">311 Service Requests</a></strong>: This dataset contains over 400,000 detailed records of complaints and/or requests filed by residents. We sift through irrelevant requests by filtering for the type of service request (more details in the comments in the JS files), among others.

4. <strong><a href="http://realtime.mbta.com/Portal/Content/Documents/Interpreting_mbta_performance_API_output_2016-04-26.pdf"></a>MBTA Performance API</strong>: This dataset provides us with a range of "dwell times" for the Red and Green MBTA lines. The dwell interval for a given stop is the amount of time the T spent waiting at that stop. Move in week might see dwell intervals increase, particularly at college-heavy stops.

5. <strong><a href="gis.cityofboston.gov/arcgis/rest/services/Education/OpenData/MapServer/2">Spatial Dataset of Colleges and Universities in the Greater Boston Area</a></strong>: This is used to construct 2d-index-capable queries on the other 4 datasets.

## Transformations

Transformations are stored in files beginning with the name: "Transform". The python files are used to create the provenance document for each transformation while the Javascript files perform the actual MongoDB queries.

## Evaluation

To run the code, setup the database structure as documented in the parent README and then run:

<code>python execute.py bemullen_dharmesh</code>



