The Purpose of this Project

Recently, a new type of station-free sharing bike — ofo, has started to appear in the Boston area. These yellow ofo bikes can be picked up or dropped off at any location where traditional bike parking is allowed. Because of its flexibility and inexpensiveness, it is quite likely that ofo will become popular and widely used in the city very soon. At that time, one of the big issue the company need to solve is the maintenance of the bikes. Since the bikes are used with a high frequency and can be left anywhere, it is essential to find a best location to gather all the bikes that need to be maintained. In order to find such a location, we will try to find out where the bikes would mostly be used and left by analyzing public transportation as well as school and restaurant locations.

Authors
Xinchun He (debhe) and Dayuan Wang (wangdayu)


Data Retrieval
We collected 5 data sets from Analyze Boston, Hubway, and Data Mechanics.
1) publicSchool.py and privateSchool.py
    We retrieved the list of Boston’s public schools and private schools, as well as their school types, location coordinates.
2) restaurants.py
    We retrieved the Boston food license dataset which contains a list of Boston’s restaurants, their street addresses, location coordinates, city, state, zip, etc.
3) busStop.py
    We collected the list bus stops, including the stop id, stop name, town, and location coordinates
4) subwayStop.py
    We collected the list of all MBTA subway stops, including the stop id, stop name, stop code, location coordinates, parent_station, etc.
5) hubwayStation.py
    We collected the locations of all Hubway bike stations as well as the number of docks at each station.


Data Transformation
First, We filtered out any informations we do not need and kept only those that we will use for further analysis. For all the bus/subway data, we kept the stop name and location coordinates. For restaurant data, we kept restaurant name and location coordinates. For Hubway bike station data, we kept the station name, location coordinate, and number of docks at each location. For school data, we kept the school name, school location coordinate, and school type. After that, we did selection on public schools and non-public schools data to filter out any elementary schools since students in those schools are too young to ride ofo. Then, we did union on the selected public and non-public schools to create a list of all schools. Next, we used those selected information to find the shortest distance from each school, restaurant, subway, and bus station to its nearest Hubway station. To to this, we used product, projection, and aggregation. Take schoolHubwayDistance.py for example. First, we performed product to get a list of all pairs of schools and Hubway station with their coordinates. Next, we did a projection to find the Euclidean distance between each pair, which is then followed by an aggregation to find the pair with shortest distance. We got the corresponding output schoolHubwayDistance, subwayHubwayDistance, restaurantHubwayDistance, and busHubwayDistance. 
We used aggregation to calculate the average distances between school and Hubway stations, subway station and Hubway stations, as well as bus station and Subway stations and store them in avgDistance. 
For each Hubway station, we calculated the frequency it was chosen as the closest station so that we can see which Hubway stations would potentially be most frequently used. To achieve this, we first used selection to create a dictionary of all hubway stations. We then aggregated the schoolHubwayDistance by hubway station name to find out the frequency that station was chosen. We used projection to produce a list of pairs (HubwayStation, frequency) in hubwayStationFreq.


How to Run
Set up mongodb as shown
Then run execute.py

 

