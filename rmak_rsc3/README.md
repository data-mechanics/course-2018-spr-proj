Boston is a busy city where many rely on public transportation. Unfortunately, we cannot expect to have all of our trains to run on time -- in which case we have to take matters into our own hands if we want to be punctual. You could call a cab, a Lyft, or an Uber instead of waiting. But why not bike to where you need to go instead of contributing to traffic?

 Our project utilizes k-means (specifically Lloyd's algorithm, which we used the SciKit KMeans library for) to identify where trains are most often late and place a Hubway station there. We have the following data sets (and the algorithms associated with them): 

1. MBTA Green Line stations and their coordinates (getGreenLineCoords.py)

2. Hubway stations and their coordinates (getHubway.py)

3. MBTA Green Line stations and their reliability (a metric that measures how many people who have waited longer than the expected time for their train, calculated by MBTA) (getReliability.py)

Here is a walk through of what we did (kCoords.py): 

1. Create a dictionary called lateTrains where keys are stations and values are reliability (rounded down to nearest integer). 

2. Create a lateTrainsCoords list. For each stop x in lateTrains, add the value corresponding to x entry -- the latitude and longitude of x, which we get from getGreenLineCoords.py. For example, if lateTrains looks like this: 

{"Hynes Convention Center": 2, "Babcock Street": 4}

and we know from that Hynes Convention Center is located at (42.0123, -71.456) and that Babcock Street is located at (42.0789, -71.000), then step 2 will create a list that looks like

lateTrainsCoords = [
(42.0123, -71.456), (42.0123, -71.456),
(42.0789, -71.000), (42.0789, -71.000), (42.0789, -71.000), (42.0789, -71.000)
]

3. Run k-means (kCoords.py) on lateTrainsCoords. The output will give us an idea of where to place a new Hubway station (or where to move a new one). 

In our experiment we set k = 40, because that is roughly the number of existing stations surrounding the Green Line. We then found the average distance from each of the 40 means we found to the closest existing Hubway station (meanDistances.py). This value is 640 feet. 

For future analysis, we will pick the stations that are the most late and see how far the nearest Hubway station is. Would it be more optimal to move those Hubway stations somewhere else? We would also like to see if there is a correlation between how many Ubers leave busy stations and Hubway station usage. 

Running instructions: 
run execute.py
If you need to run in trial mode
run execute.py --trial




