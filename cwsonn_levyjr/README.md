Project 1

MISSION STATEMENT:
As society continues to grow more and more people are congregating in cities and there are more environmental concerns that need to be taken into account. We have taken data sets containing open spaces in Boston and Cambridge, as well as bike paths in Boston and Cambridge and bike racks in Cambridge. By combining the open spaces data sets and accumulating total amount of open spaces in each area we can get an idea of how much open space there is in each area. We also accumulated the total area of open spaces in each city to track open spaces of similar sizes by using an intersection between the two data sets to get a better understanding of the contrast we were dealing with. Similarly by combining bike path datasets and accumulating the lengths of the bike paths for each city we can get an idea of how much area of each city is traversed by bikes instead of other environmentally harmful transportation. Finally, by combining the bike rack and bike path datasets for Cambridge we can see what areas have sufficient support for bikers and what areas could use more. We made a combined data set of all the coordinates for both features and found a ratio of the amount of bike paths to the amount of bike racks in Cambridge. By taking advantage of all of this data we could potentially see what parts of each area could use environmental improvements and where, and what parts are sufficient in terms of open space and biking. 

TOOLS USED/IMPORTS:
numpy
geojson

Project 2

Optimization Problem:

Using the datasets we have acquired on both bike paths and bike racks in Cambridge we solve an optimization problem that adds 500 additional bike racks in the Cambridge area. We want to add these bike racks in such a way that they are a sufficient distance away from other bike racks to avoid redundancy and cluttering, but also sufficiently close enough to known bike paths to be useful and easily accessible. In order to accomplish this we took Line-String Geojson coordinates for all of the bike paths in the Cambridge area and extracted all of the coordinates denoting the start and end points of these paths, since that is where we determined adding bike racks made the most sense. For each of these coordinates we compared the distance to every preexisting bike rack in Cambridge and created another set of distances to nearest preexisting bike rack. We found the largest distances which represent the potential spots that are furthest away from other bike racks and returned the corresponding coordinates.

Statistical Analysis:

In order to get a sense of how successful we were in our optimization problem we found the standard deviation of the closest distances to the nearest bike racks for our data set before and after we inserted the extra coordinates we had determined to be most useful. We also made a histogram for each of these sets and compared them. The updated histogram was very skewed towards the shorter distances, which made sense because the potential spots for bike racks will find themselves and return 0.0 as their shortest distance since we added them in already. We used this to prove to ourselves that we had added in the coordinates correctly. Next we compared all the distances from each bike rack to every other bike rack before and after we added in the new ones and made new histograms as well as found their standard deviations. The histograms are quite similar but the range of lengths in the updated one goes slightly further out to thr right which shows that we added in bike racks that were further away from all the preexisting bike racks. The standard deviations were also very similar with the updated one being slightly higher. Overall we think that we successfully added in new bike racks in a way to fit our goal, and showed statistically that we did it correctly.

The histograms can all be found as the .png files in our histograms folder. 


