We have used the following five datasets to examine different restaurants and businesses:
Code Enforcement - Building and Property Violations (Analyze Boston), FOOD ESTABLISHMENT INSPECTIONS (Analyze Boston), Yelp Fusion, Boston Restaurant Licenses (Analyze Boston), NYC Health Inspection (NYC Open Data), and NYC Restaurant Licenses (NYC Open Data)

We used the Yelp Fusion api to obtain ratings for each restaurant in the Boston Restaurant Licenses dataset. 
We calculated the number of health and property violations for each restaurant in the Yelp dataset.
The average severity of the ratings were also taken into account.
The NYC Health Inspection data was compared against the NYC Restaurant License data to attribute health inspections to restaurants based on address and inspection date. The number of health violations and average score was also recorded.

The fundamental question we could look to answer is: Are businesses affected by health code and property violations, and if so, to what degree?
To solve this question, one could compare average ratings for a business based on their number and severity of violations.
The correlation could be compared between NYC and Boston to see if both sets of consumers act in similar manners. To complete this study, we would need to get Yelp ratings for NYC restaurants, and building code violations. Also, more rating systems, such as Google's and Foursquare's could help determine an average rating, rather than just relying on Yelp.


________________________________________________________________________________________________________________________________________


For project 2, we are continuing working with dataset obtained from project 1: RestaurantRatingsAndHealthViolations_Boston.json that contains approximately 1200 restaurants of Boston along with Yelp ratings and health violation information. 

Given this dataset, we aim to answer the original question posed: Are businesses affected by health code violations, and if so, to what degree? After performing several statistical analyses methods, we went with Spearman’s Correlation primarily because restaurant ratings from Yelp range from 1 to 5 with 0.5 steps between each rank, so Spearman’s Correlation made the most sense to deal with ranked data. The statistical analysis found that there is in fact a negative correlation between Yelp rating and health violation severity (calculated from averaging total health violations with # of days operated) with correlation coefficient of -0.67 and p value of less than 0.05. This shows that there is a negative relationship between restaurant Yelp rating and average health violation.
(BostonRestaurantsStatsAnalysis.py)

For the optimization problem, we seek the most ideal location for restaurants by exploring the relationship between rating + health violation severity and geographical location via longitude and latitude. Initially, k-means clustering was performed on average health violation severity and restaurant rating in hoping to find a distinct clustering as we predicted for lower rated restaurants to be grouped together for higher violation severity and vice versa, but found nothing insightful to help answer our question, new approaches were taken:

Restaurants and their relevant information are placed in a graph and distance is calculated based on longitude, latitude. For each restaurant a score is taken using rating and average health violation severity (scaled to the same magnitude) to be compared to average score of top 6 restaurants with lowest distance; this allows us to compare if geographic locations affect restaurants scores. Plotting restaurant own score compared to the average score of 6 closest reveals that geographic location does not affect a restaurant’s score given the insignificant correlation coefficient below 0.2.
(BostonRestaurants_FullyConnectedMap.py creates distance graph)
(BostonRestaurantsScoreComparisonAll3.py calculates and compares scores)

The second approach involves a visualization step where top 6 closest restaurants geographically are given edges and plotted onto a map to help derive further conclusion about the relationship between restaurant’s geographical location and score. We aim to continue developing this in project #3, potentially by color coding edges or nodes to help further visualize our findings.
(BostonScoring_Map.py)
(BostonScoringEvaluations.py)

Order of Algorithms to run:
  BostonRestaurantsStatsAnalysis.py

Algorithm A) BostonRestaurants_FullyConnectedMap.py then BostonRestaurantsScoreComparisonAll3.py  
Algorithm B) BostonScoring_Map.py then BostonScoringEvaluations.py
