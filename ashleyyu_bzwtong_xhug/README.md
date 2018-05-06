Project Narrative

#Project 1

Boston has multiple K-12 schools, colleges, and universities, many of which are ranked top schools in the nation. Even so, as Boston is a major metropolis, it is also filled with crime, from minor infractions to serious felonies. We wanted to analyze the concentration of these crimes in relation to property values and nearby schools in the area. The five datasets we focused on were Public Schools, Non Public Schools, Colleges and Universities, Property Values, and Crime in Boston. We aggregated Public Schools, Non Public Schools, and Colleges and Universities data sets based on zip code so we can find the concentration of schools in the area. Later we hope to further combine these data sets with the zip codes in property values and crime so we can see if there is any correlation between the prevalence of crime, property values, and education in Boston. 

#Project 2

For part 2, we want to improve the safety for students in Boston. We want to build student help centers, so students who are passing through the area and felt threatened (being followed, harrased, etc) can come to the help centers if they didn't feel comfortable or necessary or unable to call the police.

For constraint satisfaction, we believe help centers should be located in areas where crimerates are higher, and should be at least one km away from any police departments (otherwise it would be useless). In addition, no more than five help centers are allowed considering budgets and rental spaces. We used k-means algorithm to find geographical centers of areas with higher frequency of crime. Then we filtered out centers that doesn't satisfact our constraints. The remaining centers are help center locations. 

The problem is solved in buildhelpcenters.py

For our statistical analysis, we want to find out whether the presence of police reduces crime in nearby areas. If that's the case, then help centers should be built even further away from police stations to be more useful. So we calculated the correlation coefficient between distance to closest police department and number of crimes in the area. Distances are categorized in ranges of 0.5km (i.e. 0~0.5km, 0.5~1km ... 5~5.5km). We found a correlation of around -0.865 which shows that the closer to a police station, the higher crime rate in the area. This does not necessarily imply that police "attracts" crimes, since police departments are usually in areas of higher population, where crime naturally occurs more often. But now we know at least that police stations don't help reduce crime. Therefore it's okay to build help centers not far away from police stations. 

The problem is solved in policeCrimeCorrelation.py

Geographical informations are derived by retrieving latitudes and longitudes from databases or datasets.