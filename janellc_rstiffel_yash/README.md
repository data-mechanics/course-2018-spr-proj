# Project 2: Factors Influencing Crime in Boston

We aimed 

## Authors
Janell Chen (janellc)
Rebecca Stiffelman (rstiffel)
Yash (yash)

**Question:**

What kinds of opportunities does Boston provide to help its population maintain active, healthy lifestyles? Does it provide those opportunities equally for the whole population? Particularly for parts of the population where obesity is more prevalent, how accessibe are these opportunities?

Our first two datasets look into the Boston population. The first provides demographic data-- specifically income, while the second provides information on the incidence of obesity within the city's population. With our next three datasets we explored three different avenues Boston offers to help people to maintain active, healthy lifestyles (Bike Paths, Open Space, Gyms and Health Clubs). By analyzing the locations of businesses and public spaces that promote healthy lifestyles, we hope to be able to assess their accessibility to people in areas where obesity and other exercise related health issues are most prevalent.

**Datasets Used:**

*  Crimes 
*  Neighborhoods
*  Streets
*  Streetlights

## Optimization and Statistical Analysis
### Z3 Constraint Satisfaction (findCrimeStats.py)
We used Z3 constraint satisfaction to determine a model for each district (neighborhood). The model will tell us
the minimum number of night patrols needed in each district such that every street will be covered. Multiple streets can be covered by a night patrol if they're less than 1 block away (approx 800 ft. or 0.15 miles). We will use z3 to find a satisfiable model given these constraints. (similar to edge cover).

### Correlation between Streetlights and Crime (scoringLocation.py)
We aimed to uncover correlation between number of streetlights and number of crimes that occur in Boston neighborhoods.

**Transformations:**

* transformCrimes: Finds average point (lat, long) for each street in each district where crimes existed. This is for finding the "middle" of the street - used in findCrimeStats.
* sortNeighborhoods: Aggregates various data about crimes and streetlights to the neighborhood level. Given the coordinate for a streetlight or crime, this script checks to see if it falls within the bounds of a neighborhood.



**How to obtain new transformed datasets:**

* python execute.py janellc_rstiffel
* This will create 3 json files in janellc_rstiffel/transformed_datasets
