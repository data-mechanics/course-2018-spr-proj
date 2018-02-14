# Project 1


**Question:**

What kinds of opportunities does Boston provide to help its population maintain active, healthy lifestyles? Does it provide those opportunities equally for the whole population? Particularly for parts of the population where obesity is more prevalent, how accessibe are these opportunities?

Our first two datasets look into the Boston population. The first provides demographic data-- specifically income, while the second provides information on the incidence of obesity within the city's population. With our next three datasets we explored three different avenues Boston offers to help people to maintain active, healthy lifestyles (Bike Paths, Open Space, Gyms and Health Clubs). By analyzing the locations of businesses and public spaces that promote healthy lifestyles, we hope to be able to assess their accessibility to people in areas where obesity and other exercise related health issues are most prevalent.

**Datasets Retrieved:**

*  Open Space (Analyze Boston)
*  Fitness Related Business in Boston (downloaded from Boston City Clerk's 'Doing Business As' database)
*  Existing Bike Network (Analyze Boston)
*  Income (American Community Survey via census.gov)
*  Obesity in Massachusetts (CDC)

**Transformations:**

* transformOpenSpace: Finds average size of a park in acres for each district/zipcode.
* transformFitBusiness: Aggregates various data about Open Space to district level, joins with information about fitness related businesses on zipcode
* transformBikeNetwork: Calculates the number of bike networks within 2 miles per tract district, along with medium income.

**How to obtain new transformed datasets:**

* cd into janellc_rstiffel
* python execute.py janellc_rstiffel
* This will create 3 json files in janellc_rstiffel/transformed_datasets
