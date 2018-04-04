# Project 2: Factors Influencing Crime in Boston


## Authors
* Janell Chen (janellc)
* Rebecca Stiffelman (rstiffel)
* Yash (yash)

**Question:**

What makes some Boston neighborhoods safer than other? What can the city of Boston do to help reduce crime rates? We hope to answer these questions by analyzing the influence of various factors, such as the presence of streetlights, on crime rates in different Boston neighborhoods.

**Datasets Used:**

*  Crimes - Analyze Boston
*  Neighborhoods
*  Streets
*  Streetlight Locations - Analyze Boston

**Transformations:**

* transformCrimes: Finds average point (lat, long) for each street in each district where crimes existed. This is for finding the "middle" of the street - used in findCrimeStats.
* sortNeighborhoods: Aggregates various data about crimes and streetlights to the neighborhood level. Given the coordinate for a streetlight or crime, this script checks to see if it falls within the bounds of a neighborhood.


## Optimization and Statistical Analysis
### Z3 Constraint Satisfaction (findCrimeStats.py)
We used Z3 constraint satisfaction to determine a model for each district (neighborhood). The model will tell us
the minimum number of night patrols needed in each district such that every street will be covered. Multiple streets can be covered by a night patrol if they're less than 1 block away (approx 800 ft. or 0.15 miles). We will use z3 to find a satisfiable model given these constraints. (similar to edge cover).

### Correlation between Streetlights and Crime (scoringLocation.py)
We aimed to uncover correlation between number of streetlights and number of crimes that occur in Boston neighborhoods.



## Required libraries and tools
You will need some libraries and packages.
```
python -m pip install shapely
python -m pip install pandas
install z3 via github instructions

```


**How to obtain new transformed datasets:**

* python execute.py janellc_rstiffel
* This will create 3 json files in janellc_rstiffel/transformed_datasets
