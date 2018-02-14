### Lubov McKone and Anna Goncharova: Project 1

## General Purpose

For Project 1, we aimed to retrieve and transform datasets that would allow us to look at changes in the built environment that might be associated with evictions and/or gentrification in emerging tech hubs such as Boston and San Francisco. Gentrification is often characterized by the emergence of new businesses that bring different types of jobs and services to an area (i.e. - startups, coworking spaces, juice bars).These jobs and services may serve to both attract higher earning residents in addition to being a response to the presence of upper class residences. These changes in jobs and services are often accompanied by physical changes in the built environment such as remodeling, new contructions, and house "flipping." In order to begin digging into the ways evictions might be geographically correlated with changes in both the function and appearance of the built environment, we gathered and transformed data sets from Boston and San Francisco on evictions, approved permits, property value assessments, and businesses. In the future, we hope to take a closer looking at the timing of the emergence of these different changes in order to potentially get a clearer picture of exactly how gentrification plays out longitudinally and geographically. 

1. **Foursqare API:**

	We queried the Foursquare API to get data about businesses in the Boston and San Fransisco areas. To work around Foursquare's query limit of 50 data points per request, we created a lattice of longitute and latitude points. We preserved unique data points returned with our query for "Office" and different longitute and latitude pairs. In total, we were able to retrieve 1208 data points for SF and 1478 data points for Boston. 
	
	**Some Limitations with this data source:** 

	* The Secret key to be used for the requests has to be reset frequently. 
	* The data does not contain any time references, so it is unlikely that we will be using it to see development over time. However, it is still a great for a quick reference for business activity in a certain area or part of a city.
	
	**Links to sources of data**
	
	* [Link to the official documentation](https://developer.foursquare.com/docs)
	* Link to our retrieval code 

2. **Permit data for Boston and SF**
	
	We gathered permit data using the Analyze Boston CKAN API and the DataSF Socrata API. This data contains information about approved changes to the built environment such as new constructions, additions, and remodeling.

3. **Property Assesment for Boston**
	
	We retrieved assessment data from Boston for the years 2014, 2015, 2016, and 2017. This data could allow us to determine whether certain units are owner-occupied and to determine how property values may have changed over time.

4. **Housing Inventory for SF**
	
	We retrieved San Francisco Housing Inventory data for 2011-2017. These datasets essentially contain housing specific permitting data, with additional information about the occupancy type of units.

5. **Eviction Data for Boston and SF**
	
	We retrieved eviction data from the DataSF Socrata API. This dataset will allow us to assess evidence of gentrification through housing displacement. We are currently in the process of acquiring permission to use GEOID-aggregated eviction data from the City of Boston for the years 2014, 2015, and 2016. 





 