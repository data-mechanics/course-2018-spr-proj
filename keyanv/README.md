Importance:
With the development of urban environments, it is imperative that policy makers allocate resources towards public works projects for the benefit of the community. However, such policy makers may be incentivized to localize the benefit that the "community" receives to a particular area. Ironically and unfortunately, these resources tend to go to those who need it least in order to attract wealthy people to certain areas and localize those with lower incomes to higher crime areas. Thus, these data sets can be combined to show how the development of public infrastructure and utilities, such as access to the MBTA, open spaces, and community pools can have a direct impact on the property values of an area as well as it's crime rate. Such an analysis can bring to light the issue at hand to the citizens and policy makers that may be unaware of such issues. Perhaps it is even possible, with such a data driven analysis to influence data driven decision making to determine where to develop future projects in order to maximize its utilitarian impact per dollar amount. Indeed, such a future would certainly align with a utopic view of a truly smart city.

The data for crime and property data was pulled from https://data.boston.gov in crime_data.py and property_data.py.

The data for open space and pool data was pulled from http://bostonopendata-boston.opendata.arcgis.com in open_space_data.py and pool_data.py.

Finally, the MBTA data was pulled from https://api-v3.mbta.com in mbta_stop_data.py.


For my first transformation, simplify_open_space.py, I decided to simplify the open space data in simplify_open_space.py to give it a non polygonal coordinate as an estimate to allow it to more easily interact with the other data sets. I did this by taking the average of the longitudes and latitudes for each polygon and use that as a center representation of the open space. I then calculated a rough circumradius by assuming the polygons where roughly normal in order to provide a very rough estimation as to the size of such parks. 

For my second transformation, combine_public_utilities.py, I decided to merge all of the public utilities into one format in order to have one source of longitude and latitude data, as well as extra information about each of the utilities.

For my third transformation, combine_crime_and_property.py, I merged the crime and property data to create a collection that can represent which types of streets had what types of crimes occured on them and their associated property values.

The outputs of the transformations are found in the json files: open_spaces_simplified.json, public_utilities.json, and crime_and_properties.json.
