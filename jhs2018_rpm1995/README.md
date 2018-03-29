# Exploring Environmental Friendliness of Neighborhoods in Boston

### Narrative
The idea of this project is to gather and transform data that would allow us to analyze how environmentally friendly the neighborhoods of Boston are.  Climate change is a serious concern and the U.S. is reponsible for 16% of all global carbon dioxide emissions, second only to China.  Documents released by The New York Times mention an increase in extreme precipitation events in the Northeast, and hold humans responsible for all observed warming since the mid 20th century.  We hope to try and figure out the extent to which neighborhoods in Boston are trying to reduce their carbon footprint, and offer insights into how they can try and improve their stance.

In this project, we collect datasets that give us the locations of Trees, Open spaces, Charging stations, and Hubway stations, as well as information about the neighborhoods of Boston.  Using appropriately collated data and a formula, we can hopefully assign a score to each neighborhood to understand how well they are doing on an environmental front.

### Data Portals Used

http://bostonopendata-boston.opendata.arcgis.com
https://data.boston.gov
http://datamechanics.io
https://boston.opendatasoft.com

### External Libraries Needed

Additional dependencies needed were geojson (a simple pip install will do) and shapely.  
https://pypi.python.org/pypi/Shapely
	
Machines running Windows might have to navigate to https://www.lfd.uci.edu/~gohlke/pythonlibs/#shapely, download the respective version that corresponds to their Python environments, and use pip install on the .whl file.

### Transformation 1
After downloading the datasets having run "getdata.py", "combineneighbourhood.py" projects certain columns from the two datasets about neighborhoods and saves a subset of the cross product of the two datasets, essentially carrying out a join operation, and multiple projections.  This final dataset is a list of all neigborhoods in Boston with their names and geographical coordinates that shape them.

### Transformation 2
"project_coordinates.py" applies a projection operation on the datasets related to trees, Hubway stations, and charging stations in Boston.  We transform the original datasets by storing the type of each asset ("tree"/"charge"/"hubway"), along with its coordinates in a new dataset.

### Transformation 3
"open_centroids.py" performs projections to get the coordinates of open spaces and grounds in Boston.  Some of the data are in unfavorable formats and we've taken that into account to produce a resulting dataset with a uniform format.  A function is applied to find the centroids of each of these spaces.  The resulting singular latitude and longitude is saved in a database with the type field set to "openspace".

### Transformation 4
"combine_data.py" carries out the final major transformation by essentially combining all the datasets.  It uses projected values from every dataset and maps the points of interest (environmentally- friendly assets) to one of the neighborhoods of Boston.  The resulting dataset ("greenneighbourhoods") contains, for every neighborhood in Boston, a list of asset types (e.g.: Hubway stations, charging stations, etc.) as well as their coordinates mapped to a single key value: a Boston neighborhood.

With the help of these transformations, we can now try and come up with a scoring function that takes into account these attributes to come up with a "Green Score" for each location.  We might even be able to suggest possible location to set up new assets in the future.
