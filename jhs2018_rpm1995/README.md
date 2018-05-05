# Exploring Environmental Friendliness of Neighborhoods in Boston

### Narrative
The idea of this project is to gather and transform data that would allow us to analyze how environmentally friendly the neighborhoods of Boston are.  Climate change is a serious concern and the U.S. is reponsible for 16% of all global carbon dioxide emissions, second only to China.  Documents released by The New York Times mention an increase in extreme precipitation events in the Northeast, and hold humans responsible for all observed warming since the mid 20th century.  We hope to try and figure out the extent to which neighborhoods in Boston are trying to reduce their carbon footprint, and uncover any correlation between this extent and the socioeconomic status of a neighborhood.

In this project, we collect datasets that give us the locations of Trees, Open spaces, Charging stations, and Hubway stations, as well as specific information about areas in Boston, such as Budget Allocation for various departments and Incidents of Crime.

### Data Portals Used

http://bostonopendata-boston.opendata.arcgis.com
https://data.boston.gov
http://datamechanics.io
https://boston.opendatasoft.com

### External Libraries Needed

Additional dependencies needed were geojson, Folium (a simple pip install will do), and shapely.  
https://pypi.python.org/pypi/Shapely
	
Machines running Windows might have to navigate to https://www.lfd.uci.edu/~gohlke/pythonlibs/#shapely, download the respective version that corresponds to their Python environments, and use pip install on the .whl file.

### getdata.py
This script downloads the various datasets mentioned above and stores each of them in their own Mongo collection.

### open_centroids.py
"open_centroids.py" performs projections to get the coordinates of open spaces and grounds in Boston.  Some of the data are in unfavorable formats and we've taken that into account to produce a resulting dataset with a uniform format.  A function is applied to find the centroids of each of these spaces.  The resulting singular latitude and longitude is saved in a database with the type field set to "openspace".

### project_coordinates.py
"project_coordinates.py" applies a projection operation on the remaining datasets (trees, Hubway, charge, budget, crime).  We transform the original datasets by storing the type of each asset ("tree"/"charge"...), along with its coordinates in a new dataset.

### Combine_Coordinates.py
This script merely combines data from the transformed open spaces dataset with the collection formed by "project_coordinates.py" to a new collection called "greenassets".

### display.py
This script performs several major transformations and draws inspiration from the Grid File System- a Point Access Method for indexing spatial objects.  We start by dividing the area of Boston into grids.  Every record in the "greenassets" collection is then mapped to a single cell in the grid.  So every cell (referenced by its lower left point) is now represented as a feature vector, where each element gives us the count of a particular asset (tree, Hubway station, etc.).

The "budget" field of this feature vector is assigned a value a little differently: every cell in the grid space is associated to the closest location assigned a specific budget, as opposed to a budget location being allocated to just one cell.

### kmeans_correlation.py
This script takes in as input the feature vectors produced by the previous script, performs filtering, weighting, and scaling steps, and then runs the k- Means algorithm on the feature vectors.  This essentially grouped areas in Boston that have similar distributions of "green assets".  We then tried to check if these distributions correlated with certain socioeconomic indicators (budgets assigned to schools, crime rates, etc.), with the distributions of Open Spaces in Boston showing a slight correlation with number of crime incidents.  The Rand Measure of this similarity is written to the "observations" collection.

Higher correlation values might be obtained by pruning the grid cells used in the clustering step.  This is because a lot of the point locations came from different data portals, with the range of each dataset limited to certain locations in and around Boston, skewing the shape of the clusters.

### flask_app.py
A flask webapp that visualizes the k means clustering of Open Spaces, Crime, Hubway Stations, and Charging Stations. Users can change the size of the initial scalar for transforming greenassets into a grid file as well as change the value of k for kmeans.

To run the app, install all dependencies and enter "python flask_app.py" into the terminal.
