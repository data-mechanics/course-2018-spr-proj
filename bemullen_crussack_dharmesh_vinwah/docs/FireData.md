## Fire Incidents KMeans

### Author: Claire Russack <crussack@bu.edu>

The data retrieved by "RetrieveFire.py" is a collection of all Fire Incident Reports in Boston from May, September, and December of 2017, from the data portal Analyze Boston. The data is projected to include only the unique incident numbers and the address, then Google's geocoder is used to convert the addresses to latitude and longitude points. This data is then stored in mongoDB in the collection "bemullen_crussack_dharmesh_vinwah.fires".

This data is further used in "TransformFireData.py" to do one K-Means for each of the months. This is done to determine if during the month of September, the fire incidents clusters closer to areas that students live in, such as Allston and Medford.

The K-means algorithm is initialized with k=2 (i.e 2 clusters) for each month, based on the K-means silhouette score being the highest for k=2. A high silhouette score indicates that each point has high similarity to its own cluster compared to other clusters. The silhouette score determined for May, for k in range [2,12], can be seen below.


<center>
	<img src="https://cs-people.bu.edu/dharmesh/cs591/591data/Silhouette_Score.png"/>
</center>



The centroids determined by this transformation is given below:

May Centroids =  [[ 42.34223224 -71.06216747] (South End, 02118)
                  [ 42.31453459 -71.14446154]] (Brookline, 02467)

September Centroids =  [[ 42.30666775 -71.10363356] (Jamaica Plain, 02130)
                        [ 42.3533682  -71.06483576]] (Boston, 02108)

December Centroids =  [[ 42.28743448 -71.09755387] (Mattapan, 02124)
                       [ 42.34583514 -71.07241684]] (Back Bay, 02116)
