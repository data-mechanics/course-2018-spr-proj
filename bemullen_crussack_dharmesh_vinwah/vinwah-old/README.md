In this project, I utilize date from 3 sources to get a better understanding of how the universities and colleges changes the surrounding area. In this first project, I have gathered data about properties in Boston, MBTA buss stops, universities and colleges, and businesses located in Boston, and crime data. This list can be extended with for example hospital locations, street maps, car accidents, etc., to further look at important aspects of local communities within Boston. 

In this project, I develop 3 new data sets that will help in this analysis. The first is locations of crime centers in boston, which is developed from the crime data by cleaning the data and applying K means. The algorithm uses the 10,000 last crime reports retrieved from the data portal, to find 5 clusters of areas with more crime. This data is further used in the next dataset developed. To further enhance the insight provided by this data set, I will in my following project add information, for every center, about the number of incidents associated with the center and what radius the crime area extends over.

The second data set uses university locations and crimeCenters to find, for each school, the closest crim center. This data can further be used to look for relationships between schools and reported crimes.

The last data set developed in this project is the extraction of main categories of businesses, with points describing the location of each business within that category. This data can be used to find areas where a category is more prevalent or find which category is most prevalent in a given area. I expect to use this to find what kind of businesses schools attract. 


For future projects, I want to further extend how this data can be developed into an interactive application that can help new students, and/or anyone who are intrested in sub-regions of boston, to get visual representation of the available data. Currently, my idea is to let the user have an interactive map where the user can select categories that will display the area on the map and how it relates (or not relates) to universities/selected universities.


Crdentials:
{
	"yelp_key": <your key> 
}

Key is availale at Yelp's developer page


The submited datasets will be generated when execute() is used

