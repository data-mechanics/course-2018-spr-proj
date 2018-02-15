
# Project 1

### Team:

*rikenm@bu.edu* <br />
*bm181354@bu.edu*


## Hypothesis:


Filtering and obtaining datasets from Transportation department, Housing department, market value, crime rate and so forth of datasets of the Greater Boston area.

With this information, we can calculate the CPI *--Consumer Price Index*. As described by U.S. Bureau of Labor Statistics - **'The Consumer Price Index (CPI) is a measure of the average change over time in the prices paid by urban consumers for a market basket of consumer goods and services'**.
Typically a market basket consist of Housing, Transportation, Medical care, Good and Services. Accumulating this data with other factors such as crime, neighborhood  will results in a number which when compare within city, we will be able to find difference in price inflation, livability among city of the Greater Boston Area.

### Solving this problem we will be able to create an algorithm to determine which place in the city is good/best for living. This will be done by factoring all the indices created through the algorithm.


## Data set:

1. Public school record from the Greater Boston area <br />
'http://datamechanics.io/data/bm181354_rikenm/Public_Schools.csv'

2. Transportation and Housing data set <br />
'http://datamechanics.io/data/bm181354_rikenm/htaindex_data_places_25.csv'

3. Crime in the neighborhood of the Greater Boston area <br />
'https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b/download/crime.csv'

4. Emergency Medical Service EMS Stations <br />
'http://datamechanics.io/data/bm181354_rikenm/Emergency_Medical_Service_EMS_Stations.csv'

5. Liquor Dataset to relate to Crime in the area <br />
'https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b/download/crime.csv'


## Data Transformation :

### Index Transformation:

We filtered the data set to get the required field and applied a function to find the index of Transportation, Housing on all the attribute of the dataset.  Concatenated all the new data as new dataset

### School Data Transformation:

We again filtered the dataset and applied lambda function to obtain all the information from those cities of the Greater Boston area.  Computed the indices for the school all over the city and then created a new series of dataset from old and new dataset


### Emergency Medical Transformation:

We selected relevant city information, applied a lambda function which projected it as ( City, total number of medical service ) -- City works as a key here. Computed an index with another function. Concatenated it with old and new data set to from brand new dataset.


## To Run:

```python
python execute.py biken_riken
```

While running the program, it might take few extra seconds to run. Partly due to loading some of the heavy datasets into the program.




