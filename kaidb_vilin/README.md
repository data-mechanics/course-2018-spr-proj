## CS591 Project 1
## Members:  
- Kai Bernardini
- Vasily Ilin

 ## Overview:
We compare the public employee earning report and the monthly utility bills from Analyze Boston by zip code to see if there
is a correlation between the average income in a neighborhood (we approximated it using the income of public employees as a proxy)
and how much the neighborhood pays per unit of electricity. No correlation was found.
We also retrieved the CDC binge drinking dataset by state. So far we found ten census tracts in Boston with the worst binge drinking habits.
This will likely prove useful in analyzing city life later on. For example, comparing binge drinking with the locations of colleges
or the poorest neighborhoods in Boston in order to find what binge drinking correlates more with. From the 311 dataset, there are several complaints that have reason listed as mbta. We examine geospatial relationships between the mbta transportation data, payroll information by zipcode...etc

## Transformations: 
- Payrol: Convert dollar strings to floats
- 311: Project Data to remove extraneous columns. Combine Submission time, estimated completion time  and actual completion time to compute
    - Elapsed time 
    - Estimated time till completion
    - Extra Time used to complete task 
- CDC: Various descriptive statistics, selection via boston rows...etc


## MBTA Auth. 
Please add you MBTA API_v3 key to auth.json. 

<code> {"mbta_api_key": ""}
</code>

## Generate Prov. 
In the top directory of this project, run

<code>python execute.py kaidb_vilin --trial
 </code>
This will take at 1-10 minutes depending on your connection. 
## Python Reqs:
pandas, sklearn, dml, prov, json,urlib, json, datetime, uuid