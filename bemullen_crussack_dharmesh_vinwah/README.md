# CS 591 L1 Project 1 

Authors: Brooke Mullen <bemullen@bu.edu>, Dharmesh Tarapore <dharmesh@bu.edu>, Claire Russack <crussack@bu.edu>, Vincent Wahl <vinwah@bu.edu>

## The Impact of College Students on Boston's residents

Move in week is universally chaotic and a generally unpleasant experience for both, college students
and local residents. 

In this project, we attempt to isolate the most important factors that affect the residents' quality
of life. With this information, we can model the problem as a relatively straightforward optimization
problem with constraints.

To that end, the datasets we are using are:

1. <strong><a href="https://data.boston.gov/dataset/cityscore">CityScores</a></strong>: This provides a consolidated metric that reflects residents' satisfaction with the city's efforts. We use the average of a collection of these values as an approximate indicator of how painful move in week really is.

2. <strong><a href="https://data.boston.gov/dataset/311-service-requests/resource/2968e2c0-d479-49ba-a884-4ef523ada3c0">311 Service Requests</a></strong>: This dataset contains over 400,000 detailed records of complaints and/or requests filed by residents. We sift through irrelevant requests by filtering for the type of service request (more details in the comments in the JS files), among others.

3. <strong><a href="https://data.boston.gov/dataset/fire-incident-reporting">Boston Fire Incident Reporting</a></strong>: This dataset provides us with a rich history of fire incidents reported across the city of Boston, allowing us to glean trends, particularly during move in week.

4. <strong><a href="gis.cityofboston.gov/arcgis/rest/services/Education/OpenData/MapServer/2">Spatial Dataset of Colleges and Universities in the Greater Boston Area</a></strong>: This is used to construct 2d-index-capable queries on the other 4 datasets.

## Directory Structure

The root directory contains algorithms used to retrieve and store data from a myriad of portals into Mongo collections. All Python files contained here implement the DML algorithm.

### Project 2 Specifics

Information about specifics pertaining to project 2 can be found in the <a href="docs/">docs</a> directory.

Certain data transformations were previously accomplished using pure Javascript. These can be found in the <a href="scripts/">scripts</a> directory.


## Evaluation

To run the code, setup the database structure as documented in the parent README and then run:


<code>pip install -r requirements.txt && python execute.py bemullen_crussack_dharmesh_vinwah</code>