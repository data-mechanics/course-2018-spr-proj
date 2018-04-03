Authors: Rachel Chmielinski, Rhonda Mak 

Project Goal:

Boston is unique, in that it is both an urban and educational hub, with incredibly high numbers of universities. We wanted to know, what the impact of a university is on its surrounding neighborhoods and intend to answer this question through the data we've found below. 

Data Sets:

getGrads.py - Number of graduates from every university, used to assess the magnitude of student presence per year
http://datamechanics.io/data/rmak_rsc3/Colleges_and_Universities.geojson

getNeighborhoods.py - A way to divide up Boston into manageable chunks/similar areas
https://data.boston.gov/dataset/boston-neighborhoods

getPopulation.py - A yearly record of Boston's population
https://datahub.io/core/population-city/r/unsd-citypopulation-year-fm.json

getRace.py - A yearly record of Boston's racial demographic makeup
http://datamechanics.io/data/rmak_rsc3/bostonracedemo.json

getSites.py - Open service sites, used as factor in assessing "quality of life" in each neighborhood
https://data.boston.gov/dataset/public-works-active-work-zones

getUniversities.py - Complete list of universities in Boston
https://data.boston.gov/dataset/colleges-and-universities


getWards.py - Another way to divide up Boston
https://data.boston.gov/dataset/wards


Transformations:

degreesPerNeighb.py - Aggregate degree, university, and neighborhood data sets, to determine which neighborhoods have the largest student impact

siteNeighbs.py - Aggregate site and neighborhood data to see which neighborhoods require the most attention 

uniNeighbs.py - Aggregate university and neighborhood data to count the number of universities in each 
neighborhood as well as overall -- determining scope of the problem



Run Instructions:
run execute.py