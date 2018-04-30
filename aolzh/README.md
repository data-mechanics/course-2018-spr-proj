# course-2018-spr-proj
Dataset:
NewYork Subway
NewYork Crime
NewYork School
NewYork Library
NewYork Hospital
NewYork Stores
NewYork Houses

File:
getdata.py
houses_attributes.py
norm_houses.py
correaltion.py
cluster.py

Project Description:

By using these datasets, I want to figure out if I were new to NewYork, which parts of this city are good to be residence community.

There are three parts of this project.

First I implement a web crawler to get the newyork house data from Zillow. It contains the location and price of each house.

Seconde, In order to make all data easy to be clustered and calculated, I extract the useful information from datasets and do the normalization by using the Z-order method.
Also, I give all houses rates based on their price, for example, if the price of a house is almost the same as average price, the rate is 3 stars.

Third, I use scipy.stats.pearsonr() to calculate the correlation coefficient and p value of each factor that may influent the house scoer, and use K-means to cluster all houses into 10 groups. I use the average price and score as the label names so it's easy to find out which parts are good.

Problem:

I can execute every single file successfully but if I use the execute.py to run all files, there will be a problem

"File "execute.py", line 64, in <listcomp>

    wasDerivedFrom = [(v['prov:usedEntity'], v['prov:generatedEntity'], 'wasDerivedFrom') for v in prov_json['wasDerivedFrom'].values()]

TypeError: list indices must be integers or slices, not str"

I don't know how to fix this.
