# CS 591 L1 Project 2

Authors: Brooke Mullen <bemullen@bu.edu>, Dharmesh Tarapore <dharmesh@bu.edu>, Claire Russack <crussack@bu.edu>, Vincent Wahl <vinwah@bu.edu>


## Boston Library User Analysis


Move in week is universally chaotic and a generally unpleasant experience for both, college students and local residents. 

In this project, we attempt to isolate the most important factors that affect the residents' quality
of life. With this information, we can model the problem as a relatively straightforward optimization
problem with constraints.

For this part of the project, we are looking at what could be a positive correlation to the Boston cityscore. One of the aditive metrics is the citywide library attendence. Because our focus is on how the students affected the city, we looked at the correlation between the city score and library attendence when students were in session vs. when students are not in session.

The database used was:

1. <strong><a href="https://data.boston.gov/dataset/cityscore">CityScores</a></strong>: This provides a consolidated metric that reflects residents' satisfaction with the city's efforts. We use the average of a collection of these values as an approximate indicator of how painful move in week really is.


## Analysis

The analysis done in this file is done in 'StatLibrary.py.' After splitting our information into in session and not in session, we calculated the correlation between the city scores and the library attendence. This analysis was prompted by 'scatter_library.png' which is a plot of this data showing that there may be questionable correlation. The result was that when students were not in session, there was a .3558 correlation and in session there was a .7470 correlation. This means that there a strong correlation to the score when students are in session. 


## Evaluation

To run the code, setup the database structure as documented in the parent README and then run:

<code>python execute.py bemullen_crussack_dharmesh_vinwah</code>



