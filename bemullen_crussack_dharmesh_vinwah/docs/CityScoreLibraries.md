### Author: Brooke Mullen <bemullen@bu.edu>

In this project, we attempt to isolate the most important factors that affect the residents' quality
of life. With this information, we can model the problem as a relatively straightforward optimization
problem with constraints.

For this part of the project, we are looking at what could be a positive correlation to the Boston cityscore. One of the aditive metrics is the citywide library attendence. Because our focus is on how the students affected the city, we looked at the correlation between the city score and library attendence when students were in session vs. when students are not in session.

The database used was:

1. <strong><a href="https://data.boston.gov/dataset/cityscore">CityScores</a></strong>: This provides a consolidated metric that reflects residents' satisfaction with the city's efforts. We use the average of a collection of these values as an approximate indicator of how painful move in week really is.


## Analysis

The analysis done in this file is done in 'StatLibrary.py.' After splitting our information into in session and not in session, we calculated the correlation between the city scores and the library attendence. This analysis was prompted by the following plot:

<center>
	<img src="https://cs-people.bu.edu/dharmesh/cs591/591data/city_score_scatter.png"/>
</center>
which suggests that there may be a questionable correlation. 

The result was that when students were not in session, there was a .3558 correlation and in session there was a .7470 correlation. This means that there a strong correlation to the score when students are in session. 