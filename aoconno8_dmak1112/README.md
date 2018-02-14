For project 1, we plan on looking at the five following datasets: 

   UberMovement
   
   Analyze Boston Park Boston Monthly Transactions by Zone 2015 
   
   National Oceanic and Atmospheric Administration Local Climatological Data
   
   Analyze Boston Greenhouse Gas Emissions
   
   Hubway Trip History Data
   
The question we want to answer is: Can the weather in Boston play a role in increasing the rate at which global warming occurs?

We plan to combine these data sets to be able to analyze how bad weather in Boston can affect the levels of usage of different types and lengths of transportation, and then analyze how this in turn might have an effect on the environment based on differing levels of greenhouse gas emissions. We will do so by combining these data sets into three datasets.

One of these datasets (named weatherHubway) will combine the Hubway data during the year 2015 with the weather reports in 2015 and the transportations emissions data in 2015 to analyze how weather can affect Hubway usage and determine if there seems to be a correlation between hubway usage level and emissions.

Another dataset (weatherParking) will combine ParkBoston Monthly Transactions from the year 2015 with Weather and Emissions data from the same year. We make the assumption that if there are fewer parking transactions, there are fewer people driving cars. This dataset could then show us how weather can affect how many people drive and then analyze how that in turn might affect vehicle emissions.

A final dataset (weatherUber) will combine UberMovement and weather data in 2016 to analyze how weather might increase average length of Ubers along with amount of Ubers being used. We would normally compare this with emissions data, however unfortunately the emissions data does not extend past 2015 – this could be something we expand upon in the future as more data is released. For now, we could hypothesize that as amount and length of Uber trips increases, vehicle emissions would also increase.

We could conclude that increase vehicle emissions increases the rate at which global warming occurs, and therefore if there is a correlation between weather and emissions, different weather conditions from year to year in Boston may play a role in increasing the rate of global warming.

--- Requirements ---

Python 3.6

Pandas

dateutil

numpy
