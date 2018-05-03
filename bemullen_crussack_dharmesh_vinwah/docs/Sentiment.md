### Author: Dharmesh Tarapore <dharmesh@bu.edu>
The files 'RetrieveTweets.py' and 'TransformTweets.py' are used to retrieve Tweets about the city of Boston made during the years 2016 and 2017 and then add an ISODate field to them for easy querying.

Tweets (particularly) collected over a large (logically connected) area serve as efficient approximators of societal-happiness. For a detailed look into the mechanics of sentiment analysis and our results, please download our report <a href="https://cs-people.bu.edu/dharmesh/cs591/report.pdf" target="_blank">here</a>.

---------------------------------------------------------------------------------------------------------------------

RetrieveTweets.py:
Requests tweets made within 50 kilometres of Boston City (42.3601° N, 71.0589° W) from 2016-01-01 to 2017-12-31 from and stores the data into the mongoDB collection 'bemullen_crussack_dharmesh_vinwah.tweets'. The script retrieves a copy of this data, retrieved 4th of April 2018, stored at our<a href="https://cs-people.bu.edu/dharmesh/cs591/">data portal</a>. To login, use your BU login name and your BU ID number (including the uppercase 'U').

---------------------------------------------------------------------------------------------------------------------

TransformTweets.py:
Imports the tweets from the mongoDB collection and adds ISODate fields to each tweet. 

Each Tweet is then run through the <a href="https://pypi.org/project/dharmSentiment/" target="_blank">dharmSentiment</a>module to get an idea of how the city's sentiment as a whole evolves each month.

---------------------------------------------------------------------------------------------------------------------

Results

Results are documented and explained in the report.
















