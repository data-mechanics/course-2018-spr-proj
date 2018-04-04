The problem we are addressing is the advent of Uber and Lyft, where people are increasingly using these less environmentally friendly services for the sake of convenience. Our solution combines data from Hubway and MBTA indepedently and utilizing them to find the optimal way to get to their favorite entertainment/restaurant locations. We used a custom algorithm to find the closest station, either Hubway or MBTA, to various popular entertainment and food venues. Moreover, with the crime data ready to use, we will be able to compare the coordinates of the resulting destination/station pairs with the crime, and determine if going to a certain place is worth it or not. . The data we pulled from entertainment/restaurants have been filtered extensively to find only the passing-grade locations. We decided to combine Entertainment and Food to one dataset to better organize and filter repeat data and targeting only the location (which must be in Boston area) and business name.

To see our application at work, type the following into your console:

python hubway.py
python mbta.py
python entertainment.py
python food.py
python crime.py
python projectDestinationData.py
python projectStationData.py
python findClosest.py


This will enable you to see the various destination/station pairings, giving everyone a better look at an environmentally-friendly way of having a good night out.


-----------------------------------------------------------------
-----------------------------------------------------------------

PROJECT #2 README

-----------------------------------------------------------------

Now that we have the optimal destination/station pairings for a good night out, we have to start considering other aspects people think about. Since we are in an urban environment, a primary concern people should have is safety. Therefore, we decided to extend on our initial pairings, and also try to find patterns between popular and accessible entertainment destinations and areas in Boston. Building off of the dataset produced by findClosest.py, we can use the coordinates of the destination-station pairings, and input them into the Google Maps API to see just how far the station is from the destination. With this information, we calculated the average walking distance from stations to destinations, and then used that to determine which areas of Boston are most convenient to get to, by calculating standard deviations of walking distance, and adding z-scores to the data points, allowing people less exposure to a night-time urban environment and increasing safety. Moreover, the data visualizations regarding various areas of Boston that are convenient for people, which will be implemented in Project #3, can also have other implications, such as helping people see where gentrification has struck hardest and which areas need more attention and development. Although the most direct application could be safety, our data transformations and statistical analysis can have many other applications as well.