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