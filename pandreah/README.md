# Project 2: 

pandreah@bu.edu

## Motivation:

Using data on addresses and Hubway Stations in Boston, I am looking to find where new Hubway Stations could be located in order to extend the Hubway Network around the city of Boston.

## Data Set:

Hubway Stations (from thehubway.com)

Boston Addresses (from data.boston.gov)

## Method:

1. Getting "current Hubway demand and network": calculating how many homes are within a 1KM radius of each Hubway station (walking distance) in to order get a proxy of its demand. Then, I calculate the connection of this station to the Hubway Network by counting how many stations are in a 3KM radius (a 30 minutes biking distance, a time restriction imposed on the membership).
2. Getting a "threshold demand" for adding a new station to the network: finding what kind of relationship there is bewtween population density (in relation to Hubway stations) and number of Hubway Network connections, by calculating a regression between these two variables. At the same time, I calculate the average and stadard deviation of the demand of each Hubway station. Calculating these two statistics can help when deciding which area might be better served by a new station.
3. Getting "potential Hubway demand and network": selecting houses that are outside of the Hubway range, that means they are not within a walking distance of any station (more than 1KM away), but are enough that putting a new station would make them fall inside the hubway network (no more than 4KM from the closest station).   
