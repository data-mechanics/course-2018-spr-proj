# Project 3: 

pandreah@bu.edu

## Web Application:

The web application created for this project contains:

1. A map (created using Folium) that displays the current Hubway Stations (blue circles) as well as the proposed locations for Hubway Stations (red pins). When the red pins are clicked, a popup shows the number or home addresses that this potential Hubway Station would be serving.
2. An address lookup. When a valid address is given as input to the form, the result will be:
      - the number of Hubway Stations within walking distance of the given address, or
      - if not within walking distance, if the address falls on the Hubway Extension Area (within riding distance of another Hubway Station).


*Must have a MongoDB collection containing the locations of the Hubway Stations (obtained by running **get_hubwayStations.py** from proj2). An instance of MongoDB should also be running.
