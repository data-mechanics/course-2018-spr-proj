For this project, the problem we aim to solve is to figure where would be a suitable location to install a new Hubway station. 

The data sets which we currently include are:
1. MBTA stops
2. Parking meter location
3. Street light location
4. Crime rate in boston
5. Hubway station 

Our initial thought is that the MBTA stops would discourage us from installing a new Hubway station since we think that Hubway station should aid people commute to places that MBTA can't be reached. The parking meter location also suggests that if there's a few parking spot in the area, it might be better be travel there by biking. Additionally, the street light location would be a good indicator to install as street lights will increase safety during commute. We also include crime rate data set as it can also serve as indicator help deciding whether the area is safe enough for bike traveling or not. Finally, the Hubway station dataset would serve as a location where we need to ignore. 

The data transformation that we have done so far is a merging between the locations of Hubway station and MBTA stops location and dropping all other unnecessary attributes. This would serve as all the location that would discourage us from installing a new station. The next data transformation that we did is merging between street light and crime spot. For this data set, we convert from CSV to JSON and leave with only location data. These data would contribute from a security aspect. Finally, we merge the crime and parking meter location as these are factors which would discourage us from installing the Hubway station. 