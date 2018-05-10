import folium
from folium import plugins
import pymongo

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('liwang_pyhsieh','liwang_pyhsieh')
data_set = repo['liwang_pyhsieh.joined_crash_analysis'].find()
hospital = repo['liwang_pyhsieh.hospitals'].find()
police_station = repo['liwang_pyhsieh.police_stations'].find()
candidate_hospitals = repo['liwang_pyhsieh.candidate_position_hospital'].find()

def parseLoc(s):
    locpair = s.split("\n")[-1][1:-1].split(",")
    return float(locpair[0]), float(locpair[1])



map_osm = folium.Map(location = [42.3186708, -71.0923346], zoom_start =12)

nd_array = []
for idx, item in enumerate (data_set):
    # folium.Marker( [item["nearest_hospital"]["Lat"] , item["nearest_hospital"]["Long"]],
    #                 popup = "Ni",
    #                 icon = folium.Icon(color = 'red', icon = 'police-sign')
    #                 ).add_to(map_osm)
    nd_array.append([item["location"]["coordinates"][1] , item["location"]["coordinates"][0]])

for dataobj in hospital:
    lat, longt = parseLoc(dataobj["Location"])
    folium.Marker( [lat,longt],
                popup = folium.Popup(dataobj["NAME"],parse_html=True),
                icon = folium.Icon(color = 'red', icon = 'info-sign')
                ).add_to(map_osm)

for dataobj in police_station:
    folium.Marker( [dataobj["Y"],dataobj["X"]],
                popup = folium.Popup(dataobj["NAME"], parse_html=True),
                icon = folium.Icon(color = 'blue', icon = 'info-sign')
                ).add_to(map_osm)

for dataobj in candidate_hospitals:
    folium.Marker( [dataobj["Lat"],dataobj["Long"]],
                icon = folium.Icon(color = 'green', icon = 'ok-sign')
                ).add_to(map_osm)


map_osm.add_child(plugins.HeatMap(nd_array, radius = 20))
repo.logout()
map_osm.save('./Map_Hospitals_PoliceStations.html')
