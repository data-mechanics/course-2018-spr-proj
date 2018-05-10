import folium
from folium import plugins
import pymongo
from geopy.distance import vincenty
import random

def generateRandomHexColor():
    r = lambda: random.randint(0,255)
    return '#%02X%02X%02X' % (r(),r(),r())

def getVDist(lat1, long1, lat2, long2):
    return vincenty((lat1, long1), (lat2, long2)).kilometers

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('liwang_pyhsieh','liwang_pyhsieh')
data_set = repo['liwang_pyhsieh.joined_crash_analysis'].find()
kmean = repo['liwang_pyhsieh.crash_cluster_medians'].find()
c_dist = [x for x in repo['liwang_pyhsieh.crash_cluster_distribution'].find()]

map_osm = folium.Map(location = [42.3186708, -71.0923346], zoom_start =12)

'''
nd_array = []
for idx, item in enumerate (data_set):
    # folium.Marker( [item["nearest_hospital"]["Lat"] , item["nearest_hospital"]["Long"]],
    #                 icon = folium.Icon(color = 'red', icon = 'police-sign')
    #                 ).add_to(map_osm)
    nd_array.append([item["location"]["coordinates"][1] , item["location"]["coordinates"][0]])
'''

# Cluster IDs are 0-based index
colstr_point_cluster = []

for idx, item in enumerate (kmean):
    cluster_radius = 0
    colstr_point_cluster.append(generateRandomHexColor())
    for point in c_dist:
        if point["cluster_id"] == item["_id"]:
          p_c_dist = getVDist(point["coordinates"]["Lat"], point["coordinates"]["Long"], item["coordinates"]["Lat"], item["coordinates"]["Long"])
          cluster_radius = max(cluster_radius, p_c_dist)
    
    folium.Circle( [item["coordinates"]["Lat"], item["coordinates"]["Long"]  ],
                        radius = cluster_radius*1000,
                        popup = folium.Popup(str(idx),parse_html=True),
                        fill=True,
                        fill_color='#F14D39',
                        color = 'grey',
                        fill_opacity=0.15
 
                        ).add_to(map_osm)
    folium.Marker ( [item["coordinates"]["Lat"], item["coordinates"]["Long"]  ],
                        popup = folium.Popup(str(idx),parse_html=True),
                        icon = folium.Icon(color = 'red', icon = 'info-sign') 
                        ).add_to(map_osm)
for point in c_dist:
    randcol = generateRandomHexColor()
    folium.CircleMarker( [point["coordinates"]["Lat"], point["coordinates"]["Long"] ],
        radius=3,
        fill=True, fillColor=colstr_point_cluster[point["cluster_id"]], fill_opacity=1.0,
        color=colstr_point_cluster[point["cluster_id"]], weight=1,
    ).add_to(map_osm)

repo.logout()

map_osm.save('./Map_Clustering.html')