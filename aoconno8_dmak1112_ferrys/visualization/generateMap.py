import osmnx as ox, networkx as nx
import urllib.request
import json
import folium
from tqdm import tqdm
import geopandas as pd

def generateGraph(r, G):
    print("Converting to GeoDataFrame")
    # turn graph into geodataframe
    gdf = ox.graph_to_gdfs(G, fill_edge_geometry=True)
    gdf_nodes = gdf[0]
    gdf_edges = gdf[1]
    
    # get graph centroid
    x, y = gdf_edges.unary_union.centroid.xy
    graph_centroid = (y[0], x[0])
    
    gdf_edges_red = gdf_edges[['geometry', 'u', 'v']]
    
    graph_map = folium.Map(location=graph_centroid, zoom_start=12, tiles='cartodbpositron')
    print("Generating folium map")
    for alc_license in tqdm(r):
        route_dist = alc_license["optimal_route"]["route_dist"]
        streetlights = alc_license["optimal_route"]["streetlights"]
        total_streetlights = 0
        for streetlight in streetlights:
            total_streetlights += streetlight[1]
        if route_dist > 8000:
            # These are outliers, so we are removing them for visualization
            continue
        route = alc_license["optimal_route"]["route"]

        route_nodes = list(zip(route[:-1], route[1:]))
        index = [gdf_edges_red[(gdf_edges_red['u']==u) & (gdf_edges_red['v']==v)].index[0] for u, v in route_nodes]
        gdf_route_edges = gdf_edges_red.loc[index]

        alc_name = str(alc_license["alc_name"]).replace("'", "")
        mbta_name = str(alc_license["optimal_route"]["mbta_name"]).replace("'", "")
        popup_html =  "<p style='font-family:arial;'>Location: " + alc_name + "<br>MBTA Stop: " + mbta_name + "<br>Total Streetlights: " + str(total_streetlights) + "<br>Route Distance: " + str(round(route_dist, 3)) + " meters</p>"
        for _, edge in gdf_route_edges.iterrows():
            locations = list([(lat, lon) for lon, lat in edge['geometry'].coords])
            iframe = folium.IFrame(html=popup_html, width=400, height=100)
            popup = folium.Popup(iframe, max_width=2650) 
            pl = folium.PolyLine(locations=locations, popup=popup,
                             color='#c70039', weight=5, opacity=1)
            pl.add_to(graph_map)

        # add markers for the start and end nodes
        gdf_end_node = gdf_nodes[(gdf_nodes['osmid']==str(route[-1]))]
        for _, row in gdf_end_node.iterrows():
            coords = list(row['geometry'].coords)[0]
            route_end_node = (coords[1], coords[0])
        folium.RegularPolygonMarker(route_end_node, weight=0, fill_color= "#FF5733", fill_opacity=.9, number_of_sides=15, radius=5, popup=mbta_name).add_to(graph_map)
        
        gdf_start_node = gdf_nodes[(gdf_nodes['osmid']==str(route[0]))]
        for _, row in gdf_start_node.iterrows():
            coords = list(row['geometry'].coords)[0]
            route_start_node = (coords[1], coords[0])
        folium.RegularPolygonMarker(route_start_node, weight=0, fill_color= "#900C3F", fill_opacity=.9, number_of_sides=15, radius=5, popup=alc_name).add_to(graph_map)
        
    filepath = 'output/viz.html'
    graph_map.save(filepath)

    return graph_map

print("Generating Boston graph")
G = ox.graph_from_place('Boston, Massachusetts, USA', network_type='drive')

print("Getting dataset")
url = 'http://datamechanics.io/data/aoconno8_dmak1112_ferrys/optimization.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)


generateGraph(r, G)
