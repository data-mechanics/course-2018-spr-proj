import dml
import prov.model
import datetime
import json
import uuid
import folium
import os
from math import cos, asin, sqrt


class display():
    contributor = 'jhs2018_rpm1995'
    reads = ['jhs2018_rpm1995.greenassets']
    writes = ['jhs2018_rpm1995.kmeansdata']

    def __init__(self, scale):
        self.execute(scale)

    def findcell(self, value, axis):
        # return min(axis, key=lambda x: abs(x - value))    # Wrong logic... Spent an entire night on this line
        for i in range(1, len(axis)):
            if axis[i] > value:
                return axis[i - 1]

    def distance(self, lat1, lon1, lat2, lon2):
        p = 0.017453292519943295
        a = 0.5 - cos((lat2 - lat1) * p) / 2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
        return 12742 * asin(sqrt(a))

    def closestpoint(self, cells, long, lat):                     # Will never be able to recreate this again
        return min(cells, key=lambda x: display.distance(self, lat, long, x[0][1], x[0][0]))

    def execute(self, scale):
        # Retrieve datasets
        startTime = datetime.datetime.now()
        #scale = float(input("Please enter Scale [0.01 - 0.1] "))

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jhs2018_rpm1995', 'jhs2018_rpm1995')

        print("Now running display.py")

        dir_path = os.path.dirname(os.path.abspath(__file__))
        # dir_path = "C://cs591//course-2018-spr-proj//jhs2018_rpm1995"
        filenamemap = os.path.join(dir_path, "assets.html")
        map_osm = folium.Map(location=[39, -98.1], zoom_start=4)

        assets = repo.jhs2018_rpm1995.greenassets.find()
        budgets = repo.jhs2018_rpm1995.greenobjects.find({"Type": {"$eq": "budget"}})

        # for asset in assets:                                  # Uncomment these four lines to see assets on the map
        #     coords = (asset['Location'][1], asset['Location'][0])
        #     folium.Marker(coords, popup=str(coords)).add_to(map_osm)
        # map_osm.save(filenamemap)

        grid = {}  # This will contain coordinates of the grid as keys, and assets assigned to that grid as values
        cells = []  # To hold just the coordinates of the grid
        megalist = []  # Will hold data to write to database

        i = -71.189
        while i < -70.878:
            j = 42.234
            while j < 42.406:
                coords = (j, i)
                # folium.Marker(coords, popup=str(coords)).add_to(map_osm)      # Uncomment to see grid on map
                # grid[coords] = 0                                              # For overall counts
                grid[coords] = [[0], [0], [0], [0], [0], [0]]  # [[charge], [hubway], [open spaces], [trees],
                # [budget], [crime]]
                cells.append(coords)
                j += scale
            i += scale
        # map_osm.save(filenamemap)                                             #

        xaxis = []  # Adjust scale of grid here
        i = -71.189
        while i < -70.878:
            xaxis.append(i)
            i += scale

        yaxis = []  # Adjust scale of grid here
        i = 42.234
        while i < 42.406:
            yaxis.append(i)
            i += scale

        budget_coords = []  # To store coordinates of budgets
        for budget in budgets:
            budget_coords.append([budget['Location'], budget['Budget']])

        # for coords in budget_coords:
        #             print("For coords: " + str(coords))
        #             cell = display.closestcell(cells, coords[0], coords[1])
        #             folium.Marker(cell, icon=folium.Icon(color='green')).add_to(map_osm)

        for cell in cells:
            answer = display.closestpoint(self, budget_coords, cell[1], cell[0])  # Hallelujah
            grid[cell][4][0] += float(answer[1])  # Storing budget

        for asset in assets:  # This loop finds the cell that the asset belongs to and correspondingly
            y = asset['Location'][1]  # ...increases the count of that asset type in the dictionary
            x = asset['Location'][0]  # ...representation
            typekind = asset['Type']
            ycell = display.findcell(self, y, yaxis)
            xcell = display.findcell(self, x, xaxis)
            if (ycell, xcell) in grid:  # O(1) lookup time. Hire me, Google
                # grid[(ycell, xcell)] += 1     # for overall counts
                if typekind == "charge":
                    grid[(ycell, xcell)][0][0] += 1
                elif typekind == "hubway":
                    grid[(ycell, xcell)][1][0] += 1
                elif typekind == "openspace":
                    grid[(ycell, xcell)][2][0] += 1
                elif typekind == "tree":
                    grid[(ycell, xcell)][3][0] += 1
                elif typekind == "crime":
                    grid[(ycell,xcell)][5][0] += 1
                    # grid[(ycell, xcell)][4][0] += 1
        for coords, counts in grid.items():  # Gonna save to database and display on map
            megalist.append({"coordinates": coords, "charge_count": counts[0][0], "hubway_count": counts[1][0],
                             "open_count": counts[2][0], "tree_count": counts[3][0], "budget": counts[4][0],
                             "crime_count": counts[5][0]})
            # megalist.append({"coordinates": coords, "charge_count": counts[0][0], "hubway_count": counts[1][0],
            #                  "open_count": counts[2][0], "tree_count": counts[3][0],
            #                  "crime_count": counts[4][0]})
            folium.Marker(coords, popup=str(counts)).add_to(map_osm)

        repo.dropCollection("kmeansdata")
        repo.createCollection("kmeansdata")
        repo['jhs2018_rpm1995.kmeansdata'].insert_many(megalist)
        map_osm.save(filenamemap)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}