import dml
import prov.model
import datetime
import json
import uuid
import folium
import os
from math import cos, asin, sqrt


class display(dml.Algorithm):
    contributor = 'jhs2018_rpm1995'
    reads = ['jhs2018_rpm1995.greenassets']
    writes = ['jhs2018_rpm1995.kmeansdata']

    @staticmethod
    def findcell(value, axis):
        # return min(axis, key=lambda x: abs(x - value))    # Wrong logic... Spent an entire night on this line
        for i in range(1, len(axis)):
            if axis[i] > value:
                return axis[i - 1]

    @staticmethod
    def distance(lat1, lon1, lat2, lon2):
        p = 0.017453292519943295
        a = 0.5 - cos((lat2 - lat1) * p) / 2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
        return 12742 * asin(sqrt(a))

    @staticmethod
    def closestpoint(cells, long, lat):                     # Will never be able to recreate this again
        return min(cells, key=lambda x: display.distance(lat, long, x[0][1], x[0][0]))

    @staticmethod
    def execute(trial=False):
        # Retrieve datasets
        startTime = datetime.datetime.now()

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
                grid[coords] = [[0], [0], [0], [0], [0], [0]]  # [[charge], [hubway], [open spaces], [trees], [budget], [crime]]
                cells.append(coords)
                j += 0.01
            i += 0.01
        # map_osm.save(filenamemap)                                             #

        xaxis = []  # Adjust scale of grid here
        i = -71.189
        while i < -70.878:
            xaxis.append(i)
            i += 0.01

        yaxis = []  # Adjust scale of grid here
        i = 42.234
        while i < 42.406:
            yaxis.append(i)
            i += 0.01

        budget_coords = []  # To store coordinates of budgets
        for budget in budgets:
            budget_coords.append([budget['Location'], budget['Budget']])

        # for coords in budget_coords:
        #             print("For coords: " + str(coords))
        #             cell = display.closestcell(cells, coords[0], coords[1])
        #             folium.Marker(cell, icon=folium.Icon(color='green')).add_to(map_osm)

        for cell in cells:
            answer = display.closestpoint(budget_coords, cell[1], cell[0])  # Hallelujah
            grid[cell][4][0] += float(answer[1])  # Storing budget

        for asset in assets:  # This loop finds the cell that the asset belongs to and correspondingly
            y = asset['Location'][1]  # ...increases the count of that asset type in the dictionary
            x = asset['Location'][0]  # ...representation
            typekind = asset['Type']
            ycell = display.findcell(y, yaxis)
            xcell = display.findcell(x, xaxis)
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

        for coords, counts in grid.items():  # Gonna save to database and display on map
            megalist.append({"coordinates": coords, "charge_count": counts[0][0], "hubway_count": counts[1][0],
                             "open_count": counts[2][0], "tree_count": counts[3][0], "budget": counts[4][0],
                             "crime_count": counts[5][0]})
            folium.Marker(coords, popup=str(counts)).add_to(map_osm)

        repo.dropCollection("kmeansdata")
        repo.createCollection("kmeansdata")
        repo['jhs2018_rpm1995.kmeansdata'].insert_many(megalist)
        map_osm.save(filenamemap)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):

        # Create the provenance document describing everything happening
        # in this script. Each run of the script will generate a new
        # document describing that invocation event.

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jhs2018_rpm1995', 'jhs2018_rpm1995')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet',
        # 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bwod', 'https://boston.opendatasoft.com/explore/dataset/boston-neighborhoods/')  # Boston
        # Wicked Open Data
        doc.add_namespace('ab', 'https://data.boston.gov/dataset/boston-neighborhoods')  # Analyze Boston

        this_script = doc.agent('alg:jhs2018_rpm1995#Combine_Coordinates',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # #######
        resource_greenassets = doc.entity('dat:jhs2018_rpm1995_greenassets',
                                          {
                                              prov.model.PROV_LABEL: 'Coordinates of Environment Friendly Assets in '
                                                                     'Boston',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        get_grid = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                {
                                    prov.model.PROV_LABEL: "Locations of Green Assets in Boston in a "
                                                           "grid representation",
                                    prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(get_grid, this_script)

        doc.usage(get_grid, resource_greenassets, startTime)

        # #######
        grid = doc.entity('dat:jhs2018_rpm1995_kmeans',
                          {prov.model.PROV_LABEL: 'Coordinates of All Environment Friendly Assets in Grid',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(grid, this_script)
        doc.wasGeneratedBy(grid, get_grid, endTime)
        doc.wasDerivedFrom(grid, resource_greenassets, get_grid, get_grid, get_grid)

        repo.logout()

        return doc

# display.execute()
# doc = display.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
