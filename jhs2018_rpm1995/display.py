import dml
import prov.model
import datetime
import json
import uuid
import folium
import os


class display(dml.Algorithm):
    contributor = 'jhs2018_rpm1995'
    reads = ['jhs2018_rpm1995.greenassets']
    writes = ['jhs2018_rpm1995.kmeansdata']

    @staticmethod
    def findcell(value, axis):
        # return min(axis, key=lambda x: abs(x - value))    # Wrong logic... Spent an entire night on this line
        for i in range(1, len(axis)):
            if axis[i] > value:
                return axis[i-1]

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
        filenamemap = os.path.join(dir_path, "assets.html")
        map_osm = folium.Map(location=[39, -98.1], zoom_start=4)

        assets = repo.jhs2018_rpm1995.greenassets.find()

        # for asset in assets:                                  # Uncomment these four lines to see assets on the map
        #     coords = (asset['Location'][1], asset['Location'][0])
        #     folium.Marker(coords, popup=str(coords)).add_to(map_osm)
        # map_osm.save(filenamemap)

        grid = {}       # This will contain coordinates of the grid as keys, and assets assigned to that grid as values
        megalist = []   # Will hold data to write to database

        i = -71.189
        while i < -70.878:
            j = 42.234
            while j < 42.406:
                coords = (j, i)
                # folium.Marker(coords, popup=str(coords)).add_to(map_osm)      # Uncomment to see grid on map
                # grid[coords] = 0                                              # For overall counts
                grid[coords] = [[0], [0], [0]]                                  # [[charge], [hubway], [open space]]
                j += 0.01
            i += 0.01
        # map_osm.save(filenamemap)                                             #

        xaxis = []                                                              # Adjust scale of grid here
        i = -71.189
        while i < -70.878:
            xaxis.append(i)
            i += 0.01

        yaxis = []                                                              # Adjust scale of grid here
        i = 42.234
        while i < 42.406:
            yaxis.append(i)
            i += 0.01

        for asset in assets:                    # This loop finds the cell that the asset belongs to and correspondingly
            y = asset['Location'][1]            # ...increases the count of that asset type in the dictionary
            x = asset['Location'][0]            # ...representation
            typekind = asset['Type']
            ycell = display.findcell(y, yaxis)
            xcell = display.findcell(x, xaxis)
            if (ycell, xcell) in grid:      # O(1) lookup time. Hire me, Google
                # grid[(ycell, xcell)] += 1     # for overall counts
                if typekind == "charge":
                    grid[(ycell, xcell)][0][0] += 1
                elif typekind == "hubway":
                    grid[(ycell, xcell)][1][0] += 1
                elif typekind == "openspace":
                    grid[(ycell, xcell)][2][0] += 1

        for coords, counts in grid.items():     # Gonna save to database and display on map
            megalist.append({"coordinates": coords, "charge_count": counts[0][0], "hubway_count": counts[1][0],
                             "open_count": counts[2][0]})
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
        doc.add_namespace('ab', 'https://data.boston.gov/dataset/boston-neighborhoods')   # Analyze Boston

        this_script = doc.agent('alg:jhs2018_rpm1995#Combine_Coordinates',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

# #######
        resource_greenassets = doc.entity('dat:jhs2018_rpm1995_greenassets',
                                  {prov.model.PROV_LABEL: 'Coordinates of Environment Friendly Assets in Boston',
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
