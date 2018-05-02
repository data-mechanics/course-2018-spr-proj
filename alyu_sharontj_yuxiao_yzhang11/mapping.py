import json
import sys
sys.path.append('..')
import folium
print (folium.__file__)
print (folium.__version__)
from branca.colormap import linear
import dml
import statistics as stats

def get_boundary(info):
        value_list = list(info.values())
        mean = stats.mean(value_list)
        # print(str(mean))
        std = stats.stdev(value_list)
        # print(str(std))
        low = mean-3*std
        high = mean + 3*std
        return low, high

def normalize(value, low, high):
    return float((value-low)/(high-low))

def Mapping(key):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')
    dbresults = repo['alyu_sharontj_yuxiao_yzhang11.Result'].find()



    result = []

    for info in dbresults:
        zipcode = info['Zipcode']
        score = info[key]
        result.append((zipcode, score))

    low, high = get_boundary(dict(result))
    normresult = []
    for k,v in result:
        normV = normalize(v,low, high) * 100
        normresult.append((k,normV))


    sortresult = sorted(normresult, key=lambda x: x[1], reverse=True)
    dictR = dict(sortresult)
    print(dictR)


    # import geo json data
    geo_json_data = json.load(open('ZIP_Codes.geojson'))

    if key == 'rent':
        colormap = linear.GnBu.scale(
            sortresult[-1][1],
            sortresult[0][1]
        )
    elif key == 'transportation':
        colormap = linear.YlGn.scale(
            sortresult[-1][1],
            sortresult[0][1]
        )
    elif key == 'safety':
        colormap = linear.PuRd.scale(
            sortresult[-1][1],
            sortresult[0][1]
        )
    elif key == 'facility':
        colormap = linear.BuPu.scale(
            sortresult[-1][1],
            sortresult[0][1]
        )
    else :
        colormap = linear.YlOrRd.scale(
            sortresult[-1][1],
            sortresult[0][1]
        )


    # create map, center it on Boston
    m = folium.Map([42.324725, -71.093327], tiles='CartoDB Positron', zoom_start=12)


    def styleFunc(feature):
        x = dictR.get(feature['properties']['ZIP5'],None)
        return {
            'fillColor': '#d3d3d3' if x is None else colormap(x),
            'color': 'black',
            'weight': 1,
            'dashArray': '5, 5',
            'fillOpacity': .9,
        }

    # apply the Boston zipcode areas outlines to the map
    folium.GeoJson(geo_json_data,
                  style_function= styleFunc
                  ).add_to(m)

    # display map
    m.save("templates/heatafter.html")



if __name__ == "__main__":
    Mapping("rent")
