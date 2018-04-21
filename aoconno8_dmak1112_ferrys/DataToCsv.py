import pandas as pd
import json
import math
import urllib.request


url = 'http://datamechanics.io/data/aoconno8_dmak1112_ferrys/optimization.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
d = json.loads(response)


optlist = []
otherlist = []
for i in range(len(d)):
    score = d[i]['optimal_route']['score']
    dist = d[i]['optimal_route']['route_dist']
    if d[i]['optimal_route']['route_dist'] > 8000:
        continue
    if not math.isnan(d[i]['optimal_route']['score']):
        optlist.append(("OPT", score, dist))
    for i in d[i]['other_routes']:
        if i['route_dist'] > 8000:
            continue
        if not math.isnan(i['score']):
            otherlist.append(("OTHER", i['score'], i['route_dist']))
otherlist = sorted(otherlist, key=lambda x: x[1])
optlist = sorted(optlist, key=lambda x: x[1])





full_list = [] 
for i in range(len(optlist)):
    symbol = optlist[i][0]
    score = optlist[i][1]
    dist = optlist[i][2]
    full_list.append([symbol, score, dist])

for i in range(len(otherlist)):
    symbol = otherlist[i][0]
    score = otherlist[i][1]
    dist = otherlist[i][2]
    full_list.append([symbol, score, dist])


df = pd.DataFrame(full_list, columns=['symbol','date','price'])
df.to_csv("data.csv",index=False)
print("Done")

