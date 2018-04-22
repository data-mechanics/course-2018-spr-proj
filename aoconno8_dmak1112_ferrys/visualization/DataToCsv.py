import pandas as pd
import json
import math
import urllib.request


url = 'http://datamechanics.io/data/aoconno8_dmak1112_ferrys/optimization.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
d = json.loads(response)


otherlist = []
for i in range(len(d)):
    score = d[i]['optimal_route']['score']
    dist = d[i]['optimal_route']['route_dist']
    if d[i]['optimal_route']['route_dist'] > 8000:
        continue
    if d[i]['optimal_route']['type'] == 'shortest':
        symbol = 'SHORT'
    else:
        symbol = 'SAFE'
    if not math.isnan(d[i]['optimal_route']['score']):
        otherlist.append((symbol, score, dist))
    for i in d[i]['other_routes']:
        if i['type'] == 'safest':
            symbol = 'SAFE'
        else:
            symbol = 'SHORT'
        if i['route_dist'] > 8000:
                continue
        if not math.isnan(i['score']):
            otherlist.append((symbol, i['score'], i['route_dist']))
otherlist = sorted(otherlist, key=lambda x: x[1])





full_list = [] 


for i in range(len(otherlist)):
    symbol = otherlist[i][0]
    score = otherlist[i][1]
    dist = otherlist[i][2]
    full_list.append([symbol, score, dist])


df = pd.DataFrame(full_list, columns=['symbol','date','price'])
df.to_csv("app/templates/data.csv",index=False)
print("Done")

