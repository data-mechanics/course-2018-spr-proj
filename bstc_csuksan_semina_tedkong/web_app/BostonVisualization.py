# -*- coding: utf-8 -*-
"""
Created on Sun Apr 22 00:28:19 2018

@author: Alexander
"""

import pandas as pd
import numpy as np
import json
import urllib2
import matplotlib.pyplot as plt
import random
import requests


urls = 'http://datamechanics.io/data/BostonScoring_Map.json'
with requests.get(urls) as url:
    data = url.text
    data = json.dumps(data)
temp = json.loads(data)
file = pd.read_json(temp, lines=True)
#print(file)

# TEMPORARY INPUT
new = {"latitude":42.3777464032,"longitude":-71.0518522561,"name":"149 Eat Street","rate":0.4698795181,"yelp":0.5}
check = [-99999999, new['latitude'],new['longitude']]
data = np.array(file[['name','latitude','longitude', 'rate', 'yelp']])

ret = []
rate_level = 1
yelp_level = 1
yelp_level = random.randint(yelp_level * 2, 10) / 2
rate_level = random.uniform(1, 3 - (rate_level - 1))
check += [rate_level, yelp_level]

for ind,val in enumerate(data):
    x = (val[1] - check[1])**2
    y = (val[2] - check[2])**2
    x2 = ((val[3] - check[3])**2)
    y2 = ((val[4] - check[4])**2)
    distance1 = np.sqrt(x+y)
    distance2 = np.sqrt(x2+y2)
    distance = ((distance1 * 20) + distance2)/21
    if len(ret) < 3:
        ret += [(ind, distance)]
    else:
        for i,v in enumerate(ret):
            if v[1] > distance:
                ret[i] = (ind, distance)
                break

local = []
for i in ret:
    local += [data[i[0]]]

local = np.array(local)
    
print(local)

plt.plot(data[:,1],data[:,2], 'gx')
plt.plot(check[1],check[2], 'rx')
plt.plot(local[:,1],local[:,2], 'bx')


