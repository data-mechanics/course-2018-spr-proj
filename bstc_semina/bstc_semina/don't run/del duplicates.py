# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 01:08:04 2018

@author: Alexander
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('bstc_semina', 'bstc_semina')

docs = repo.bstc_semina.getNYCRestaurantData.find({})

found = []
deleted = 0
for i in docs:
    #print(i)
    if "license_permit_holder" not in i:
        repo.bstc_semina.getNYCRestaurantData.delete_one(i)
        deleted+=1
    elif i["license_permit_holder"] in found:
        repo.bstc_semina.getNYCRestaurantData.delete_one(i)
        deleted+=1
    else:
        found += i["license_permit_holder"]

