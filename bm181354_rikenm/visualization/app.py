from flask import Flask
from flask import render_template, jsonify


import requests

import pandas as pd

import pymongo
import json

from bson import json_util
from bson.json_util import dumps

app = Flask(__name__)

# TODO:- Change this
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017


# field from mongo [see and change this]
# remove '_id'
FIELDS = {'Latitude_normalized': True, 'Longitude_normalized': True, 'Popularity': True, 'Y_label': True,'_id' : False}

@app.route("/")
def index():
    return render_template("index.html")


#MARK: - Sending JSON data to the request [Make another one as well]
@app.route("/hubway/projects", methods =['GET'])
def choose_projects():
    # good
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('bm181354_rikenm', 'bm181354_rikenm')
    collection = repo['bm181354_rikenm.solutionLeastPopularStationsdb']
    #.find({})

    # filter out unncessary data
    projects = collection.find(projection=FIELDS)
    
    json_projects = []
    for project in projects:
         json_projects.append(project)
    json_projects = json.dumps(json_projects, default=json_util.default)
    client.close()
    return (json_projects)

# Map data
@app.route("/hubway/boston", methods =['GET'])
def boston_map():
    return open("static/geojson/boston.json",'r').read()

# slider gets this data
@app.route("/cluster/<int:number>", methods =['GET'])
def cluster_data(number):
    return str(number)

# load compute data
@app.route("/hubway/compute", methods =['GET'])
def compute_data():
    return open("compute_data.json",'r').read()

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000,debug=True)
