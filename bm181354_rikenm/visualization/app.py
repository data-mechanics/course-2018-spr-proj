from flask import Flask
from flask import render_template
from pymongo import MongoClient
import json

import dml

from bson import json_util
from bson.json_util import dumps

app = Flask(__name__)

# TODO:- Change this
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017

DBS_NAME = 'donorschoose'
COLLECTION_NAME = 'projects'

# field from mongo [see and change this]
FIELDS = {'school_state': True, 'resource_type': True, 'poverty_level': True, 'date_posted': True, 'total_donations': True, '_id': False}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/hubway/projects")
def donorschoose_projects():
    
    # good
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('bm181354_rikenm', 'bm181354_rikenm')
    #connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
    collection = repo['bm181354_rikenm.stat_analysis']
    #collection = connection[DBS_NAME][COLLECTION_NAME]
    
    # filter out unncessary data
    projects = collection.find(projection=FIELDS)
    #
    
    json_projects = []
    for project in projects:
        json_projects.append(project)
    json_projects = json.dumps(json_projects, default=json_util.default)
    connection.close()
    return json_projects

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000,debug=True)
