from flask import Flask
from flask import render_template, jsonify
import pymongo
import json

from bson import json_util
from bson.json_util import dumps

app = Flask(__name__)

# TODO:- Change this
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017

DBS_NAME = 'donorschoose'
COLLECTION_NAME = 'projects'

# field from mongo [see and change this]
# remove '_id'
FIELDS = {'Latitude_normalized': True, 'Longitude_normalized': True, 'Popularity': True, 'Y_label': True,'_id' : False}

@app.route("/")
def index():
    return render_template("index.html")


#MARK: - Sending JSON data to the request [Make another one as well]
@app.route("/hubway/projects", methods =['GET'])
def donorschoose_projects():
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


@app.route("/hubway/boston", methods =['GET'])
def boston_map():
    file = open("static/geojson/boston.json")

    ls = []
    for line in file:
        ls.append(line)
    value = json.dumps(ls,default=json_util.default)
    return value



if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000,debug=True)
