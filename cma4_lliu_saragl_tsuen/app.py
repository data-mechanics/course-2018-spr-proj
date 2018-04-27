import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import sin, cos, sqrt, atan2, radians
import sys
from flask import Flask, render_template

app = Flask(__name__)

@app.route("/")
def hello():
	return render_template('index.html', data = execute(False))

#Retrieves Data from Find_closest
def execute(trial = False):
    '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
    startTime = datetime.datetime.now()


    
    # Set up the database connection.
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('cma4_lliu_saragl_tsuen', 'cma4_lliu_saragl_tsuen')

    destinations = None

    
    destinations = repo['cma4_lliu_saragl_tsuen.closest'].aggregate([{'$sample': {'size': 1000}}], allowDiskUse=True)
    
    
    data = []

    for i in destinations:
    	data.append(i)

    print(len(data))


    repo.logout()

    endTime = datetime.datetime.now()

    return data



if __name__ == '__main__':
	app.debug = True
	app.run()

## eof
