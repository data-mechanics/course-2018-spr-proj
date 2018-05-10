from flask import Flask, render_template, request, redirect
import json
import jsonschema
from jsonschema import validate
import geocoder
from math import *
from pymongo import MongoClient


client = MongoClient()
repo = client.repo
repo.authenticate('pandreah', 'pandreah')

s = repo['pandreah.hubwayStations']
stations = list(s.find())




app = Flask(__name__)

schema = {
    "type" : "object",
    "properties" : {
        "houseNumber" : {"type" : "string"},
        "streetName" : {"type" : "string"},
        "city" : {"type" : "string"},
        "state" : {"type" : "string"},
    },
}


@app.route('/')
def index():
    '''This will render the first page that the user sees.'''
    return render_template('index.html')

@app.route('/checkAddress', methods= ['POST', 'GET'])
def checkAddress():
    address = []
    n = request.form['Number']
    st = request.form['Street']
    cit = request.form['City']
    sta = request.form['State']
    address.append({ "houseNumber" : n, "streetName" : st, "city" : cit, "state" : sta})
    print(address)
    # try:
    #     validate(address, schema)
    # except jsonschema.exceptions.ValidationError:
    #     print("This is not a valid address")

    a = address[0]["houseNumber"] + " " + address[0]["streetName"] + " " +  address[0]["city"] + " " +  address[0]["state"]
    g = geocoder.arcgis(a)
    print(g.json)
    geos = g.json
    cord = [round(geos["lat"],8), round(geos["lng"], 8)]

    counting_hubs = 0  # counter for number of Hubways around
    counting_ring = 0

    for station in stations:
        center_lat = float(station["Latitude"])
        center_lng = float(station["Longitude"])

        distanceK = 6371 * acos(
            cos(radians(90 - center_lat)) * cos(radians(90 - float(cord[0]))) + sin(radians(90 - center_lat)) * sin(
                radians(90 - float(cord[0]))) * cos(radians(center_lng - float(cord[1]))))

        if distanceK <= 3:
            counting_ring += 1
            if distanceK <= 1:
                counting_hubs += 1
    if counting_hubs != 0:
        return render_template("index.html", address=a, coords=cord, hubs= counting_hubs)
    elif counting_ring != 0:
        return render_template("index.html", address=a, coords=cord, ring= counting_ring)
    else:
        return render_template("index.html", address=a, coords=cord, message="Sorry, this address isn't near a Hubway Station nor is it in the Hubway Extension Area.")









if __name__ == '__main__':
    app.run()
