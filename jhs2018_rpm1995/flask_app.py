import jsonschema
from flask import Flask, jsonify, abort, make_response, request, render_template, redirect, url_for
#from flask_httpauth import HTTPBasicAuth
import requests
from flask_pymongo import PyMongo
import dml
from displayforflask import display
from kmeansforflask import kmeans_correlation

app = Flask(__name__)

app.config['MONGO_DBNAME'] = 'repo'
app.config['MONGO_USERNAME'] = 'jhs2018_rpm1995'
app.config['MONGO_PASSWORD'] = 'jhs2018_rpm1995'

mongo = PyMongo(app, config_prefix='MONGO')

#auth = HTTPBasicAuth()

@app.route('/', methods = ['GET', 'POST'])
def index():
    if request.method == 'POST':
        if request.form['submit'] == "Open Space":
            return render_template('index.html' , kmeansmap = 'openClust')
        elif request.form['submit'] == "Crime":
            return render_template('index.html', kmeansmap = 'crimeClust')
        elif request.form['submit'] == "Hubway stations":
            return render_template('index.html', kmeansmap = 'hubwayClust')
        elif request.form['submit'] == "Charging stations":
            return render_template('index.html', kmeansmap = 'chargeClust')
    return render_template('index.html', kmeansmap = 'crimeClust')

@app.route('/crimeClust.html')
def crimeClust():
    return render_template('crimeClust.html')

@app.route('/openClust.html')
def openClust():
    return render_template('openClust.html')

@app.route('/hubwayClust.html')
def hubwayClust():
    return render_template('hubwayClust.html')

@app.route('/chargeClust.html')
def chargeClust():
    return render_template('chargeClust.html')

@app.route('/scale', methods=['GET','POST'])
def scale():
    if request.method == 'POST':
        stuff = float(request.form['scale'])
        dis = display(stuff)
        kmeans = kmeans_correlation()
        return redirect(url_for('index'))
    else:
        return render_template('scale.html')

@app.route('/changek', methods=['GET','POST'])
def changek():
    if request.method == 'POST':
        stuff = int(request.form['k'])
        kmeans = kmeans_correlation(k = stuff)
        return redirect(url_for('index'))
    else:
        return render_template('changek.html')

if __name__ == "__main__":
    app.run(debug = True)