import flask
from flask import Flask, Response, request, render_template, redirect, url_for
from flask_pymongo import PyMongo

from alyu_sharontj_yuxiao_yzhang11.mapping import Mapping



import json
import sys
sys.path.append('..')
import folium
print (folium.__file__)
print (folium.__version__)
from branca.colormap import linear
import dml




app = Flask(__name__)
app.config['MONGO_DBNAME']='repo'
mongo = PyMongo(app)





@app.route("/", methods=['GET'])
def welcome():
    return render_template('welcome.html')


@app.route('/generate')
def renderMap():

    Mapping("rent")

    return render_template('generate.html')

@app.route("/choose",methods=['GET'])
def choose():

    return render_template('choose.html')

@app.route("/made_choice", methods = ["GET", "POST"] )
def made_choose():
    formData = request.values if request.method == "GET" else request.values
    #print(formData)
   # print(type(formData))
    choice = [item for item in formData.items()][0][1]

    #print("choice is ", choice, type(choice))
    Mapping(choice)
    return render_template('generate.html')






if __name__ == "__main__":
    app.run(port=5000, debug=True)
