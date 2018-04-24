import urllib.request
import json
import dml
from flask import Flask, jsonify, abort, make_response, request, render_template
from flask_pymongo import PyMongo

app = Flask(__name__)
#app.config['MONGO_DBNAME'] = 'repo'
#mongo = PyMongo(app)

#app.secret_key='debhe_shizhan0_wangdayu_xt'

@app.route("/", methods=["GET","POST"])
def mainPage():
	if request.method == "GET":
		return render_template("index.html")

@app.route("/schoolHubwayStation", methods=["GET","POST"])
def schoolHubwayStation():
	if(request.method == 'POST'):
		schoolNameInput = request.form.get('schoolNameInput')
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
		schoolHubwayList = []
		schoolHubwayList = repo['debhe_shizhan0_wangdayu_xt.schoolHubwayDistance'].find()
	#schoolHubwayList = list(mongo.db.debhe_shizhan0_wangdayu_xt.schoolHubwayDistance.find())
		for row in schoolHubwayList:
			if row['schoolName'] == schoolNameInput:
				schoolName = row['schoolName']
				hubwayStationName = row['hubwayStation']
				schoolCorX = row['School_Cor_x']
				schoolCorY = row['School_Cor_y']
				hubwayCorX = row['Hubway_Cor_x']
				hubwayCorY = row['Hubway_Cor_y']
		repo.logout()
		return render_template("schoolHubwayStation.html", schoolName = schoolName, hubwayStationName = hubwayStationName, school_x=schoolCorX, school_y=schoolCorY, hubway_x=hubwayCorX, hubway_y=hubwayCorY)
	return render_template("schoolHubwayStation.html")

if __name__ == '__main__':
    app.run(debug=True)