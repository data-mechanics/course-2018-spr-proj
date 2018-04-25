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
		find = False
	#schoolHubwayList = list(mongo.db.debhe_shizhan0_wangdayu_xt.schoolHubwayDistance.find())
		for row in schoolHubwayList:
			if row['schoolName'] == schoolNameInput:
				schoolName = row['schoolName']
				hubwayStationName = row['hubwayStation']
				schoolCorX = row['School_Cor_x']
				schoolCorY = row['School_Cor_y']
				hubwayCorX = row['Hubway_Cor_x']
				hubwayCorY = row['Hubway_Cor_y']
				find = True
		repo.logout()
		if(find == True):
			return render_template("schoolHubwayStation.html", schoolName = schoolName, hubwayStationName = hubwayStationName, school_x=schoolCorX, school_y=schoolCorY, hubway_x=hubwayCorX, hubway_y=hubwayCorY)
		else:
			return render_template("schoolHubwayStation.html", message = "School Not Found")
	return render_template("schoolHubwayStation.html")

@app.route("/schoolNewBikeHub", methods=["GET","POST"])
def schoolNewBikeHub():
	if(request.method == 'POST'):
		schoolNameInput = request.form.get('schoolNameInput')
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
		schoolList = []
		schoolList = repo['debhe_shizhan0_wangdayu_xt.newSchoolSubDis'].find()
		for row in schoolList:
			if row['schoolName'] == schoolNameInput:
				schoolName = row['schoolName']
				subwayName = row['subwayStation']
				schoolCorX = row['schoolX']
				schoolCorY = row['schoolY']
				subwayCorX = findSubwayX(subwayName)
				subwayCorY = findSubwayY(subwayName)
		repo.logout()
		return render_template("schoolNewBikeHub.html", schoolName = schoolName, subwayStationName = subwayName, school_x=schoolCorX, school_y=schoolCorY, subway_x=subwayCorX, subway_y=subwayCorY)
	return render_template("schoolNewBikeHub.html")

def findSubwayX(Name):
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
	subwayList = repo['debhe_shizhan0_wangdayu_xt.subwayStop'].find()
	for row in subwayList:
		if( row['stopName'] == Name ):
			repo.logout()
			return row['X']
	repo.logout()
	return "Not Found"

def findSubwayY(Name):
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
	subwayList = repo['debhe_shizhan0_wangdayu_xt.subwayStop'].find()
	for row in subwayList:
		if( row['stopName'] == Name ):
			repo.logout()
			return row['Y']
	repo.logout()
	return "Not Found"

@app.route("/checkAllSchools", methods=["GET"])
def checkAllSchools():
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
	schoolList = []
	schoolList = repo['debhe_shizhan0_wangdayu_xt.schoolHubwayDistance'].find()
	returnList = []
	for row in schoolList:
		s_n = row['schoolName']
		s_x = row['School_Cor_x']
		s_y = row['School_Cor_y']
		returnList.append([s_n, s_x, s_y])
	return render_template("checkAllSchools.html", schools = returnList)


if __name__ == '__main__':
    app.run(debug=True)