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

@app.route("/index3", methods=["GET","POST"])
def testPage():
	if request.method == "GET":
		return render_template("index3.html")

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

@app.route("/schoolSubwayStation", methods=["GET","POST"])
def schoolSubwayStation():
	if(request.method == 'POST'):
		schoolNameInput = request.form.get('schoolNameInput')
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
		schoolHubwayList = []
		schoolHubwayList = repo['debhe_shizhan0_wangdayu_xt.schoolSubwayDistance'].find()
		find = False
		for row in schoolHubwayList:
			if row['schoolName'] == schoolNameInput:
				schoolName = row['schoolName']
				subwayStationName = row['subwayStation']
				schoolCorX = row['schoolX']
				schoolCorY = row['schoolY']
				subwayCorX = row['subwayX']
				subwayCorY = row['subwayY']
				print(subwayCorY)
				find = True
		repo.logout()
		if(find == True):
			return render_template("schoolSubwayStation.html", schoolName = schoolName, subwayStationName = subwayStationName, school_x=(schoolCorX), school_y=schoolCorY, subway_x=subwayCorX, subway_y=subwayCorY)
		else:
			return render_template("schoolSubwayStation.html", message = "School Not Found")
	return render_template("schoolSubwayStation.html")

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

@app.route('/OnClickHubway/<schooName>', methods = ['GET'])
def onClickHubway(schoolName):
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
	schoolHubwayList = []
	schoolHubwayList = repo['debhe_shizhan0_wangdayu_xt.schoolHubwayDistance'].find()
	for row in schoolHubwayList:
		if row['schoolName'] == schoolNameInput:
			hubwayStationName = row['hubwayStation']
			schoolCorX = row['School_Cor_x']
			schoolCorY = row['School_Cor_y']
			hubwayCorX = row['Hubway_Cor_x']
			hubwayCorY = row['Hubway_Cor_y']
	repo.logout()
	return render_template("OnClickHubway.html", schoolName = schoolName, hubwayStationName = hubwayStationName, school_x=schoolCorX, school_y=schoolCorY, hubway_x=hubwayCorX, hubway_y=hubwayCorY)
		


@app.route("/showTwoResult", methods=["GET","POST"])
def showTwoResult():
	if(request.method == 'POST'):
		schoolNameInput = request.form.get('schoolNameInput')
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
		newStationList = repo['debhe_shizhan0_wangdayu_xt.newSchoolSubDis'].find()
		for row in newStationList:
			if row['schoolName'] == schoolNameInput:
				subwayName = row['subwayStation']
				schoolCorX = row['schoolX']
				schoolCorY = row['schoolY']
				subwayCorX = findSubwayX(subwayName)
				subwayCorY = findSubwayY(subwayName)
		oldStationList = repo['debhe_shizhan0_wangdayu_xt.schoolHubwayDistance'].find()
		for row in oldStationList:
			if row['schoolName'] == schoolNameInput:
				hubwayCorX = row['Hubway_Cor_x']
				hubwayCorY = row['Hubway_Cor_y']
				hubwayStationName = row['hubwayStation']
		schoolName = schoolNameInput
		repo.logout()
		return render_template("showTwoResult.html", schoolName = schoolName, subwayStationName = subwayName, school_x = schoolCorX, school_y = schoolCorY, subway_x= subwayCorX, subway_y = subwayCorY,hubwayStationName = hubwayStationName, hubway_x=hubwayCorX, hubway_y=hubwayCorY )
	return render_template("showTwoResult.html")


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
	repo.logout()
	return render_template("checkAllSchools.html", schools = returnList)

@app.route("/checkAllHubway", methods=["GET"])
def checkAllHubway():
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
	hubwayList = repo['debhe_shizhan0_wangdayu_xt.hubwayStation'].find()
	hubwayResultList = []
	for row in hubwayList:
		h_n = row['station']
		h_x = row['X']
		h_y = row['Y']
		hubwayResultList.append([h_n, h_x, h_y])
	repo.logout()
	return render_template("checkAllHubway.html", hubways = hubwayResultList)

@app.route("/allSchoolAndHubway", methods=["GET"])
def allSchoolAndHubway():
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
	hubwayList = repo['debhe_shizhan0_wangdayu_xt.hubwayStation'].find()
	hubwayResultList = []
	for row in hubwayList:
		h_n = row['station']
		h_x = row['X']
		h_y = row['Y']
		hubwayResultList.append([h_n, h_x, h_y])
	schoolList = repo['debhe_shizhan0_wangdayu_xt.schoolHubwayDistance'].find()
	returnList = []
	for row in schoolList:
		s_n = row['schoolName']
		s_x = row['School_Cor_x']
		s_y = row['School_Cor_y']
		returnList.append([s_n, s_x, s_y])
	repo.logout()
	return render_template("allSchoolAndHubway.html", hubways = hubwayResultList, schools = returnList)

@app.route("/schoolAllHubway", methods=["GET, POST"])
def schoolAllHubway():
	hubwayList = findAllHubway()
	if(request.method == "POST"):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
		schoolList = repo['debhe_shizhan0_wangdayu_xt.schoolHubwayDistance'].find()
		for row in schoolList :
			if(row['schoolName'] == schoolNameInput):
				s_x = row['School_Cor_x']
				s_y = row['School_Cor_y']
		repo.logout()
		return render_template("schoolAllHubway.html", schoolName = schoolNameInput, school_x = s_x, school_y = s_y, hubways = hubwayList)
	repo.logout()
	return render_template("schoolAllHubway.html", hubways = hubwayList)

if __name__ == '__main__':
    app.run(debug=True)