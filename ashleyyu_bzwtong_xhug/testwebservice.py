import jsonschema
from flask import Flask, jsonify, abort, make_response, request
from flask.ext.httpauth import HTTPBasicAuth

app = Flask(__name__)
auth = HTTPBasicAuth()

users = [
	{'id': 1, 'username': u'xhug'},
	{'id': 2, 'username': u'ashleyyu'},
	{'id': 3, 'username': u'bzwtong'}
]

schema = {
	"type": "object",
	"properties": {"username": {"type": "string"}},
	"required" : ["username"]
}

@app.route('/client', methods=['OPTIONS'])
def show_api():
	return jsonify(schema)

@app.route('/client', methods=['GET'])
@auth.login_required
def show_client():
	return open('client.html', 'r').read()

@app.route('/app/api/v0.1/users', methods=['GET'])
def get_users(): # Server-side reusable name for function.
	print("I'm responding.")
	return jsonify({'users': users})

@app.route('/app/api/v0.1/users/', methods=['GET'])
def get_user(user_id):
	user = [user for user in users if user['id'] == user_id]
	if len(user) == 0:
		abort(404)
	return jsonify({'user': user[0]})

@app.errorhandler(404)
def not_found(error):
	return make_response(jsonify({'error': 'Not found foo.'}), 404)

@app.route('/app/api/v0.1/users', methods=['POST'])
def create_user():
	print(request.json)
	if not request.json:
		print('Request not valid JSON.')
		abort(400)

	try:
		jsonschema.validate(request.json, schema)
		user = { 'id': users[-1]['id']+1, 'userename': request.json['username']}










