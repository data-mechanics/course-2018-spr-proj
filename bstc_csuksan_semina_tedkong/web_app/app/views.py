import json
from app import app
from flask import render_template
from flask import request

@app.route('/', methods=['GET','POST'])
def index():
    return render_template('index.html', title='Restaurant Recommender')

# example of passing data to route and sending data back
@app.route('/generate/<phone_num>', methods=['GET','POST'])
def getpayload(phone_num):

    payload = {
        'phone_num': phone_num
    }

    return json.dumps(payload)

#receive data from template (note that location is only available once browser successfully locates user)
@app.route('/receivedata', methods=['POST'])
def receive_data():
	print(request.form['yelp'])
	print(request.form['violation'])
	print(request.form['location'])
	return render_template('index.html', title='Restaurant Recommender')
'''
@socketio.on_error()        # Handles the default namespace
def error_handler(e):
    pass

@socketio.on_error('/chat') # handles the '/chat' namespace
def error_handler_chat(e):
    pass

@socketio.on_error_default  # handles all namespaces without an explicit error handler
def default_error_handler(e):
    pass

'''
