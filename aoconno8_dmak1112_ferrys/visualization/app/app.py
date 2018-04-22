from flask import Flask, jsonify, abort, make_response, request, render_template

app = Flask(__name__)


@app.route("/")
def main():
    return render_template('index.html', map=True, graph=False)

@app.route('/map')
def get_map():
    return render_template('viz.html')

@app.route('/graph')
def get_graph():
    return render_template('viz2.html')

@app.route('/show_graph')
def show_map():
	return render_template('index.html', map=False, graph=True)


if __name__ == '__main__':
    app.run(debug=True)
        