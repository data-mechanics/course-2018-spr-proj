from flask import Flask, render_template, request

import optCrime
import json

app = Flask(__name__)


@app.route('/')
def main():
    return render_template('index.html')

@app.route('/optimize', methods=["GET","POST"])
def optimize():
    if request.method == "POST":
        distance=float(request.form['distance'])
        district=request.form.get('district')
        #results={}
        obj = optCrime.optCrime()
        results=obj.execute(d_input=district, distance_input=distance)
        total=results[district]['total']
        return render_template('results.html', total=total, results=results,district=district, distance=distance)
    else:
        return render_template('index.html')


if __name__ == "__main__":
    app.run()