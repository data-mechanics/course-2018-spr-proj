from flask import Flask
import io
import numpy as np
import matplotlib.pyplot as plt
import dml
import ast
import copy
from math import *
from haversine import haversine
from flask import *
import base64

app = Flask(__name__)

@app.route('/', methods=['GET','POST'])
def hello_world():
    if request.method == 'POST':
        skip = float(request.form['skip'].strip())
        bins = int(request.form['bins'].strip())
    else:
        # possible ranges
        skip = 100
        bins = 16

    ranges = []

    for i in range(bins):
        ranges.append([round((i+1)*skip, 2), 0])

    UTILS = ['mbta_stop', 'pool', 'open_space']
    # generate the intial dictionary
    distance_vs_frequency = {}
    for util in UTILS:
        # NOTE: ranges is a template, so a deep copy must be stored here
        distance_vs_frequency[util] = copy.deepcopy(ranges)

    count = 0
    with open('min_dists.csv','r') as f:
        next(f)
        for line in f:
            arr = line.strip().split(',')
            #if count > 10000:
            #    break
            for i in [0,1,2]:
                index = int(float(arr[i])*1000//skip)
                if index >= bins:
                    index = -1
                if i == 0:
                    distance_vs_frequency['mbta_stop'][index][1] += 1
                elif i == 1:
                    distance_vs_frequency['pool'][index][1] += 1
                elif i == 2:
                    distance_vs_frequency['open_space'][index][1] += 1
                else:
                    print('error!')
                    return -1
            count += 1
    # store the plot urls
    plot_urls = []

    # generate statistics
    for util in UTILS:
        x_dist = []
        y_freq = []

        # reformat data
        for points in distance_vs_frequency[util]:
            if points[1] > 0:
                x_dist.append(points[0])
                y_freq.append(points[1])

        m, b = np.polyfit(x_dist, y_freq, 1)

        img = io.BytesIO()

        plt.title("Distance from nearest " + util.replace('_',' ').title() + " vs Crime Increase")
        plt.xlabel("Distance from nearest " + util + " (in m)")
        plt.ylabel("Non-Cumulative Increase in Number of Crimes")
        plt.plot(x_dist, y_freq, '.')
        plt.plot(np.unique(x_dist), np.poly1d(np.polyfit(x_dist, y_freq, 1))(np.unique(x_dist)))
        plt.ylim(ymin=0)
        plt.savefig(img, format='png')
        img.seek(0)

        plot_urls.append(base64.b64encode(img.getvalue()).decode())
        plt.clf()
    return render_template('index.html', skip=skip, bins=bins, image=plot_urls)
if __name__ == '__main__':
    app.run(debug=True)