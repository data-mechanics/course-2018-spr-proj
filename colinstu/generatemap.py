from flask import Flask, request, url_for, render_template
import dml


app = Flask(__name__)

# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('colinstu', 'colinstu')
data = repo.colinstu.combineneighborhoodpoverty


@app.route('/', methods=['GET'])
def get_data():
    try:
        povertydata = repo.colinstu.combineneighborhoodpoverty
        povertyoutput = {
            "type": "FeatureCollection", "features": []}
        for s in povertydata.find():
            if s['percent_impoverished'] == 'N/a':
                pov = 0
            else:
                pov = float(s['percent_impoverished'][:-1])
            povertyoutput['features'].append({'type': 'Feature', 'properties': {'city': s['city'],
                                                                                'poverty_rate': s['poverty_rate'],
                                                                                'percent_impoverished': pov,
                                                                                'population': s['population']},
                                              'geometry': s['geometry']
                                              })

        kmeansdata = repo.colinstu.kmeans
        kmeansoutput = {
            "type": "FeatureCollection", "features": []}
        x = 0
        for s in kmeansdata.find():
            x += 1
            kmeansoutput['features'].append({'type': 'Feature',
                                             'properties': {'point': str(x)},
                                             'geometry': {"type": "Point",
                                                          "coordinates": s['closest centroid']
                                                          }})
        print('got data')
    except:
        print("error")

    return render_template('foodavailability.html', povertyoutput=povertyoutput, kmeansdata=kmeansoutput)


if __name__ == '__main__':
    app.run(debug=True)
