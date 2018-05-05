from tqdm import tqdm
import dml
import json
import jsonschema
from flask import Flask, jsonify, abort, make_response, request, send_from_directory
import pymongo

def parse_eviction_year(date):
  '''
  Get date in format 'm/d/yyyy' and return just the year
  '''
  if(type(date) is float or "/" not in date):
    return -1
  date_arr = date.split("/")
  if(len(date_arr) < 3):
    return -1
  if(len(date_arr[2]) != 2):
    return -1
  final = int(date_arr[2])
  if(final > 17 or final < 13 ): 
    return -1
  return final

def get_evictions_from_database():
  client = dml.pymongo.MongoClient()
  repo = client.repo
  repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
  evictions_collection = repo['agoncharova_lmckone.boston_evictions']
  evictions_cursor = evictions_collection.find()
  evictions_arr = []
  years = []
  count = 0
  for eviction in evictions_cursor:
    count += 1
    year = parse_eviction_year(eviction["date"])
    if(year == -1): # don't save this data point if no year
      continue
    years.append(year)
    formatted_eviction = {
      "type": "Feature", 
      "id": eviction["id"],
      "properties": {
        "date" : eviction["date"],
        "year" : year,
        "address": eviction["address"],
        "latitude": eviction["latitude"],
        "longitude": eviction["longitude"],        
      },
      "geometry": {
        "type": "Point", 
        "coordinates": [eviction["longitude"], eviction["latitude"]]
      }
    }
    evictions_arr.append(formatted_eviction)
  # format it in the valid format to serve to the map
  repo.logout()
  print("Formatted " + str(count) + " evictions")
  return evictions_arr

def generate_evictions_json_file():
  '''
  Writes formatted evictions to the file
  '''
  print("about to write evictions to a json file")
  evictions_file = open("./website/data/evictions.json","w")
  evictions_file.write('var evictions_json = {')
  evictions_file.write('"type": "FeatureCollection",')
  evictions_file.write('"features":')

  evictions = get_evictions_from_database()
  evictions_file.write(json.dumps(evictions))
  evictions_file.write("}")

def generate_correlations_json_file():
  '''
  Gets the correlation data from the db and 
  serves to the frontend for the correlations d3 graph
  Display data for all of the correlations, but 
  dynamically change depending on income level
  '''
  client = dml.pymongo.MongoClient()
  repo = client.repo
  repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
  correlations_collection = repo['agoncharova_lmckone.correlations']
  correlations_cursor = correlations_collection.find()
  formatted_data = {}
  # data points format
  # {"title":"","subtitle":"","ranges":[-1, 1],"measures":[corr, 1],"markers":[corr]}
  keys = ['businesses', 'evictions']
  for corr in correlations_cursor:
    for key in corr:
      if key in keys:
        for value in corr[key]:
          for income_level in corr[key][value]:
            correlation = corr[key][value][income_level]
            upp_end = 1
            if(correlation < 0):
              upp_end = -1
            d = { "title" : key.title(),
                  "subtitle": value.title(),
                  "ranges" : [-1, 1],
                  "measures": [correlation, correlation],
                  "markers": [correlation]
                }
            if(income_level not in formatted_data):
              formatted_data[income_level] = [d]              
            else:
              formatted_data[income_level].append(d)
  print("about to write correlations to a json file")              
  correlations_file = open("./website/data/correlations.json","w")
  correlations_file.write(json.dumps(formatted_data))
  return 0

app = Flask(__name__, static_url_path='')

@app.route('/website/data/<path:path>')
def serve_data(path):
  # get the evictions data from the database and 
  # generate the json file
  print(path + " requested")
  if(path == "evictions.json"):
    generate_evictions_json_file()
    print("Generated evictions json file for the map")
  if(path == "correlations.json"):
    generate_correlations_json_file()
    print("Generated correlations json file for the graph")
  return send_from_directory('./website/data', path)

# main css file
@app.route('/website/css/<path:path>')
def serve_css(path):
  return send_from_directory('./website/css', path)

# javascript files
@app.route('/website/js/<path:path>')
def serve_js(path):
  return send_from_directory('./website/js', path)

# custom route for the iframe
@app.route('/evictions_map', methods=['GET'])
def get_boston_evictions():
  return open('./website/html/evictions_map.html','r').read()

# html file for the correlations graph
@app.route('/', methods=['GET'])
def get_index_page():
  return open('./website/html/index.html','r').read()

@app.errorhandler(404)
def not_found(error):
  return make_response(jsonify({'error': 'Not found.'}), 404)

if __name__ == '__main__':
  app.run(debug=True)
