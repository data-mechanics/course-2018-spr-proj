#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 27 18:01:17 2018

@author: jlove
"""

from flask import Flask, abort
from flask_restful import reqparse, Api, Resource
import json
import requests
import pymongo

#os.urandom for random data


app = Flask(__name__)
api = Api(app)
    
class Neighborhood(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument("location",required=True, type=dict)
    def post(self):
        args = self.parser.parse_args(strict=True)
        location = args['location']
        return {}
        

api.add_resource(Neighborhood,"/progress")
            
if __name__ == '__main__':
    app.run()