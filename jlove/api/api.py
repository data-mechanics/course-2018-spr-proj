#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 27 18:01:17 2018

@author: jlove
"""

from flask import Flask, abort
from flask_restful import reqparse, Api, Resource
from run_k_means import kmeans
import json
import requests
import pymongo

#os.urandom for random data


app = Flask(__name__)
api = Api(app)


class Hubs(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument("number", required=True, type=int)

    def get(self):
        args = self.parser.parse_args(strict=True)
        number = args['number']
        work = kmeans(number)
        locations = work.run()

        return locations
        

api.add_resource(Hubs, "/kmeans")
            
if __name__ == '__main__':
    app.run()