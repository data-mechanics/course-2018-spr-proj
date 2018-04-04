# Filename: TransformFireData.py
# Author: Claire Russack <crussack@bu.edu>
# Description: K-Means clustering.
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import prequest
import geocoder
from sklearn.cluster import KMeans
import pandas as pd
import numpy as np

class TransformFireData(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = []
    writes = []

    @staticmethod
    def execute(trial = False):
        pass

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass