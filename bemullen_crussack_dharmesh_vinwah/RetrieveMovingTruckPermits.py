# Filename: RetrieveMovingTruckPermits.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
# Description: Retrieve datasets from the sources provided and generate 
#              the data lineage.
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid

class RetrieveMovingTruckPermits(dml.Algorithm):

    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = []
    writes = ["bemullen_crussack_dharmesh_vinwah.moving_truck_permits"]

    @staticmethod
    def execute(trial = False):
        pass

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass