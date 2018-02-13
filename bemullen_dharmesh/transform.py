# Filename: transform.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
# Description: Implements transformations on data collections.
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class transform(dml.Algorithm):
    contributor = 'bemullen_dharmesh'
    reads = []
    writes = []


    @staticmethod
    def execute(trial=False):
        '''Retrieve data from Mongo before working on transformations.'''

        startTime = datetime.datetime.now()

        # Setup database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Setup database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_dharmesh', 'bemullen_dharmesh')

        repo.logout()

        return doc



