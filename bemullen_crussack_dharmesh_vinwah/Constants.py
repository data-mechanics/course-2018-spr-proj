# Filename: Constants.py
# Author: Dharmesh Tarapore <dharmesh@bu.edu>
# Description: Store values in one place so we don't 
#              keep adding multiple copies of the same string.
import dml
import prov.model
import datetime
import uuid

class Constants(dml.Algorithm):

	BASE_AUTH = "bemullen_crussack_dharmesh_vinwah"

	CONTRIBUTOR = BASE_AUTH
	contributor = BASE_AUTH
	
	reads = []
	writes = []

	@staticmethod
	def execute(trial=False):
		pass

	@staticmethod
	def provenance(doc=prov.model.ProvDocument(),startTime= None,endTime=None):
		pass