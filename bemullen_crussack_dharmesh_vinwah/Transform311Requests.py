# Filename: Transform311Requests.py
# Author: Vincent Wahl <vinwah@bu.edu>
# Description:  Transform the 311 data,
#               find the minimum amount of people
#               required to accomodate all incoming calls, 
#               find average and standard diviation of minimum
#               amount of people required to accomodate each
#               incoming call.

import datetime
from operator import itemgetter
import numpy as np
import dml
import prov.model
import pandas as pd 
import matplotlib.pyplot as plt 

class Transform311Requests(dml.Algorithm):

    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = ["bemullen_crussack_dharmesh_vinwah.service_requests"]
    writes = ["bemullen_crussack_dharmesh_vinwah.service_request_personel_required",\
    "bemullen_crussack_dharmesh_vinwah.service_request_personel_required_specific_periods"]

    # Utility methods
    @staticmethod
    def select(R, s):
        return [t for t in R if s(t)]

    @staticmethod
    def project(R, p):
        return [p(t) for t in R]

    # optimization solver:  given time intervals of work loads, what is 
    # the amount of resources required to accomodate
    #                       for all the jobs
    # input:  intervals in a list with elements on form (datetime(start_time), datetime(end_time))
    # output: (minimum number of resources on maximum load, average number of resources,
    # standared diviation of resources, total amount of intervals)
    @staticmethod
    def intervalPartitioning(data):
        # sort in increasing order of start times
        D = sorted(data, key=itemgetter(0))
        # project on form (start_time, end_time, [unavailable_resource,...], assigned_resource)
        D = Transform311Requests.project(D, lambda x: (x[0],x[1],[],-1)) 
        # resource list, extended if needed
        resources = [0]

        for i in range(len(D)):
            # find available resource
            for resource in resources:
                # assign a resource that is not in its unavailable list
                if resource not in D[i][2]:
                    D[i] = (D[i][0], D[i][1], D[i][2], resource)
        
            # if no resource is available, extend resources by one and assign to interval
            if D[i][3] == -1:
                resource = len(resources)
                resources.append(resource)
                D[i] = (D[i][0], D[i][1], D[i][2], resource)

            # put P[i]'s resource into unavailable list of P[j] if j's start time overlap with i's
            for j in range(i+1, len(D)):
                if D[i][1] < D[j][0]:
                    break
                D[j][2].append(D[i][3])

        # number of resources required to accomodate each resource
        resources_required = Transform311Requests.project(D, lambda x: len(x[2])+1)

        avg_resources_required   = np.mean(resources_required)
        stdv__resources_required = np.std(resources_required)

        return (len(resources), avg_resources_required, stdv__resources_required, len(D))

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')


        # Expected time of each call
        time_to_call = datetime.timedelta(seconds=180)
        # Length of each period to look at                                     
        length_of_period = datetime.timedelta(days=10)                                     
        # start of first period
        start_date = datetime.datetime.strptime('2016-02-01 00:00:00', '%Y-%m-%d %H:%M:%S')

        # load service requests from repo
        service_requests = repo['bemullen_crussack_dharmesh_vinwah.service_requests']

        # project data on form (start_time, end_time, type)
        d = lambda x: (datetime.datetime.strptime(x['open_dt'], '%Y-%m-%dT%H:%M:%S'),\
            datetime.datetime.strptime(x['open_dt'], '%Y-%m-%dT%H:%M:%S')+time_to_call, x['TYPE'])
        D = Transform311Requests.project(service_requests.find(), d)

        # filter data on relevant inquiries
        f = lambda x: x[2] in ["Improper Storage of Trash (Barrels)", "Student Move-in Issues",\
                               "Unsafe Dangerous Conditions", "Overflowing or Un-kept Dumpster",\
                               "Squalid Living Conditions", "Illegal Posting of Signs",\
                               "Undefined Noise Disturbance", "Loud Parties/Music/People",\
                               "Bicycle Issues", "Student Overcrowding", "Illegal Rooming House",\
                               "Parking on Front/Back Yards (Illegal Parking)", "Trash on Vacant Lot",\
                               "Parking Enforcement"]
        D = Transform311Requests.select(D, f)

        # project data on form (start_time, end_time)
        d = lambda x: (x[0], x[1])
        D = Transform311Requests.project(D, d)

        # variables to iterate over
        # start of period 
        period_start = start_date                   
        # end of period
        period_end = start_date + length_of_period  

        # return true if inquiries is in the period
        s = lambda x: period_start <= x[0] <= period_end or period_start <= x[1] <= period_end

        data = []
        # iterate over all periods
        while(period_end < datetime.datetime.strptime('2018-03-30 00:00:00', '%Y-%m-%d %H:%M:%S')):
            # filter inquiries from period
            dates_in_period = Transform311Requests.select(D, s)
            # find max, avg, std, and total_number_of_inquiries for period
            (max_required, avg_required, std_required,\
                total_number_of_inquiries) = Transform311Requests.intervalPartitioning(dates_in_period)

            data.append( (period_start,period_end,max_required,avg_required\
                ,std_required,total_number_of_inquiries))
            period_start = period_end
            period_end = period_end + length_of_period

        result = []
        # put into mongoDB dictionary format
        for d in data:
            result.append({'start': d[0], 'end': d[1], 'max_required': d[2],\
                'avg_required': d[3], 'std_required': d[4], 'total_number_of_inquiries': d[5]})

        # insert into mongoDB, collection "bemullen_crussack_dharmesh_vinwah.service_request_personel_required"
        repo.dropCollection('bemullen_crussack_dharmesh_vinwah.service_request_personel_required')
        repo.createCollection('bemullen_crussack_dharmesh_vinwah.service_request_personel_required')
        repo['bemullen_crussack_dharmesh_vinwah.service_request_personel_required'].insert_many(result)


        # Selected periods we especialy want to look at
        selected_periods = [datetime.datetime.strptime('2016-08-30 00:00:00', '%Y-%m-%d %H:%M:%S'),
                            datetime.datetime.strptime('2017-08-30 00:00:00', '%Y-%m-%d %H:%M:%S'),

                            datetime.datetime.strptime('2016-12-22 00:00:00', '%Y-%m-%d %H:%M:%S'),
                            datetime.datetime.strptime('2017-12-22 00:00:00', '%Y-%m-%d %H:%M:%S'),

                            datetime.datetime.strptime('2016-06-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
                            datetime.datetime.strptime('2017-06-01 00:00:00', '%Y-%m-%d %H:%M:%S'),

                            datetime.datetime.strptime('2016-10-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
                            datetime.datetime.strptime('2017-10-01 00:00:00', '%Y-%m-%d %H:%M:%S'),

                            datetime.datetime.strptime('2016-02-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
                            datetime.datetime.strptime('2017-02-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
                           ]

        data = []
        for period_start in selected_periods:
            period_end   = period_start + datetime.timedelta(days=10)

            # filter inquiries from period
            dates_in_period = Transform311Requests.select(D, lambda x: period_start <= x[0] <= period_end or
                                                  period_start <= x[1] <= period_end)
            # find max, avg, std, and total_number_of_inquiries for period
            (max_required,avg_required,std_required,\
                total_number_of_inquiries) = Transform311Requests.intervalPartitioning(dates_in_period)
            data.append( (period_start,period_end,max_required,\
                avg_required,std_required,total_number_of_inquiries))

        result = []
        # put into mongoDB dictionary format
        for d in data:
            result.append({'start': d[0], 'end': d[1], 'max_required': d[2], 'avg_required': d[3],\
                'std_required':d[4], 'total_number_of_inquiries':d[5]})

        # insert into mongoDB, collection
        key = "bemullen_crussack_dharmesh_vinwah.service_request_personel_required_specific_periods"
        repo.dropCollection(key)
        repo.createCollection(key)
        repo[key].insert_many(result)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') 
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bdpr', 'https://data.boston.gov/api/3/action/datastore_search_sql')
        doc.add_namespace('bdpm', 'https://data.boston.gov/datastore/odata3.0/')
        doc.add_namespace('csdt', 'https://cs-people.bu.edu/dharmesh/cs591/591data/')
        doc.add_namespace('datp', 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/')


        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#Transform311Requests',\
            {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        service_requests = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#service_requests',\
            {prov.model.PROV_LABEL:'311 Service Requests for Boston city',\
            prov.model.PROV_TYPE:'ont:DataSet'})        
        get_service_requests = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label':'311 Service Requests for Boston City'})        
        doc.wasAssociatedWith(get_service_requests, this_script)
        doc.used(get_service_requests, service_requests, startTime)
        doc.wasAttributedTo(get_service_requests, this_script)
        doc.wasGeneratedBy(service_requests, get_service_requests, endTime)        
        
        repo.logout()
        return doc