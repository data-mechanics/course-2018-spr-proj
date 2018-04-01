The following two pices of code, 'RetrieveServiceRequests.py' and 'TransformServiceRequests.py', is used to find the minimum amount of 311 dispatchers required to accomodate for all received inquiries.

The motivation for determining the minimum amount of dispatchers was prompted by their own requirement of responding within 30seconds to each inquiry. The 'TransformServiceRequests.py' finds how many people will be required to respond to the inquiries to achive this. 

This data will further be used to determine how students change the max and average load on the 311 central.

---------------------------------------------------------------------------------------------------------------------

RetrieveServiceRequests.py:
Requests data over all 311 inquiries in the period 2016-02-01 to 2018-03-30 from data.boston.gov, and stores the data into the mongoDB collection 'bemullen_crussack_dharmesh_vinwah.service_requests'

---------------------------------------------------------------------------------------------------------------------

TransformServiceRequests.py:
Imports the 311 data from the mongoDB and filters it on relevant categories. The relevant categories are all categories that concerns non-bussiness attributed reports. 

Then each inquiry is projected to the form (start_time, end_time) which reflects the start time of the received call and estimated end time of call. We have used an expected length of call set to 3 minutes. The start time of the call is given by the field in the 311 data "open_dt"

Once on this form, the data is filtered into 10days periods.

Each period is then fed into "TransformServiceRequests.intervalPartitioning(data)" which will find: minimum amount of 311 dispatchers required to accomodate at the maximum load from the period, the average amount of dispatchers required to accomodate each call in the period, the standard diviation of amount of dispatchers required to accomodate each call in the period, and number of inquiries received in the period. 

This data is stored into two mongoDB collections, 'bemullen_crussack_dharmesh_vinwah.service_request_personel_required' for all 10-days periods and 'bemullen_crussack_dharmesh_vinwah.service_request_personel_required_specific_periods' for a selection of periods we are especialy intrested in (move-in period and 4 specified controll periods), on the form:
{
    start: <start date and time of period>,
    end: <end date and time of period>,
    max_required: <minimum amount of 311 dispatchers required to accomodate at the maximum load>,
    avg_required: <average amount of dispatchers required to accomodate each call>,
    std_required: <standard diviation of amount of dispatchers required to accomodate each call>,
    total_number_of_inquiries: <number of inquiries received in the period>
}

---------------------------------------------------------------------------------------------------------------------

The TransformServiceRequests.intervalPartitioning(data) algorithm:
The algorithm takes in a list of intervals describing jobs, sorts the intervals according to increasing starting time. For each job J[i], it assignes J[i] a resource and for all J[j] that has a starting time conflicting with the J[i] the resource assigned to J[i] is added to J[j]'s unavailable resource list. If no resource is available, the list of resources is extended by 1. After doing this for a period, the returned value is a tuple with:

max_required = length of resource list which coresponds to number of resources used at max load

avg_required = average of all jobs on: number of occupied resources at the time of resceiving job + 1 (its assigned resource).

std_required = standard diviation of all jobs on: number of occupied resources at the time of resceiving job + 1 (its assigned resource).

total_number_of_inquiries = length of the list of jobs



















