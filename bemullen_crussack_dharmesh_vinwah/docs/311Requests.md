### Author: Vincent Wahl <vinwah@bu.edu>
The following two pices of code, 'Retrieve311Requests.py' and 'Transform311Requests.py', is used to find the minimum amount of 311 dispatchers required to accommodate for all received inquiries.

The motivation for determining the minimum amount of dispatchers was prompted by their own requirement of responding within 30 seconds to each inquiry. The 'Transform311Requests.py' finds how many people will be required to respond to the inquiries to achieve this. 

This data will further be used to determine how students change the max and average load on the 311 central.

---------------------------------------------------------------------------------------------------------------------

Retrieve311Requests.py:
Requests data over all 311 inquiries in the period 2016-02-01 to 2018-03-30 from <a href="https://data.boston.gov/dataset/311-service-requests">Analyze Boston</a>, and stores the data into the mongoDB collection 'bemullen_crussack_dharmesh_vinwah.service_requests'. The script retrieves a copy of this data, retrieved 4th of April 2018, stored on this <a href="https://cs-people.bu.edu/dharmesh/cs591/591data/service_requests_filtered.json">server</a>.

---------------------------------------------------------------------------------------------------------------------

Transform311Requests.py:
Imports the 311 data from the mongoDB and filters it on relevant categories. The relevant categories are all categories that concerns non-bussiness attributed reports. 

Then each inquiry is projected to the form (start_time, end_time) which reflects the start time of the received call and estimated end time of call. We have used an expected length of each call set to 3 minutes. The start time of the call is given by the field in the 311 data "open_dt"

Once on this form, the data is filtered into 10days periods.

Each period is then fed into "Transform311Requests.intervalPartitioning(data)" which will find: minimum amount of 311 dispatchers required to accommodate at the maximum load from the period, the average amount of dispatchers required to accomodate each call in the period, the standard deviation of amount of dispatchers required to accommodate each call in the period, and number of inquiries received in the period. 

This data is stored into two mongoDB collections, 'bemullen_crussack_dharmesh_vinwah.service_request_personel_required' for all 10-days periods and 'bemullen_crussack_dharmesh_vinwah.service_request_personel_required_specific_periods' for a selection of periods we are especially interested in (move-in period and 4 specified control periods), on the form:
{
    start: <start date and time of period>,
    end: <end date and time of period>,
    max_required: <minimum amount of 311 dispatchers required to accommodate at the maximum load>,
    avg_required: <average amount of dispatchers required to accommodate each call>,
    std_required: <standard deviation of amount of dispatchers required to accommodate each call>,
    total_number_of_inquiries: <number of inquiries received in the period>
}

---------------------------------------------------------------------------------------------------------------------

The Transform311Requests.intervalPartitioning(data) algorithm:
Keep a global list of resources initialized to [1], describe each job by (start time, end time, [unavailable resources], assigned resource) and initialize unavailable resources to [] and assigned resource to -1.
The algorithm takes in a list of intervals describing jobs, and sorts the intervals according to increasing starting time. Then, for each job J[i], in increasing order of i, it assigns J[i] a resource that is not in its unavailable list, and for all J[j], j != i, that has a starting time earlier than J[i]'s end time, the resource assigned to J[i] is added to J[j]'s unavailable resource list. If no resource is available for J[i], the list of resources is extended by 1 and assigned to J[i]. After doing this for all jobs in a period, the returned value is a tuple with:

max_required = length of resource list which corresponds to number of resources used at max load

avg_required = average of all jobs on: number of occupied resources at the time of resceiving job + 1 (its assigned resource).

std_required = standard diviation of all jobs on: number of occupied resources at the time of resceiving job + 1 (its assigned resource).

total_number_of_inquiries = length of the list of jobs

---------------------------------------------------------------------------------------------------------------------

Results

The two graphs below shows the total amount of inquires to 311 and the average amount of dispatchers required to accommodate each call in our selected periods as an average of all results from same period. From the graphs, we can see that the total amount of inquires to 311 and the average amount of dispatchers required to accommodate each call increases during the move-in week.


<center>
<img src="https://cs-people.bu.edu/dharmesh/cs591/591data/avg_num_inquiries.png"/>
</center>


<center>
<img src="https://cs-people.bu.edu/dharmesh/cs591/591data/avg_personel_required.png"/>
</center>

















