/*
 * Filename: transformServiceRequests.js
 * Author: Dharmesh Tarapore <dharmesh@bu.edu>
 *
 */
d = db.bemullen_crussack_dharmesh_vinwah;

service_requests = d.service_requests;

// Filter out open requests since we won't use them just yet.
service_requests.find({CASE_STATUS: {$eq: "Closed"}}).forEach(function(doc){
    doc.closed_dt = new Date(doc.closed_dt);
    service_requests.save(doc);
});

// Break service requests down (selection) on the basis of 
// the criteria specified in CASE_STATUS and then 
// average the number of requests received. 
// Finally, project this into a binned dataset of the form
// (month, year, number of service requests).
var binnedServiceRequests = service_requests_binned = service_requests.aggregate(
    [
    {
        $match: {
            $and: [
            {"CASE_STATUS": {$eq: "Closed"}},
            {
                $or: [
                    
                    {"TYPE": {$eq: "Improper Storage of Trash (Barrels)"}},
                    {"TYPE": {$eq: "Student Move-in Issues"}},
                    {"TYPE": {$eq: "Unsafe Dangerous Conditions"}},
                    {"TYPE": {$eq: "Overflowing or Un-kept Dumpster"}},
                    {"TYPE": {$eq: "Squalid Living Conditions"}},
                    {"TYPE": {$eq: "Illegal Posting of Signs"}},
                    {"TYPE": {$eq: "Undefined Noise Disturbance"}},
                    {"TYPE": {$eq: "Loud Parties/Music/People"}},
                    {"TYPE": {$eq: "Bicycle Issues"}},
                    {"TYPE": {$eq: "Illegal Use"}},
                    {"TYPE": {$eq: "Student Overcrowding"}},
                    {"TYPE": {$eq: "Illegal Rooming House"}},
                    {"TYPE": {$eq: "Overflowing or Un-kept Dumpster"}},
                    {"TYPE": {$eq: "Parking on Front/Back Yards (Illegal Parking)"}},
                    {"TYPE": {$eq: "Trash on Vacant Lot"}},
                    {"TYPE": {$eq: "Parking Enforcement"}}
                    ]}
                
            ]
        }
    },
    {
        $group: {
            _id: {
                month: {$month: "$closed_dt"},
                year: {$year: "$closed_dt"},
            },
            number_of_requests: {$sum: 1},
        },
    }]);

arrayVals = binnedServiceRequests.toArray();
d.service_requests_monthly.remove({});
db.createCollection("service_requests_monthly");
d.service_requests_monthly.insertMany(arrayVals);
