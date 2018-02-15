d = db.bemullen_dharmesh;
enforcements = d.code_enforcements;
service_requests = d.service_requests;

mbta_red = d.mbta_red_dwells;
mbta_green = d.mbta_green_dwells;
universities = d.universities;

enforcements.find({Status_DTTM: {$not: {$type: 9}}, Status_DTTM: {$ne: null}}).forEach(function(doc) {
    doc.Status_DTTM = new ISODate(doc.Status_DTTM);
    enforcements.save(doc);
});

enforcements_binned = enforcements.aggregate([{
    $match: {
        "Status": {$eq: "Closed"}
    }},
    {$group: {
        _id: {
            month: {$month: "$Status_DTTM"},
            year: {$year: "$Status_DTTM"}
        },
        average_enforced: {$sum: 1}
    }
}]);

service_requests.find({CASE_STATUS: {$eq: "Closed"}}).forEach(function(doc){
    doc.closed_dt = new Date(doc.closed_dt);
    service_requests.save(doc);
});

service_requests_binned = service_requests.aggregate(
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

// Filter our by date and status

// Create dates for MBTA data
mbta_red_dwells.find().forEach(function(doc) {
    dr = new Date(0);
    dr.setSeconds(parseFloat(doc.arr_dt));
    doc.date = dr;
    mbta_red_dwells.save(doc);
});

// Create dates for MBTA data
mbta_green_dwells.find().forEach(function(doc) {
    dr = new Date(0);
    dr.setSeconds(parseFloat(doc.arr_dt));
    doc.date = dr;
    mbta_green_dwells.save(doc);
});

mbta_red_dwells.find({dwell_time_sec: {$not: {$type: "int"}}}).forEach(function(doc) {
    doc.dwell_time_sec = parseInt(doc.dwell_time_sec)
    mbta_red_dwells.save(doc);
});

mbta_green_dwells.find({dwell_time_sec: {$not: {$type: "int"}}}).forEach(function(doc) {
    doc.dwell_time_sec = parseInt(doc.dwell_time_sec)
    mbta_green_dwells.save(doc);
});

mbta_red_binned = mbta_red_dwells.aggregate(
    [
    {
        $group: {
            _id: {
                month: {$month: "$date"},
                year: {$year: "$date"},
            },
            dwell_time: {$avg: "$dwell_time_sec"},
        },
    }]);

mbta_green_binned = mbta_green_dwells.aggregate(
    [
    {
        $group: {
            _id: {
                month: {$month: "$date"},
                year: {$year: "$date"},
            },
            dwell_time: {$avg: "$dwell_time_sec"},
        },
    }]);

db.createCollection(mbta_dwell_times);