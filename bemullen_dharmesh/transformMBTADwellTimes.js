/*
 * Filename: transformCodeEnforcements.js
 * Author: Dharmesh Tarapore <dharmesh@bu.edu>
 * Description: Data transformations on the MBTA dwell intervals dataset.
 */
d = db.bemullen_dharmesh;

mbta_red_dwells = d.mbta_red_dwells;
mbta_green_dwells = d.mbta_green_dwells;
// universities = d.universities;

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

var redBinned = mbta_red_binned = mbta_red_dwells.aggregate(
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

redArrayVals = redBinned.toArray();
d.mbta_red_dwells_monthly.remove({});
db.createCollection("mbta_red_dwells_monthly");
d.mbta_red_dwells_monthly.insertMany(redArrayVals);

var greenBinned = mbta_green_binned = mbta_green_dwells.aggregate(
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

greenArrayVals = greenBinned.toArray();
d.mbta_green_dwells_monthly.remove({});
db.createCollection("mbta_green_dwells_monthly");
d.mbta_green_dwells_monthly.insertMany(greenArrayVals);
