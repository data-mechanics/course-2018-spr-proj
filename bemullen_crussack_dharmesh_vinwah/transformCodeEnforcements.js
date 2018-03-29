/*
 * Filename: transformCodeEnforcements.js
 * Author: Dharmesh Tarapore <dharmesh@bu.edu>
 * Description: Data transformations on the code enforcements dataset.
 */
d = db.bemullen_crussack_dharmesh_vinwah;
enforcements = d.code_enforcements;


// Here, we perform  a selection to filter by dates that are not null
// to cast them correctly for ease of use in the future.
enforcements.find({
    $and:[
        {Status_DTTM: {$not: {$type: 9}}},
        {Status_DTTM: {$ne: null}}  
        ]}).forEach(function(doc) {
            doc.Status_DTTM = new Date(doc.Status_DTTM);
            enforcements.save(doc);
        });

// Here, we filter by closed tickets first, then perform a
// projection to develop a dataset of the form (month, year, number of violations)
var enforcements_binned = enforcements.aggregate([{
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

arrayVals = enforcements_binned.toArray();
d.enforcements_monthly.remove({});
db.createCollection("enforcements_monthly");
d.enforcements_monthly.insertMany(arrayVals);