/*
 * Filename: transformCityScores.js
 * Author: Dharmesh Tarapore <dharmesh@bu.edu>
 * Description: Data transformations on the cityscores dataset.
 */
d = db.bemullen_dharmesh;
scores = d.cityscores;

// Cleanup nulls and ensure score dates are appropriately stored and null values removed
scores.find({CTY_SCR_DAY: {$not: {$type: 1}}, CTY_SCR_DAY: {$ne: null}}).forEach(function(doc) {
    doc.CTY_SCR_DAY = parseFloat(doc.CTY_SCR_DAY);
    scores.save(doc);
});

scores.find({ETL_LOAD_DATE: {$not: {$type: 9}}, ETL_LOAD_DATE: {$ne: null}}).forEach(function(doc) {
    doc.ETL_LOAD_DATE = new Date(doc.ETL_LOAD_DATE);
    scores.save(doc);
});

// Aggregate scores in 3 steps:
// 1. Filter out records not related to city services satisfaction surveys,
//    311 call center performance, or part 1 crimes.
//
// 2. For each metric for each month, calculate the average score
//
// 3. Store this as scores_binned of the form (month, year, cityscore_avg)
var scores_binned = scores.aggregate(
    [
    {$match: {
        $or: [
        {"CTY_SCR_DAY": {$ne: null}},
        {"CTY_SCR_NAME": {$eq: "CITY SERVICES SATISFACTION SURVEYS"}},
        {"CTY_SCR_NAME": {$eq: "311 CALL CENTER PERFORMANCE"}},
        {"CTY_SCR_NAME": {$eq: "PART 1 CRIMES"}},  
        ]}
    },
    {
        $group: {
            _id: {
                month: {$month: "$ETL_LOAD_DATE"},
                year: {$year: "$ETL_LOAD_DATE"},
            },
            cityscore: {$avg: "$CTY_SCR_MONTH"},
        },
    }]);

arrayVals = scores_binned.toArray();
d.cityscores_monthly.remove({});
db.createCollection("cityscores_monthly");
db.bemullen_dharmesh.cityscores_monthly.insertMany(arrayVals);



