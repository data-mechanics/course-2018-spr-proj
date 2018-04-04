/*
 * A simple utility (sourced from StackOverflow)
 * to obtain all the keys in a collection.
 */
function getKeysForCollection(name) {
    mr = db.runCommand({
        "mapreduce" : "bemullen_crussack_dharmesh_vinwah." + name,
        "map" : function() {
            for (var key in this) { emit(key, null); }
        },
    "reduce" : function(key, stuff) { return null; }, 
    "out": "bemullen_crussack_dharmesh_vinwah." + name + "_keys"
    });
    return db[mr.result].distinct("_id");
}