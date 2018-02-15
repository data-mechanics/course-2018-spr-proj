function getKeysForCollection(name) {
    mr = db.runCommand({
        "mapreduce" : "bemullen_dharmesh." + name,
        "map" : function() {
            for (var key in this) { emit(key, null); }
        },
    "reduce" : function(key, stuff) { return null; }, 
    "out": "bemullen_dharmesh." + name + "_keys"
    });
    return db[mr.result].distinct("_id");
}