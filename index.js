var MongoClient = require('mongodb').MongoClient
var Batch = require('batch');
var Promise = require("bluebird")
var promisifyAll = Promise.promisifyAll;
var promisify = Promise.promisify;

module.exports = Db;

function Db(dbUrl) {
    var self = this;
    MongoClient.connect(dbUrl, function(err, db) {
        if (err) {
           console.log("err", err)
           throw err
        }
        self.db = db
        self.log("debug", "Connected correctly to server");
    });
}

Db.prototype.log = function (level, message) {
    console.log(level + ": " + message);
}

Db.prototype.get = function (type, id) {
    return this.query(type, {id: id})
        .then(function (docs) {
            return docs[0];
        });
}

Db.prototype.query = function (type, query, options) {
    var self = this;
    var collection = this.db.collection(type);
    var res = collection.find(query)
    var toArray = promisify(res.toArray, res)
    var load = options && options._load
    if (!load) {
        return toArray()
    }

    var loadColl = this.db.collection(load);
    return toArray().then(function (result) {
        var ids = result.map(function (item) {
            return item[load];
        })
        var flattened = ids.reduce(function (all, current) {
            return all.concat(current);
        }, [])
        var joinQuery = loadColl.find({id: {$in: flattened }})
        var toArray = promisify(joinQuery.toArray, joinQuery);
        return Promise.all([Promise.resolve(result), toArray()])
    }).then(function (all) {
        var items = all.shift();
        var joinedItems = all.shift();

        items = items.map(function (item) {
            var ids = item[load]
            item[load] = ids.map(function (id) {
                return joinedItems.filter(function (joined) {
                    return joined.id == id
                }).pop()
            })
            return item;
        })
        return items;
    });
}

Db.prototype.byIds = function (type, ids) {
    return Promise.all(ids.map(this.get.bind(this, type)))
}

Db.prototype.save = function (type, id, obj) {
    var collection = this.db.collection(type)
    var findOneAndUpdate = promisify(collection.findOneAndUpdate, collection)
    var res = findOneAndUpdate({id: id}, obj, {sort: 'id', upsert: true})
    return res;
}

Db.prototype.saveOne = function (type, obj) {
    return this.save(type, obj.id, obj)
}

Db.prototype.deleteById = function (type, id) {
    var collection = this.db.collection(type)
    var deleteOne = promisify(collection.deleteOne, collection)
    return deleteOne({
        id: id
    })
}

Db.prototype.deletedCollection = function (type)  {
    return type + "-deleted"
}

Db.prototype.softDelete = function (type, id) {
    var self = this;
    var collection = this.db.collection(type);
    this.get(type, id, function(err, doc) {
        if (err) throw new Error("Could not find " + type + "/" + id)
        self.create(self.deletedCollection(type), doc, function (err, result) {
            collection.remove({id: id}, function (err, result) {
                console.log("deleted", err, result)
            })
        })
    })
}