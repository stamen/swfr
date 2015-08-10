"use strict";

var fs = require("fs"),
    path = require("path"),
    url = require("url"),
    util = require("util");

var LRU = require("lru-cache");

var CACHE = LRU({
  max: 10 * 1024 * 1024, // 10MB
  length: function(x) {
    return JSON.stringify(x.length);
  }
});

module.exports = (function() {
  // TODO prioritize
  // TODO add a function that determines whether a given persister is appropriate
  var persisters = {};

  return {
    init: function() {
      var persisters = 
            fs.readdirSync(path.join(__dirname, "persisters"))
            .map(function(x) {
              var persister = require(path.join(__dirname, "persisters", x));

              return [persister.protocol, persister];
            })
            .reduce(function(obj, data) {
              obj[data[0]] = data[1];

              return obj;
            }, {});
    },

    load: function(payload, callback) {
      var uri = payload.uri;

      if (uri && typeof uri === "string") {
        uri = url.parse(uri);
      }

      if (uri && persisters[uri.protocol]) {
        var val;

        if ((val = CACHE.get(payload.uri))) {
          return setImmediate(callback, null, val);
        }

        return persisters[uri.protocol].load(uri, callback);
      }

      // pass-through
      return setImmediate(callback, null, payload);
    },

    save: function(taskToken, payload, callback) {
      var payloadString = JSON.stringify(payload);

      if (payloadString.length <= 32 * 1024) {
        // use SWF (pass-through)

        return setImmediate(callback, null, payload);
      }

      // TODO this doesn't account for the addition of a timestamp
      if (payloadString.length <= 400 * 1024) {
        // use DynamoDB
        var persister = persisters["dynamodb:"],
            uri = persister.makeURI(taskToken);

        // has it already been saved? (is it in the cache?)
        var val;

        if ((val = CACHE.get(uri))) {
          return setImmediate(callback, null, {
            uri: uri
          });
        }

        return persister.save(taskToken, payload, function(err, ref) {
          if (!err) {
            CACHE.set(ref.uri, payload);
          }

          return callback.apply(null, arguments);
        });
      }

      return setImmediate(callback, new Error(util.format("Payload for task %s is too large (%dKB)", taskToken, payload.length)));
    }
  };
})();
