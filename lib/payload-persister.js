"use strict";

var fs = require("fs"),
    path = require("path"),
    url = require("url"),
    util = require("util");

// TODO prioritize
// TODO add a function that determines whether a given persister is appropriate
var persisters = fs.readdirSync(path.join(__dirname, "persisters"))
  .map(function(x) {
    var persister = require(path.join(__dirname, "activities", x));

    return [persister.protocol, persister];
  })
  .reduce(function(obj, data) {
    obj[data[0]] = data[1];

    return obj;
  }, {});

module.exports = {
  load: function(payload, callback) {
    var uri = payload.uri;

    if (uri && typeof uri === "string") {
      uri = url.parse(uri);
    }

    if (uri && persisters[uri.protocol]) {
      return persisters[uri.protocol].load(uri, callback);
    }

    // pass-through
    return setImmediate(callback, null, payload);
  },

  save: function(taskToken, payload, callback) {
    var payload = JSON.stringify(payload);

    if (payload.length <= 32 * 1024) {
      // use SWF (pass-through)

      return setImmediate(callback, null, payload);
    }

    // TODO this doesn't account for the addition of a timestamp
    if (payload.length <= 400 * 1024) {
      // use DynamoDB

      return persisters["dynamodb:"].save(taskToken, payload, callback);
    }

    return setImmediate(callback, new Error(util.format("Payload for task %s is too large (%dKB)", taskToken, payload.length)));
  }
};
