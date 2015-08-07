"use strict";

var fs = require("fs"),
    path = require("path");

var activities = fs.readdirSync(path.join(__dirname, "activities"))
  .map(function(a) {
    var activity = require(path.join(__dirname, "activities", a));

    if (activity.name && activity.version) {
      return [JSON.stringify({
        name: activity.name,
        version: activity.version
      }), activity];
    }

    console.warn("Activity in %s is not fully defined; please ensure that it has a name and version", a);
    return null;
  })
  .reduce(function(hash, data) {
    if (data) {
      hash[data[0]] = data[1];
    }

    return hash;
  }, {});

module.exports = function(name, version) {
  return activities[JSON.stringify({
    name: name,
    version: version
  })];
};
