"use strict";

var fs = require("fs"),
    path = require("path");

var activities = function activities(activitiesFolder) {
  return fs.readdirSync(activitiesFolder)
  .filter(function(a) { 
    var suffix = "js";
    return a.indexOf(suffix, a.length - suffix.length) !== -1;
  })
  .map(function(a) {
    var activity = require(path.join(activitiesFolder, a));

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
};

module.exports = function(activitiesFolder) {
  return function(name, version) {
    return activities(activitiesFolder)[JSON.stringify({
      name: name,
      version: version
    })];
  };
};
