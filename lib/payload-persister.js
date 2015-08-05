"use strict";

var url = require("url"),
    util = require("util");

var AWS = require("aws-sdk");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || AWS.config.region || "us-east-1"
});

var dynamodb = new AWS.DynamoDB();

// TODO load modules
// TODO prioritize
// TODO add a function that determines whether a given persister is appropriate
var persisters = {
  "dynamodb:": {
    // Assumptions:
    // key attribute is named "key" and is a String
    // "result" attribute exists, is a String, and contains JSON
    load: function(uri, callback) {
      var tableName = uri.hostname,
          key = uri.pathname;

      return dynamodb.getItem({
        TableName: tableName,
        Key: {
          key: {
            S: key
          }
        },
        ConsistentRead: true
      }, function(err, data) {
        if (err) {
          return callback(err);
        }

        try {
          return callback(null, JSON.parse(data.Item.result.S));
        } catch (err) {
          return callback(err);
        }
      });
    },

    save: function(key, payload, callback) {
      // TODO make this configurable (use the SWF domain name?)
      var tableName = "test";

      return dynamodb.putItem({
        TableName: tableName,
        Item: {
          key: {
            S: key
          },
          result: {
            S: payload
          },
          createdAt: {
            S: new Date().toISOString()
          }
        }
      }, function(err, data) {
        if (err) {
          return callback(err);
        }

        return callback(null, {
          uri: util.format("dynamodb://%s/%s", tableName, taskToken)
        });
      });
    }
  }
};

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
