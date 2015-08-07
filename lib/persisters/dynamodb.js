"use strict";

var util = require("util");

var AWS = require("aws-sdk"),
    env = require("require-env");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || AWS.config.region || "us-east-1"
});

var dynamodb = new AWS.DynamoDB();

var PROTOCOL = "dynamodb:",
    // TODO make this configurable (use the SWF domain name?)
    TABLE_NAME = env.require("AWS_DYNAMODB_TABLE");

var makeURI = function(key) {
  return util.format("%s//%s/%s", PROTOCOL, TABLE_NAME, key);
};

// TODO add a function that determines whether a given persister is appropriate
module.exports = {
  // Assumptions:
  // key attribute is named "key" and is a String
  // "result" attribute exists, is a String, and contains JSON
  load: function(uri, callback) {
    var tableName = uri.hostname,
        key = uri.pathname.slice(1);

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

  makeURI: makeURI,

  protocol: PROTOCOL,

  save: function(key, payload, callback) {
    return dynamodb.putItem({
      TableName: TABLE_NAME,
      Item: {
        key: {
          S: key
        },
        result: {
          S: JSON.stringify(payload)
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
        uri: makeURI(key)
      });
    });
  }
};
