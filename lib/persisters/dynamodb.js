"use strict";

var util = require("util");

var AWS = require("aws-sdk");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || AWS.config.region || "us-east-1"
});

var dynamodb = new AWS.DynamoDB();

// TODO add a function that determines whether a given persister is appropriate
module.exports = {
  protocol: "dynamodb:",

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
        uri: util.format("dynamodb://%s/%s", tableName, taskToken)
      });
    });
  }
};
