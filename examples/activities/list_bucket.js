"use strict";

var assert = require("assert"),
    url = require("url");

var AWS = require("aws-sdk");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || AWS.config.region || "us-east-1"
});

var s3 = new AWS.S3();

module.exports = function listBucket(bucket, callback) {
  var uri = url.parse(bucket);

  assert.equal("s3:", uri.protocol, "Protocols other than s3: are unsupported.");

  var listObjects = function(memo, nextMarker, done) {
    if (memo && !nextMarker) {
      return setImmediate(callback, null, memo);
    }

    memo = memo || [];

    return s3.listObjects({
      Bucket: uri.hostname,
      Delimiter: "/",
      Marker: nextMarker || "",
      Prefix: uri.pathname.slice(1)
    }, function(err, data) {
      if (err) {
        return callback(err);
      }

      var objects = data.Contents.map(function(x) {
        return x.Key;
      });

      if (data.NextMarker) {
        console.log("NextMarker:", data.NextMarker);
        return listObjects(memo.concat(objects), data.NextMarker, done);
      }

      return done(null, memo.concat(objects));
    });
  };

  return listObjects(null, null, callback);
};

module.exports.version = "1.0";
