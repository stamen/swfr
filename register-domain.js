"use strict";

var AWS = require("aws-sdk");

var argv = process.argv.slice(2);

if (argv.length < 2) {
  console.warn("Usage: register-domain.js <domain> <retention in days> [description]");
  process.exit(1);
}

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || "us-east-1"
});

var swf = new AWS.SWF();

swf.registerDomain({
  name: argv.shift(),
  workflowExecutionRetentionPeriodInDays: argv.shift(),
  description: argv.shift()
}, function(err, data) {
  if (err) {
    throw err;
  }

  process.stdout.write(JSON.stringify(data, null, "  "));
});
