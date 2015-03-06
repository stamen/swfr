"use strict";

var AWS = require("aws-sdk");

var argv = process.argv.slice(2);

if (argv.length < 4) {
  console.warn("Usage: register-workflow.js <domain> <name> <version> <tasklist> [description]");
  process.exit(1);
}

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || "us-east-1"
});

var swf = new AWS.SWF();

swf.registerActivityType({
  domain: argv.shift(),
  name: argv.shift(),
  version: argv.shift(),
  defaultTaskList: {
    name: argv.shift()
  },
  description: argv.shift()
}, function(err, data) {
  if (err) {
    throw err;
  }

  process.stdout.write(JSON.stringify(data, null, "  "));
});
