"use strict";

var crypto = require("crypto");

var AWS = require("aws-sdk");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || "us-east-1"
});

var swf = new AWS.SWF();

// hash the attributes to give us predictable activity ids
// NOTE: also prevents duplicate activities
var hashStream = crypto.createHash("sha512"),
    attrs = {
    domain: "SplitMerge",
    workflowType: {
      name: "SplitMergeWorkflow.start",
      version: "1.5"
    },
    taskList: {
      name: "splitmerge_workflow_tasklist"
    },
    input: JSON.stringify("input"),
    executionStartToCloseTimeout: "600", // 10 minutes
    taskStartToCloseTimeout: "30", // 30 seconds
    childPolicy: "TERMINATE"
    // TODO tagList
    // TODO taskPriority
  };

hashStream.end(JSON.stringify(attrs));
attrs.workflowId = hashStream.read().toString("hex");

swf.startWorkflowExecution(attrs, function(err, data) {
  if (err) {
    throw err;
  }

  console.log("domain:", attrs.domain);
  console.log("workflowId:", attrs.workflowId);
  console.log("Response:", data);
});
