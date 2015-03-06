"use strict";

var AWS = require("aws-sdk");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || "us-east-1"
});

var swf = new AWS.SWF();

swf.startWorkflowExecution({
  domain: "SplitMerge",
  workflowId: "workflowId", // TODO a UUID
  workflowType: {
    name: "SplitMergeWorkflow.start",
    version: "1.5"
  },
  taskList: {
    name: "splitmerge_workflow_tasklist"
  },
  input: JSON.stringify("input"),
  executionStartToCloseTimeout: '120', // 2 minutes
  taskStartToCloseTimeout: '30' // 30 seconds
  // TODO childPolicy
  // TODO tagList
  // TODO taskPriority
}, function(err, data) {
  if (err) {
    throw err;
  }

  console.log("domain:", "SplitMerge");
  console.log("workflowId:", "workflowId");
  console.log("Response:", data);
});
