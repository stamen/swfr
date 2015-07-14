"use strict";

var stream = require("stream"),
    util = require("util");

var P = require("bluebird");

var SyncDeciderContext = require("./SyncDeciderContext");

var SyncDeciderWorker = function(fn) {
  stream.Writable.call(this, {
    objectMode: true,
    highWaterMark: 1 // limit the number of buffered tasks
  });

  this._write = function(task, encoding, callback) {
    // pass the input to the function and provide the rest as the context
    var context = new SyncDeciderContext(task),
        chain = P.bind(context),
        input = task.input;

    P
      .bind(context)
      .then(fn.bind(context, chain, input)) // partially apply the worker fn w/ the input
      .catch(function(err) {
        console.warn(err.stack);
      })
      .finally(function() {
        if (this.result) {
          console.log("Output:", this.result);
        }

        return callback();
      });
  };
};

util.inherits(SyncDeciderWorker, stream.Writable);

module.exports = SyncDeciderWorker;
