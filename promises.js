"use strict";

var util = require("util");

var Promise = require("bluebird"),
    retry = require("bluebird-retry");

// 2 questions:
//   has this instantiation of this function been executed yet? (straight-up
//   replay)
//   has the result of this instantiation been requested? (async)

var history = [];

var methods = {
  echo: function(input, callback) {
    return callback(null, input);
  }
};

var newCancellablePromise = function() {
  return new Promise(function(resolve) {
    // don't resolve synchronously, giving us a chance to cancel it
    return setImmediate(resolve);
  }).cancellable();
};

var call = function(method, input) {
  return new Promise(function(resolve, reject) {
    if (history.length > 0) {
      var entry = history[0];

      if (entry.method === method) {
        history.shift();
        console.log("using history:", entry);

        if (entry.failed) {
          return reject(entry.result);
        }

        return resolve(entry.result);
      }

      return reject(new Error(util.format("Unexpected entry in history:", entry)));
    }

    // do the thing
    console.log("Calling %s(%s)", method, input);
    // TODO append to decisions

    var promise = newCancellablePromise();

    // pass this promise forward and immediately cancel it (to end the chain)
    resolve(promise);
    return promise.cancel();

    // return resolve(new Promise(function(resolve, reject) {
    //   return reject(new Error("execution queued"));
    // }));
    // return resolve(Promise.reject(new Error("execution queued")));
    // return reject(new Error("execution queued"));

    /*
    return methods[method](input, function(err, result) {
      if (err) {
        console.warn(err);
        return reject(err);
      }

      return resolve(result);
    });
    */
  });
};

var myWorkflow = function() {
  console.log("Running workflow");

  var calls = Promise.map([0, 1, 2, 3, 4], function(i) {
    // need to bind arguments to fn, as retry doesn't accept them
    return retry(call.bind(null, "echo", i), {
      interval: 1, // this either running for playback or remotely, so waiting does nothing (0 leads to the default value)
      max_tries: 5
    })
      .catch(function(err) {
        // unwrap cancellation errors and propagate them
        if (err.failure && err.failure instanceof Promise.CancellationError) {
          throw err.failure;
        }

        throw err;
      });
  }, {
    concurrency: 2
  });
  // var calls = Promise.map([0, 1, 2, 3, 4], call.bind(null, "echo"), {
  //   concurrency: 2
  // });

/*
var calls = [0, 1, 2, 3, 4].map(function(i) {
  return call("echo", i);
});
*/

  return Promise.all(calls)
    .then(function(output) {
      console.log("Output:", output);

      var sum = output.reduce(function(a, b) {
        return a + b;
      }, 0);

      console.log("Sum:", sum);

      return call("echo", sum);
    })
    .then(function(result) {
      console.log("Result:", result);

      return call("echo", "done");
    })
    .then(function() {
      console.log("done.");
    });
    // .catch(Promise.CancellationError, function(err) {
    //   console.warn("Chain interrupted:", err);
    // })
    // .catch(function(err) {
    //   console.warn(err);
    // })
    // .finally(function() {
    //   console.log("Finally.");
    // });
};

history.push({
  method: "echo",
  result: 0
});
history.push({
  method: "echo",
  result: 1
});
history.push({
  method: "echo",
  result: new Error("Fake error for input 2"),
  failed: true
});
history.push({
  method: "echo",
  result: 2
});
history.push({
  method: "echo",
  result: 3
});
// history.push({
//   method: "echo",
//   result: 4
// });

Promise
  .resolve()
  .then(myWorkflow)
  .catch(Promise.CancellationError, function(err) {
    console.warn("Chain interrupted:", err);
  })
  .catch(function(err) {
    console.warn(err);
  })
  .finally(function() {
    console.log("done (with this attempt).");
  });
// myWorkflow();

/*
  .then(null, null, function() {
    console.log("appending 0");

    history.push({
      method: "echo",
      result: 0
    });

    // return Promise.resolve();
  })
  .then(workflow)
  .then(function() {
    history.push({
      method: "echo",
      result: 1
    });
  })
  .then(workflow)
  .then(function() {
    history.push({
      method: "echo",
      result: 2
    });
  })
  .then(workflow)
  .then(function() {
    history.push({
      method: "echo",
      result: 3
    });
  })
  .then(workflow)
  .then(function() {
    history.push({
      method: "echo",
      result: 4
    });
  })
  .then(workflow)
  .then(function() {
    history.push({
      method: "echo",
      result: 10
    });
  })
  .then(workflow)
  .then(function() {
    history.push({
      method: "echo",
      result: "done"
    });
  })
  .then(workflow);
  */

/*
call("echo", 0)
  .then(function(output) {
    console.log("Output:", output);
  })
  .catch(function(err) {
    console.warn(err);
  });
  */

/*
var p = new Promise(function(resolve, reject) {
  resolve("resolve");
  reject("reject");
});

p
  .then(function() {
    console.log("then:", arguments);
  })
  .catch(function() {
    console.log("catch:", arguments);
  })
  .finally(function() {
    console.log("finally:", arguments);
  });
  */

 /*
Promise.resolve(".resolve")
  .then(function() {
    console.log("then:", arguments);
  })
  .then(function() {
    console.log("then:", arguments);
  })
  .catch(function() {
    console.log("catch:", arguments);
  });
  */

// to join, use Promise.join() or .all()
