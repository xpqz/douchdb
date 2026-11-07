// Reduce wrapper shared by the native engine (src/view.rs) and the WASM
// runner (wasm/qjs-runner). The source placeholder below is replaced by the
// user's reduce function; the wrapper takes JSON keys and values plus the rereduce
// flag and returns the JSON result ("null" when the function returns nothing).
(function () {
  function sum(values) {
    var total = 0;
    for (var i = 0; i < values.length; i++) total += values[i];
    return total;
  }
  function log(message) { }
  function isArray(value) { return Array.isArray(value); }
  function toJSON(value) { return JSON.stringify(value); }
  var __reduce = (__DOUCHDB_SOURCE__);
  if (typeof __reduce !== "function") {
    throw new Error("reduce must be a function");
  }
  return function (keys, values, rereduce) {
    var result = __reduce(JSON.parse(keys), JSON.parse(values), rereduce);
    return result === undefined ? "null" : JSON.stringify(result);
  };
})()
