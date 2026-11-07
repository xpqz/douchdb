// Map wrapper shared by the native engine (src/view.rs) and the WASM runner
// (wasm/qjs-runner). The source placeholder below is replaced by the
// user's map function; the wrapper takes a JSON array of documents and returns a JSON
// array holding, per document, the [key, value] pairs it emitted.
(function () {
  function sum(values) {
    var total = 0;
    for (var i = 0; i < values.length; i++) total += values[i];
    return total;
  }
  function log(message) { }
  function isArray(value) { return Array.isArray(value); }
  function toJSON(value) { return JSON.stringify(value); }
  var __out = [];
  var emit = function (key, value) {
    __out.push([
      key === undefined ? null : key,
      value === undefined ? null : value
    ]);
  };
  var __map = (__DOUCHDB_SOURCE__);
  if (typeof __map !== "function") {
    throw new Error("map must be a function");
  }
  return function (json) {
    var docs = JSON.parse(json);
    var all = [];
    for (var i = 0; i < docs.length; i++) {
      __out = [];
      __map(docs[i]);
      all.push(__out);
    }
    return JSON.stringify(all);
  };
})()
