// Search index wrapper shared by the native engine (src/search.rs) and the
// WASM runner (wasm/qjs-runner). The source placeholder below is replaced by
// the user's index function; the wrapper takes a JSON array of documents and
// returns a JSON array holding, per document, the [name, value] pairs it
// indexed. Document ids come from the host, never from the document object.
(function () {
  var __out = [];
  function log(message) { }
  var index = function (name, value, options) {
    if (value === undefined || value === null) return;
    __out.push([String(name), value]);
  };
  var __index = (__DOUCHDB_SOURCE__);
  if (typeof __index !== "function") {
    throw new Error("index must be a function");
  }
  return function (json) {
    var docs = JSON.parse(json);
    var all = [];
    for (var i = 0; i < docs.length; i++) {
      __out = [];
      __index(docs[i]);
      all.push(__out);
    }
    return JSON.stringify(all);
  };
})()
