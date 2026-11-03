(ns douchdb.http
  "Small HTTP helpers shared by the Jepsen DB and client implementations."
  (:require [clj-http.client :as http]))

(def ^:dynamic *timeout-ms*
  "Per-request socket timeout. Individual requests may override it."
  5000)

(defn request
  "Issues an HTTP request and returns the clj-http response map. Transport
  errors are returned as {:status nil :error throwable} so callers can turn
  them into indeterminate Jepsen operations."
  ([method url]
   (request method url {}))
  ([method url opts]
   (try
     (http/request
       (merge {:method method
               :url url
               :throw-exceptions false
               :as :json
               :socket-timeout *timeout-ms*
               :connection-timeout 2000
               :headers {"Accept" "application/json"}}
              opts))
     (catch Exception e
       {:status nil :error e}))))

(defn status [response]
  (:status response))

(defn ok? [response]
  (<= 200 (or (:status response) 0) 299))
