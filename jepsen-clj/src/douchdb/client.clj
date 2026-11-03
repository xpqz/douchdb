(ns douchdb.client
  "A Jepsen client for douchdb's CouchDB-compatible document API.

  The workload is a set of independent registers. A document is
  {_id: k, value: v}; reads use a read quorum, writes use a write quorum, and
  compare-and-set is a read followed by a conditional PUT using If-Match."
  (:require [cheshire.core :as json]
            [douchdb.db :as db]
            [douchdb.http :as http]
            [jepsen.client :as client]
            [jepsen.independent :as independent]))

(def ^:const read-quorum 2)
(def ^:const write-quorum 2)

(defn- doc-id [key]
  (str "k-" key))

(defn- doc-url [c key]
  (str (db/db-url (:node c) (:db-name c)) "/" (doc-id key)))

(defn- fetch
  "Returns {:ok true :doc doc-or-nil} or {:ok false :response response}."
  [c key]
  (let [response (http/request :get (doc-url c key)
                               {:query-params {"r" (str read-quorum)}})]
    (case (:status response)
      200 {:ok true :doc (:body response)}
      404 {:ok true :doc nil}
      {:ok false :response response})))

(defn- put-doc!
  [c key value rev if-match?]
  (let [body (cond-> {"value" value}
               rev (assoc "_id" (doc-id key) "_rev" rev))
        headers (cond-> {"Content-Type" "application/json"}
                  (and rev if-match?) (assoc "If-Match" (str "\"" rev "\"")))]
    (http/request :put (doc-url c key)
                  {:query-params {"w" (str write-quorum)}
                   :headers headers
                   :body (json/generate-string body)})))

(defn- classify-write
  [response]
  (case (:status response)
    (200 201) {:type :ok}
    202 {:type :info :error :quorum-not-met}
    (409 412) {:type :conflict}
    nil {:type :info :error (str (:error response))}
    {:type :info :error (str "unexpected status " (:status response))}))

(defn- write-register
  "Writes the register as a conditional update from the revision we just read.
  A concurrent writer can win the race; that is a failed write, not a retry
  loop that can starve under contention."
  [c key value]
  (let [{:keys [ok doc response]} (fetch c key)]
    (if-not ok
      {:type :info :error (str "read-before-write failed: " (:status response))}
      (let [rev (get doc :_rev)
            result (classify-write (put-doc! c key value rev false))]
        (if (= :conflict (:type result))
          {:type :fail :error :revision-mismatch}
          result)))))

(defn- read-register
  [c key]
  (let [{:keys [ok doc response]} (fetch c key)]
    (if ok
      {:type :ok :value (get doc :value)}
      {:type :info :error (str "read failed: " (:status response))})))

(defn- cas-register
  [c key expected new-value]
  (let [{:keys [ok doc response]} (fetch c key)]
    (if-not ok
      {:type :info :error (str "read-before-cas failed: " (:status response))}
      (let [current (get doc :value)
            rev (get doc :_rev)]
        (cond
          (not= current expected)
          {:type :fail :error :value-mismatch}

          ;; A CAS from nil can create the document. If another process created
          ;; it between our read and write, the create loses with 409.
          (nil? doc)
          (let [result (classify-write (put-doc! c key new-value nil false))]
            (if (= :conflict (:type result))
              {:type :fail :error :concurrent-create}
              result))

          :else
          (let [result (classify-write (put-doc! c key new-value rev true))]
            (if (= :conflict (:type result))
              {:type :fail :error :revision-mismatch}
              result)))))))

(defrecord DouchClient [node db-name]
  client/Client
  (open! [_ test node]
    (->DouchClient node (:db-name test)))

  (close! [_ _])

  (setup! [this _] this)

  (teardown! [_ _])

  (invoke! [this _ op]
    (let [[key value] (:value op)
          c this
          completed (fn [op result]
                      (cond-> (assoc op :type (:type result))
                        (:error result) (assoc :error (:error result))))]
      (case (:f op)
        :write (completed op (write-register c key value))
        :read (let [result (read-register c key)]
                (assoc op
                       :type (:type result)
                       :value (independent/tuple key (:value result))))
        :cas (let [[expected new-value] value]
               (completed op (cas-register c key expected new-value)))))))

(defn douchdb []
  (->DouchClient nil nil))
