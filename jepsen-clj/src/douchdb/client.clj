(ns douchdb.client
  "A Jepsen client for douchdb's CouchDB-compatible document API.

  The workload is a set of independent registers. A document is
  {_id: k, value: v}; reads use a read quorum, writes use a write quorum, and
  compare-and-set is a read followed by a conditional PUT using If-Match."
  (:require [cheshire.core :as json]
            [douchdb.db :as db]
            [douchdb.http :as http]
            [douchdb.liveness :as liveness]
            [jepsen.client :as client]
            [jepsen.independent :as independent]))

(defn- doc-id [key]
  (str "k-" key))

(defn- doc-url [c key]
  (str (db/db-url (:node c) (:db-name c)) "/" (doc-id key)))

(defn- fetch
  "Returns {:ok true :doc doc-or-nil} or {:ok false :response response}."
  [c key]
  (let [response (http/request :get (doc-url c key)
                               {:query-params {"r" (str (:read-quorum c))}})]
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
                  {:query-params {"w" (str (:write-quorum c))}
                   :headers headers
                   :body (json/generate-string body)})))

(defn- classify-write
  "A write that provably never reached the server failed; one that may have
  reached it is indeterminate."
  [response]
  (case (:status response)
    (200 201) {:type :ok}
    202 {:type :info :error :quorum-not-met}
    (409 412) {:type :conflict}
    nil (if (http/not-sent? response)
          {:type :fail :error (str "not sent: " (:error response))}
          {:type :info :error (str (:error response))})
    {:type :info :error (str "unexpected status " (:status response))}))

(defn- read-failed
  "A failed read changes nothing, and neither does a write or CAS whose
  preliminary read failed, because the PUT was never sent. Both are :fail."
  [what response]
  {:type :fail
   :error (str what " failed: "
               (or (:status response) (:error response)))})

(defn- write-register
  "Writes the register as a conditional update from the revision we just read.
  A concurrent writer can win the race; that is a failed write, not a retry
  loop that can starve under contention."
  [c key value]
  (let [{:keys [ok doc response]} (fetch c key)]
    (if-not ok
      (read-failed "read-before-write" response)
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
      (read-failed "read" response))))

(defn- cas-register
  [c key expected new-value]
  (let [{:keys [ok doc response]} (fetch c key)]
    (if-not ok
      (read-failed "read-before-cas" response)
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

(defn- cas-write-register
  "Reads the current value and CASes it to new-value with If-Match. A lost race
  is a failed write; the conditional PUT cannot have changed the document."
  [c key new-value]
  (let [{:keys [ok doc response]} (fetch c key)]
    (if-not ok
      (read-failed "read-before-cas" response)
      (let [rev (get doc :_rev)]
        (if (nil? doc)
          (let [result (classify-write (put-doc! c key new-value nil false))]
            (if (= :conflict (:type result))
              {:type :fail :error :concurrent-create}
              result))
          (let [result (classify-write (put-doc! c key new-value rev true))]
            (if (= :conflict (:type result))
              {:type :fail :error :revision-mismatch}
              result)))))))

(defrecord DouchClient [node db-name read-quorum write-quorum]
  client/Client
  (open! [_ test node]
    (liveness/mark-origin!)
    (->DouchClient node (:db-name test)
                   (:read-quorum test 2)
                   (:write-quorum test 2)))

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
               (completed op (cas-register c key expected new-value)))
        :cas-write (completed op (cas-write-register c key value))))))

(defn douchdb []
  (->DouchClient nil nil nil nil))
