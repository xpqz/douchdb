(ns douchdb.db
  "Jepsen DB lifecycle for a douchdb cluster.

  The nodes are Docker container names. Jepsen's control layer is configured
  with a dummy SSH transport: setup and teardown talk to douchdb over HTTP, and
  failures are injected by douchdb.nemesis through the Docker socket."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [douchdb.http :as http]
            [jepsen.db :as jdb]))

(defn env
  [key default]
  (or (System/getenv key) default))

(defn db-name []
  (env "DOUCHDB_DB" "jepsen"))

(defn port []
  (env "DOUCHDB_PORT" "5984"))

(defn node-url
  "Returns a base URL for a node. Accepts a bare hostname, a host:port pair,
  or an already-qualified http(s) URL."
  [node]
  (cond
    (re-find #"^https?://" node) node
    (str/includes? node ":") (str "http://" node)
    :else (str "http://" node ":" (port))))

(defn db-url
  ([node] (db-url node (db-name)))
  ([node name] (str (node-url node) "/" name)))

(defn root-url [node]
  (node-url node))

(defn reachable?
  [node]
  (= 200 (:status (http/request :get (root-url node)))))

(defn wait-ready
  "Waits until every node answers GET / with 200."
  ([nodes] (wait-ready nodes 60000))
  ([nodes timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (let [down (remove reachable? nodes)]
         (cond
           (empty? down) true
           (< (System/currentTimeMillis) deadline)
           (do (Thread/sleep 250) (recur))
           :else
           (throw (ex-info (str "nodes did not become ready: " (pr-str down))
                           {:nodes down}))))))))

(defn db-exists?
  [node name]
  (= 200 (:status (http/request :get (str (node-url node) "/" name)))))

(defn wait-for-db
  "Waits until every node agrees on whether the database exists."
  [nodes name exists? timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [wrong (remove #(= exists? (db-exists? % name)) nodes)]
        (cond
          (empty? wrong) true
          (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 250) (recur))
          :else
          (throw (ex-info (str "database " name
                               (if exists? " did not appear on " " did not disappear from ")
                               (pr-str wrong))
                          {:nodes wrong :exists? exists?})))))))

(defn ensure-db!
  [node name]
  (let [response (http/request :put (str (node-url node) "/" name))]
    (when-not (#{200 201 202 412} (:status response))
      (throw (ex-info (str "could not create database " name " on " node)
                      {:response response})))))

(defn delete-db!
  [node name]
  (let [response (http/request :delete (str (node-url node) "/" name))]
    (when-not (#{200 202 404} (:status response))
      (throw (ex-info (str "could not delete database " name " on " node)
                      {:response response})))))

(defrecord DouchDB []
  jdb/DB
  (setup! [_ test node]
    (let [nodes (:nodes test)
          name (or (:db-name test) (db-name))]
      (wait-ready nodes)
      (ensure-db! node name)
      (wait-for-db nodes name true 60000)
      (info "douchdb database" name "ready on all nodes")))

  (teardown! [_ test node]
    (let [nodes (:nodes test)
          name (or (:db-name test) (db-name))]
      (try
        (delete-db! node name)
        (wait-for-db nodes name false 60000)
        (catch Exception e
          (warn e "database teardown did not complete cleanly"))))))

(defn douchdb []
  (->DouchDB))
