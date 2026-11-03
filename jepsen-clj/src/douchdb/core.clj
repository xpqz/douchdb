(ns douchdb.core
  "Run a real Jepsen linearizability test against a douchdb cluster.

  The cluster is expected to be running as Docker containers named
  douchdb-jepsen-0, douchdb-jepsen-1, ... on a user-defined Docker network.
  jepsen-clj/run.sh builds and starts that cluster."
  (:gen-class)
  (:import (knossos.model Model))
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [douchdb.client :as dc]
            [douchdb.db :as db]
            [douchdb.nemesis :as dn]
            [jepsen.checker :as checker]
            [jepsen.control :as control]
            [jepsen.core :as jepsen]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.nemesis :as nemesis]
            [jepsen.net :as net]
            [jepsen.os :as os]
            [knossos.model :as model]))

(def default-nodes
  (str/split
    (or (System/getenv "DOUCHDB_NODES")
        "douchdb-jepsen-0,douchdb-jepsen-1,douchdb-jepsen-2")
    #","))

(def cli-options
  [["-t" "--time-limit SECONDS" "Test duration in seconds"
    :default 30
    :parse-fn #(Long/parseLong %)
    :validate [#(pos? %) "must be positive"]]
   ["-c" "--concurrency N" "Concurrent client processes (workload-dependent)"
    :default 8
    :parse-fn #(Long/parseLong %)
    :validate [#(pos? %) "must be positive"]]
   ["-w" "--workload WORKLOAD" "register, contention or cas"
    :default "register"
    :validate [#{"register" "contention" "cas"} "must be register, contention or cas"]]
   [nil "--read-quorum N" "Read quorum r (default 2)"
    :default 2
    :parse-fn #(Long/parseLong %)
    :validate [#(and (>= % 1) (<= % 3)) "must be 1, 2 or 3"]]
   [nil "--write-quorum N" "Write quorum w (default 2)"
    :default 2
    :parse-fn #(Long/parseLong %)
    :validate [#(and (>= % 1) (<= % 3)) "must be 1, 2 or 3"]]
   [nil "--no-final-reads" "Skip the final read of every key"]
   ["-i" "--nemesis-interval SECONDS" "Seconds between fault and recovery"
    :default 10
    :parse-fn #(Long/parseLong %)
    :validate [#(pos? %) "must be positive"]]
   ["-f" "--fault FAULT" "kill or partition"
    :default "kill"
    :validate [#{"kill" "partition"} "must be kill or partition"]]
   ["-n" "--no-nemesis" "Run without faults"]
   [nil "--nodes NODES" "Comma-separated douchdb node names"
    :default (str/join "," default-nodes)]
   [nil "--db NAME" "Database name"
    :default (db/env "DOUCHDB_DB" "jepsen")]
   [nil "--network NAME" "Docker network containing the nodes"
    :default (db/env "DOUCHDB_NETWORK" "douchdb-jepsen-net")]
   ["-s" "--seed SEED" "Random seed"
    :parse-fn #(Long/parseLong %)]
   ["-h" "--help"]])

(defn- write-op [_ _]
  {:type :invoke :f :write :value (rand-int 5)})

(defn- cas-write-op [_ _]
  {:type :invoke :f :cas-write :value (rand-int 5)})

(defn- read-op [_ _]
  {:type :invoke :f :read})

(defn- workload-generator
  "Returns [group-size generator].

  register: one writer and one reader per key.
  contention: two writers and one reader per key, so writers race on the same
  revision and exercise conflict classification.
  cas: one If-Match writer and one reader per key."
  [workload]
  (case workload
    "register"
    [2 (gen/reserve 1 write-op read-op)]
    "contention"
    [3 (gen/reserve 2 write-op read-op)]
    "cas"
    [2 (gen/reserve 1 cas-write-op read-op)]))

(defn- make-workload
  [workload]
  (let [[group-size fgen] (workload-generator workload)]
    (independent/concurrent-generator group-size (range) (fn [_key] fgen))))

(defn- nemesis-generator
  [interval fault]
  (let [fault-op (case fault
                   "kill" {:type :info :f :kill :value :one}
                   "partition" {:type :info :f :partition :value :one})
        heal-op (case fault
                  "kill" {:type :info :f :start :value :all}
                  "partition" {:type :info :f :heal :value :all})]
    (gen/cycle
      [(gen/sleep interval)
       fault-op
       (gen/sleep interval)
       heal-op])))

(defrecord CasWriteRegister [value]
  Model
  (step [_ op]
    (let [translated (if (= :cas-write (:f op))
                       (assoc op :f :write)
                       op)
          next (model/step (model/cas-register value) translated)]
      (if (model/inconsistent? next)
        next
        (->CasWriteRegister (:value next))))))

(defn- checker
  []
  (independent/checker
    (checker/linearizable
      {:model (->CasWriteRegister nil)})))

(defn- test-map
  [{:keys [nodes time-limit concurrency nemesis-interval fault no-nemesis
           network seed workload read-quorum write-quorum no-final-reads]
    db-name :db}]
  (let [tracked (independent/track-keys (make-workload workload))
        body (cond->> tracked
               (not no-nemesis)
               (gen/nemesis (nemesis-generator nemesis-interval fault)))
        timed (gen/time-limit time-limit body)
        generator (if no-final-reads
                    timed
                    (gen/concat
                      timed
                      (independent/final-generator (constantly {:f :read}))))]
    (cond-> {:name (str "douchdb-" (if no-nemesis "no-fault" fault)
                        "-" workload "-r" read-quorum "-w" write-quorum)
             :nodes nodes
             :db-name db-name
             :read-quorum read-quorum
             :write-quorum write-quorum
             :concurrency concurrency
             :ssh {:dummy? true}
             :remote control/ssh
             :os os/noop
             :net net/noop
             :db (db/douchdb)
             :client (dc/douchdb)
             :nemesis (if no-nemesis
                        nemesis/noop
                        (dn/docker-nemesis network))
             :generator generator
             :checker (checker)
             :time-limit time-limit}
      seed (assoc :seed seed))))

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]}
        (cli/parse-opts args cli-options)]
    (cond
      (:help options)
      (do (println "Run a Jepsen linearizability test against douchdb.\n")
          (println summary)
          (System/exit 0))

      (seq errors)
      (do (binding [*out* *err*]
            (doseq [error errors] (println error)))
          (System/exit 2))

      (seq arguments)
      (do (binding [*out* *err*]
            (println "unexpected arguments:" (str/join " " arguments)))
          (System/exit 2))

      :else
      (let [nodes (->> (str/split (:nodes options) #",")
                       (map str/trim)
                       (remove str/blank?)
                       vec)
            options (assoc options :nodes nodes)
            group-size (case (:workload options)
                         "contention" 3
                         2)]
        (when-not (zero? (mod (:concurrency options) group-size))
          (binding [*out* *err*]
            (println (str "concurrency must be a multiple of " group-size
                          " for the " (:workload options) " workload")))
          (System/exit 2))
        (let [test (test-map options)]
        (try
          (let [result (jepsen/run! test)
                results (:results result)]
            (println)
            (println "Jepsen results:")
            (pp/pprint results)
            (println)
            (println (case (:valid? results)
                       true "VALID: no linearizability violations found."
                       false "INVALID: linearizability violation found."
                       "UNKNOWN: checker could not reach a verdict."))
            (System/exit (if (true? (:valid? results)) 0 1)))
          (catch Exception e
            (.printStackTrace e)
            (System/exit 1))))))))
