(ns douchdb.liveness
  "Runtime liveness samples and checker.

  A background future records Docker container state and HTTP health while the
  workload runs. The checker compares outages with the nemesis history, so a
  node that dies without a kill/partition is a failure, while expected fault
  windows are allowed. It also bounds indeterminate operations and reports
  throughput and latency gaps."
  (:require [cheshire.core :as json]
            [clojure.java.shell :as shell]
            [clojure.pprint :as pp]
            [clojure.tools.logging :refer [warn]]
            [douchdb.db :as db]
            [douchdb.http :as http]
            [jepsen.checker :as checker]
            [jepsen.store :as store]
            [jepsen.util :as util]))

(defonce ^:private origin
  ;; Jepsen history times are relative to an origin it fixes after database
  ;; setup, so a clock started before the run would skew every sample against
  ;; the nemesis history by the setup time.
  (atom nil))

(defn mark-origin!
  "Records Jepsen's relative-time origin. Call it from code that runs inside
  the test case, where the origin is bound (client open! does)."
  []
  (when-let [o util/*relative-time-origin*]
    (compare-and-set! origin nil o)))

(defn- docker-state
  [node]
  (try
    (let [{:keys [exit out err]}
          (shell/sh "docker" "inspect" "--format" "{{json .State}}" node)]
      (if (zero? exit)
        (let [state (json/parse-string out true)]
          {:running (boolean (:Running state))
           :status (:Status state)
           :exit-code (:ExitCode state)
           :restart-count (:RestartCount state)})
        {:error (str "docker inspect failed: " err)}))
    (catch Exception e
      {:error (.getMessage e)})))

(defn- health
  [node]
  (let [response (http/request :get (db/root-url node)
                               {:socket-timeout 1000 :connection-timeout 1000})]
    {:status (:status response)
     :error (some-> (:error response) .getMessage)}))

(defn start!
  "Starts sampling. Returns a monitor map that must be passed to stop!."
  [nodes interval-ms]
  (let [samples (atom [])
        stop (atom false)
        future (future
                 (try
                   (while (not @stop)
                     (let [now (util/linear-time-nanos)]
                       (swap! samples conj
                              {:nanos now
                               :nodes (into {}
                                            (map (fn [node]
                                                   [node (merge (docker-state node)
                                                                {:health (health node)})]))
                                            nodes)}))
                     (Thread/sleep interval-ms))
                   (catch InterruptedException _)
                   (catch Exception e
                     (warn e "liveness monitor stopped"))))]
    {:nodes (vec nodes)
     :interval-ms interval-ms
     :samples samples
     :stop stop
     :future future}))

(defn stop!
  [{:keys [stop future]}]
  (when stop (reset! stop true))
  (when future (future-cancel future)))

(defn- fault-intervals
  "Returns {node [[start end] ...]} from nemesis info operations. Times are in
  nanoseconds, like Jepsen history times and monitor samples."
  [history nodes]
  (let [grace (* 3 1000 1000 1000)]
    (loop [ops (seq history)
           down (zipmap nodes (repeat nil))
           intervals (zipmap nodes (repeat []))]
      (if-let [op (first ops)]
        (let [value (:value op)
              completed? (vector? value)
              targets (when (and completed?
                                 (contains? #{:kill :partition} (:f op)))
                        (second value))
              heals (when (and completed?
                               (contains? #{:start :heal :stop} (:f op)))
                      (case (:f op)
                        :start (second value)
                        (:heal :stop) nodes))
              time (:time op)
              down (reduce (fn [m node] (assoc m node (- time grace))) down targets)
              intervals (reduce (fn [m node]
                                  (if-let [start (get down node)]
                                    (update m node conj [start (+ time grace)])
                                    m))
                                intervals
                                (or heals []))
              down (reduce (fn [m node] (assoc m node nil)) down (or heals []))]
          (recur (next ops) down intervals))
        (reduce-kv (fn [m node start]
                     (if start
                       (update m node conj [start Long/MAX_VALUE])
                       m))
                   intervals
                   down)))))

(defn- expected-down?
  [intervals node time]
  (some (fn [[start end]] (<= start time end)) (get intervals node)))

(defn checker
  "Returns a Jepsen checker over monitor samples. Fails on an unexpected node
  outage or excessive indeterminate operations."
  [{:keys [samples max-indeterminate-pct liveness-interval-ms]}]
  (reify checker/Checker
    (check [_ test history opts]
      (let [grace (* 2 1000 1000 1000)
            history-end (if (seq history) (apply max (map :time history)) 0)
            base @origin
            samples (when base
                      (->> @samples
                           (map #(-> % (assoc :time (- (:nanos %) base))
                                     (dissoc :nanos)))
                           (filter #(<= 0 (:time %) (+ history-end grace)))))
            nodes (:nodes test)
            intervals (fault-intervals history nodes)
            downs (for [{:keys [time nodes]} samples
                        [node state] nodes
                        :let [{:keys [running health restart-count]} state
                              down? (or (false? running)
                                        (not= 200 (:status health)))]
                        :when (and down? (not (expected-down? intervals node time)))]
                    {:time time :node node :health (:status health)
                     :running running :restart-count restart-count})
            unexpected-restarts (for [[previous current] (partition 2 1 samples)
                                      [node state] (:nodes current)
                                      :let [old (get (:nodes previous) node)]
                                      :when (and old
                                                 (> (or (:restart-count state) 0)
                                                    (or (:restart-count old) 0))
                                                 (not (expected-down? intervals node (:time current))))]
                                  {:time (:time current) :node node})
            client-ops (filter #(and (contains? #{:ok :fail :info} (:type %))
                                     (not (contains? #{:kill :start :partition :heal :stop}
                                                     (:f %))))
                               history)
            ok-ops (filter #(= :ok (:type %)) client-ops)
            indeterminate (count (filter #(= :info (:type %)) client-ops))
            total (count client-ops)
            indeterminate-pct (if (zero? total) 0.0 (* 100.0 (/ indeterminate total)))
            times (sort (map :time ok-ops))
            max-gap-ns (if (< (count times) 2)
                         0
                         (apply max (map - (rest times) times)))
            duration-ns (if (> total 0)
                          (- (apply max (map :time client-ops))
                             (apply min (map :time client-ops)))
                          1)
            throughput (if (pos? duration-ns)
                         (* 1e9 (/ (count ok-ops) duration-ns))
                         0.0)
            valid? (boolean (and (seq samples)
                                 (empty? downs)
                                 (empty? unexpected-restarts)
                                 (<= indeterminate-pct max-indeterminate-pct)))]
        (store/with-out-file test ["liveness.edn"]
          (pp/pprint {:interval-ms liveness-interval-ms
                      :samples samples}))
        (cond-> {:valid? valid?
                 :unexpected-down downs
                 :unexpected-restarts unexpected-restarts
                 :samples (count samples)
                 :indeterminate-pct indeterminate-pct
                 :max-indeterminate-pct max-indeterminate-pct
                 :throughput-ops-per-sec throughput
                 :max-ok-gap-ms (/ max-gap-ns 1e6)}
          (nil? base) (assoc :error "Jepsen's time origin was never recorded"))))))
