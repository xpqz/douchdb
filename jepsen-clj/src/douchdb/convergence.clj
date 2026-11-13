(ns douchdb.convergence
  "What CouchDB promises while the network is split, and a checker for it.

  A CouchDB cluster is not a single copy of the data during a partition. A
  write that reaches fewer than w copies is still stored (202), a writer can
  read a stale revision and start a conflict branch, and different copies can
  answer reads differently until anti-entropy catches up. Reads can therefore
  go backwards, which a linearizability checker rightly rejects. CouchDB
  promises less, and this checker holds douchdb to exactly that:

  - no invented data: every read returns nil or a value some write that was
    not known to have failed tried to store, invoked before the read returned;
  - acknowledged writes survive: the revision of every 201 write is in the
    document's final revision tree, as the winner, an ancestor or a conflict;
  - convergence: once the network heals, every node holds the same leaves, so
    every node picks the same winner."
  (:require [douchdb.db :as db]
            [douchdb.http :as http]
            [jepsen.checker :as checker]))

(def ^:dynamic *deadline-ms*
  "How long one key may take to converge after the final heal. Anti-entropy
  runs every second in the test cluster; this bounds a key that never does."
  120000)

(defn- revisions
  "The revision ids in one open_revs entry's history, newest first."
  [doc]
  (let [{:keys [start ids]} (:_revisions doc)]
    (if (and start (seq ids))
      (map-indexed (fn [i id] (str (- start i) "-" id)) ids)
      [(:_rev doc)])))

(defn- node-state
  "One node's own copy of a document: its leaves and every revision in its
  tree. r=1 makes the node answer from its local replica; with n equal to the
  node count, every node holds every document. nil if the node did not
  answer."
  [node db-name doc-id]
  (let [response (http/request :get (str (db/db-url node db-name) "/" doc-id)
                               {:query-params {"open_revs" "all"
                                               "revs" "true"
                                               "r" "1"}})]
    (case (:status response)
      200 (let [docs (keep :ok (:body response))]
            {:leaves (into (sorted-set) (map :_rev) docs)
             :revs (into #{} (mapcat revisions) docs)})
      404 {:leaves (sorted-set) :revs #{}}
      nil)))

(defn final-state
  "Polls every node until they all answer with the same leaves for doc-id, or
  the deadline passes. Returns {:converged? bool :leaves ... :revs ...
  :nodes {node leaves}} with :revs the union of every node's tree."
  [nodes db-name doc-id]
  (let [deadline (+ (System/currentTimeMillis) *deadline-ms*)]
    (loop []
      (let [states (into {} (map (fn [n] [n (node-state n db-name doc-id)])) nodes)
            answered (vals states)
            converged? (and (every? some? answered)
                            (apply = (map :leaves answered)))]
        (if (or converged? (>= (System/currentTimeMillis) deadline))
          {:converged? converged?
           :leaves (some-> (first answered) :leaves vec)
           :revs (into #{} (mapcat :revs) (filter some? answered))
           :nodes (into (sorted-map) (map (fn [[n s]] [n (some-> s :leaves vec)])) states)}
          (do (Thread/sleep 200) (recur)))))))

(defn- write?
  [op]
  (#{:write :cas-write} (:f op)))

(defn- phantom-reads
  "Ok reads whose non-nil value no write could have produced: no write of that
  value was invoked before the read returned, or every such write is known to
  have failed."
  [history]
  (let [failed (into #{} (comp (filter #(and (= :fail (:type %)) (write? %)))
                               (map :index))
                     history)
        ;; Pair each write invocation with its completion by process.
        attempts (loop [ops history, open {}, out []]
                   (if-let [op (first ops)]
                     (cond
                       (and (write? op) (= :invoke (:type op)))
                       (recur (rest ops) (assoc open (:process op) op) out)

                       (and (write? op) (contains? open (:process op)))
                       (recur (rest ops) (dissoc open (:process op))
                              (if (contains? failed (:index op))
                                out
                                (conj out (get open (:process op)))))

                       :else (recur (rest ops) open out))
                     (into out (vals open))))
        first-invoke (reduce (fn [m op]
                               (update m (:value op) (fnil min Long/MAX_VALUE) (:index op)))
                             {} attempts)]
    (->> history
         (filter #(and (= :ok (:type %)) (= :read (:f %)) (some? (:value %))))
         (remove #(< (get first-invoke (:value %) Long/MAX_VALUE) (:index %)))
         (mapv #(select-keys % [:index :process :value])))))

(defn checker
  "A per-key checker for the promises above. Wrap it in independent/checker.
  It needs the :final-state op that the partition generator issues for every
  key after the last heal."
  []
  (reify checker/Checker
    (check [_ _ history _]
      (let [final (->> history
                       (filter #(and (= :final-state (:f %))
                                     (integer? (:process %))
                                     (#{:ok :info} (:type %))
                                     (map? (:value %))))
                       last)
            state (:value final)
            acked (keep #(when (and (= :ok (:type %)) (write? %)) (:rev %)) history)
            lost (if state (vec (remove (set (:revs state)) acked)) [])
            phantoms (phantom-reads history)
            converged? (boolean (:converged? state))]
        {:valid? (cond
                   (or (seq phantoms) (and state (seq lost))) false
                   (nil? state) :unknown
                   :else converged?)
         :acknowledged-writes (count acked)
         :phantom-reads phantoms
         :lost-writes lost
         :converged? converged?
         :final-leaves (:leaves state)
         :node-leaves (when-not converged? (:nodes state))}))))
