(ns douchdb.nemesis
  "Docker-backed fault injection for the local Jepsen cluster.

  Jepsen's standard nemeses expect SSH control and iptables. This namespace
  drives the node containers directly through the Docker CLI, which is mounted
  into the Jepsen runner container."
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [douchdb.db :as db]
            [jepsen.nemesis :as nemesis]))

(defn- docker
  [& args]
  (let [{:keys [exit out err]} (apply shell/sh "docker" args)]
    (if (zero? exit)
      (str/trim out)
      (throw (ex-info (str "docker " (str/join " " args) " failed")
                      {:exit exit :out out :err err})))))

(defn- docker-ok?
  [& args]
  (zero? (:exit (apply shell/sh "docker" args))))

(defn- node-container
  [node]
  node)

(defn- choose
  [nodes value]
  (let [nodes (vec nodes)]
    (case value
      :one [(rand-nth nodes)]
      :all nodes
      :majority (take (inc (quot (count nodes) 2)) (shuffle nodes))
      :minority (take (quot (count nodes) 2) (shuffle nodes))
      nil [(rand-nth nodes)]
      (if (coll? value) value [value]))))

(defn- start-node!
  [node]
  (docker "start" (node-container node)))

(defn- kill-node!
  [node]
  (docker-ok? "kill" (node-container node)))

(defn- disconnect-node!
  [network node]
  (docker-ok? "network" "disconnect" network (node-container node)))

(defn- connect-node!
  [network node]
  (docker-ok? "network" "connect" network (node-container node)))

(defrecord DockerNemesis [network]
  nemesis/Nemesis
  (setup! [this test]
    (doseq [node (:nodes test)]
      (start-node! node)
      (connect-node! network node))
    (db/wait-ready (:nodes test))
    this)

  (invoke! [_ test op]
    (let [nodes (:nodes test)
          targets (choose nodes (:value op))]
      (case (:f op)
        :kill
        (do (doseq [node targets] (kill-node! node))
            (info "killed" (pr-str targets))
            (assoc op :value [:killed targets]))

        :start
        (do (doseq [node targets] (start-node! node))
            (db/wait-ready targets 60000)
            (info "started" (pr-str targets))
            (assoc op :value [:started targets]))

        :partition
        (do (doseq [node targets] (disconnect-node! network node))
            (info "disconnected" (pr-str targets) "from" network)
            (assoc op :value [:disconnected targets]))

        :heal
        (do (doseq [node nodes] (connect-node! network node))
            (db/wait-ready nodes 60000)
            (info "healed network")
            (assoc op :value :healed))

        :stop
        (do (doseq [node nodes] (connect-node! network node))
            (assoc op :value :healed))

        (assoc op :value [:ignored (:f op)]))))

  (teardown! [this test]
    (doseq [node (:nodes test)]
      (try
        (start-node! node)
        (connect-node! network node)
        (catch Exception e
          (warn e "could not restore node" node))))
    (try
      (db/wait-ready (:nodes test) 60000)
      (catch Exception e
        (warn e "cluster did not become ready during nemesis teardown"))))

  nemesis/Reflection
  (fs [_] #{:kill :start :partition :heal :stop}))

(defn docker-nemesis
  [network]
  (->DockerNemesis network))
