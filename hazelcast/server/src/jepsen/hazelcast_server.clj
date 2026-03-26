(ns jepsen.hazelcast-server
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (com.hazelcast.core Hazelcast)
           (com.hazelcast.config.cp FencedLockConfig
                                    SemaphoreConfig
                                    CPSubsystemConfig
                                    CPMapConfig)
           (com.hazelcast.config Config
                                 MapConfig
                                 MergePolicyConfig
                                 SplitBrainProtectionConfig)
           ))

(def opt-spec
[["-m" "--members MEMBER-LIST" "Comma-separated list of peers to connect to"
  :parse-fn (fn [m]
                (str/split m #"\s*,\s*"))],
 [nil "--persistent PERSISTENT" "Is persistence enabled?"],
 [nil "--license LICENSE" "Hazelcast Enterprise License"]])


(defn prepare-cp-subsystem-config
  "Prepare Hazelcast CPSubsystemConfig"
  [config members persistent]
  (let [cpSubsystemConfig (.getCPSubsystemConfig config)
        raftAlgorithmConfig (.getRaftAlgorithmConfig cpSubsystemConfig)
        semaphoreConfig (SemaphoreConfig. "jepsen.cpSemaphore" false, 0)
        lockConfig1 (FencedLockConfig. "jepsen.cpLock1" 1)
        lockConfig2 (FencedLockConfig. "jepsen.cpLock2" 2)
        cpMapConfig (doto (CPMapConfig. "jepsen.cp.map.purge")
                              (.setPurgeEnabled true))
        ]

       (.setLeaderElectionTimeoutInMillis raftAlgorithmConfig 1000)
       (.setLeaderHeartbeatPeriodInMillis raftAlgorithmConfig 1500)
       (.setCommitIndexAdvanceCountToSnapshot raftAlgorithmConfig 250)
       (.setFailOnIndeterminateOperationState cpSubsystemConfig true)

       (.setCPMemberCount cpSubsystemConfig (count members))
       (.setSessionHeartbeatIntervalSeconds cpSubsystemConfig 5)
       (.setSessionTimeToLiveSeconds cpSubsystemConfig 300)

       (.setPersistenceEnabled cpSubsystemConfig (= persistent "true"))

       (.addSemaphoreConfig cpSubsystemConfig semaphoreConfig)
       (.addLockConfig cpSubsystemConfig lockConfig1)
       (.addLockConfig cpSubsystemConfig lockConfig2)
       (.addCPMapConfig cpSubsystemConfig cpMapConfig)

       cpSubsystemConfig))

(defn -main
  "Go go go"
  [& args]
  (let [{:keys [options
                arguments
                summary
                errors]} (cli/parse-opts args opt-spec)
        config  (Config.)
        members (:members options)

        _ (.setProperty config "hazelcast.enterprise.license.key" (:license options))

        ; Timeouts
        _ (.setProperty config "hazelcast.client.max.no.heartbeat.seconds" "90")
        _ (.setProperty config "hazelcast.heartbeat.interval.seconds" "1")
        _ (.setProperty config "hazelcast.max.no.heartbeat.seconds" "5")
        _ (.setProperty config "hazelcast.operation.call.timeout.millis" "5000")
        _ (.setProperty config "hazelcast.wait.seconds.before.join" "0")
        _ (.setProperty config "hazelcast.merge.first.run.delay.seconds" "1")
        _ (.setProperty config "hazelcast.merge.next.run.delay.seconds" "1")
        _ (.setProperty config "hazelcast.cp.snapshot.chunk.max.size.mb" "1")

        ; Network config
        _       (.. config getNetworkConfig getJoin getMulticastConfig
                    (setEnabled false))
        tcp-ip  (.. config getNetworkConfig getJoin getTcpIpConfig)
        _       (doseq [member members]
                  (.addMember tcp-ip member))
        _       (.setEnabled tcp-ip true)

        ; prepare the CP subsystem
        _ (prepare-cp-subsystem-config config members (:persistent options))

        ; Quorum for split-brain protection
        quorum (doto (SplitBrainProtectionConfig.)
                 (.setName "majority")
                 (.setEnabled true)
                 (.setMinimumClusterSize (inc (int (Math/floor
                                       (/ (inc (count (:members options)))
                                          2))))))
        _ (.addSplitBrainProtectionConfig config quorum)

        ; Queues
        queue-config (doto (.getQueueConfig config "jepsen.queue")
                       (.setName "jepsen.queue")
                       (.setBackupCount 2)
                       (.setSplitBrainProtectionName "majority"))
        _ (.addQueueConfig config queue-config)

        merge-policy-config (doto (MergePolicyConfig.)
                          (.setPolicy
                            "jepsen.hazelcast_server.SetUnionMergePolicy"))

        ; Maps with CRDTs
        crdt-map-config (doto (MapConfig.)
                    (.setName "jepsen.crdt-map")
                    (.setMergePolicyConfig merge-policy-config))
        _ (.addMapConfig config crdt-map-config)

        ; Maps without CRDTs
        map-config (doto (MapConfig.)
                     (.setName "jepsen.map")
                     (.setSplitBrainProtectionName "majority"))
        _ (.addMapConfig config map-config)

        ; Launch
        hc      (Hazelcast/newHazelcastInstance config)]
    (loop []
      (Thread/sleep 1000))))
