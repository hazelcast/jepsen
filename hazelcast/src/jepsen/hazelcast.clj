(ns jepsen.hazelcast
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.tools.logging :refer [info]]
            [clojure.tools.logging :refer [warn]]
            [clojure.string :as str]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io]
            [knossos.model :as model]
            [jepsen [checker :as checker]
             [cli :as cli]
             [client :as client]
             [core :as jepsen]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [independent :as independent]
             [nemesis :as nemesis]
             [tests :as tests]
             [reconnect :as rc]
             [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian])
  (:import (java.util.concurrent TimeUnit)
           (java.time Duration)
           (com.hazelcast.client.config ClientConfig)
           (com.hazelcast.config Config)
           (com.hazelcast.config.cp CPMapConfig)
           (com.hazelcast.client HazelcastClient)
           (com.hazelcast.core Hazelcast HazelcastInstance)
           (knossos.model Model)
           (java.util UUID)
           (java.io IOException)))

(defn start-stop
  "A generator which emits a start after a t1 second delay, and then a stop
  after a t2 second delay."
  [t1 t2]
  (cycle [(gen/sleep t1)
               {:type :info :f :start}
               (gen/sleep t2)
               {:type :info :f :stop}]))

(def local-server-dir
  "Relative path to local server project directory"
  "server")

(def local-server-jar
  "Relative to server fat jar"
  (str local-server-dir "/target/hazelcast-server.jar"))

(def dir
  "Remote path for hazelcast stuff"
  "/opt/hazelcast")

(def jar
  "Full path to Hazelcast server jar on remote nodes."
  (str dir "/server.jar"))

(def pid-file (str dir "/server.pid"))
(def log-file (str dir "/server.log"))
(def data-dir (str dir "/cp-data"))

(def reentrant-lock-acquire-count 2)
(def num-permits 2)
(def invalid-fence 0)
(def purgeable-cp-map-name "jepsen.cp.map.purge")


(defn build-server!
  "Ensures the server jar is ready"
  [test node]
  (when (= node (jepsen/primary test))
    (when-not (.exists (io/file local-server-jar))
      (info "Building server")
      (let [{:keys [exit out err]} (sh "lein" "uberjar" :dir "server")]
        (info out)
        (info err)
        (info exit)
        (assert (zero? exit))))))

(defn install!
  "Installs the server on remote nodes."
  []
  (c/cd dir
    (c/exec :mkdir :-p dir)
    (c/upload (.getCanonicalPath (io/file local-server-jar))
              jar)))

(defn start!
  "Launch hazelcast server"
  [test node]
  (c/cd dir
        (cu/start-daemon!
          {:chdir dir
           :logfile log-file
           :pidfile pid-file}
          "/usr/bin/java"
          :-jar jar
          :--license (:license test)
          :--persistent (:persistent test)
          :--members (->> (:nodes test)
                          (map cn/ip)
                          (str/join ",")))))

(defn stop!
  "Kill hazelcast server"
  [test node]
  (c/cd dir
        (c/su
          (cu/stop-daemon! pid-file))))

(defn db
  "Installs and runs hazelcast nodes"
  []
  (reify db/DB
    (setup! [_ test node]
      (build-server! test node)
      (jepsen/synchronize test)
      (debian/install [:openjdk-17-jdk-headless])
      (install!)
      (start! test node)
      (Thread/sleep 15000))

    (teardown! [_ test node]
      (stop! test node)
      (c/su
        (c/exec :rm :-rf log-file pid-file data-dir)))

    db/LogFiles
    (log-files [_ test node]
      [log-file])))

(defn ^HazelcastInstance connect
  "Creates a hazelcast client for the given node."
  [node cp-direct-to-leader-routing]
  (let [config (ClientConfig.)
        ; Global op timeouts
        _ (.setProperty config "hazelcast.client.heartbeat.interval" "1000")
        _ (.setProperty config "hazelcast.client.heartbeat.timeout" "5000")
        _ (.setProperty config "hazelcast.client.invocation.timeout.seconds" "5")
        ; The current Jepsen framework version several times instantiates the client
        ; with the same name, which brings to error
;        _ (.setInstanceName config node)
        ; Enable or disable CPDirectToLeaderRouting
        _ (.setCPDirectToLeaderRoutingEnabled config (Boolean/parseBoolean cp-direct-to-leader-routing))

        net (doto (.getNetworkConfig config)
              ; Don't retry operations when network fails (!?)
              (.setRedoOperation false)
              ; Don't use a local cache of the partition map
              (.setSmartRouting false))
        _ (info :net net)

        _ (doto (.getConnectionRetryConfig (.getConnectionStrategyConfig config))
          ; Try reconnecting indefinitely
          (.setClusterConnectTimeoutMillis Long/MAX_VALUE))

        ; Only talk to our node (the client's smart and will try to talk to
        ; everyone, but we're trying to simulate clients in different network
        ; components here)
        ; Connect to our node
        _ (.addAddress net (into-array String [node]))]
    (HazelcastClient/newHazelcastClient config)))

(defn connect-lite-member
  ^HazelcastInstance
  [node test]
  (let [config (Config.)
        network (.getNetworkConfig config)
        join (.getJoin network)
        tcp-ip (.getTcpIpConfig join)
        cp-subsystem (.getCPSubsystemConfig config)

        ;; CP Map config
        cp-map-config (doto (CPMapConfig. purgeable-cp-map-name)
                        (.setPurgeEnabled true))]

    ;; CP subsystem config
    (.setCPMemberCount cp-subsystem 5)
    (.addCPMapConfig cp-subsystem cp-map-config)

    ;; Lite member (NOT CP eligible)
    (.setLiteMember config true)

    ;; Disable multicast
    (.setEnabled (.getMulticastConfig join) false)

    ;; Enable TCP/IP join
    (.setEnabled tcp-ip true)
    (doseq [n (:nodes test)]
      (.addMember tcp-ip (cn/ip n)))

    (.setProperty config "hazelcast.shutdownhook.enabled" "false")
    (.setProperty config "hazelcast.enterprise.license.key" (:license test))

    (info "Starting Hazelcast LITE MEMBER on" node)
    (Hazelcast/newHazelcastInstance config)))


(defrecord PurgeableCPMapModel [state]
  Model
  (step [this op]
    ;; 1 Ignore non-completed ops
    (if (not= :ok (:type op))
      this

      (case (:f op)

        :put
        ;; 2 Writes update model state
        (PurgeableCPMapModel.
          (assoc state (:key op) (:value op)))

        :get
        ;; 3 Reads validate against model state
        (let [expected (get state (:key op))]
          (if (= expected (:value op))
            this
            (let [inc (model/inconsistent
                        (str "expected " expected ", got " (:value op)))]
              ;; 4 Attach the real op to the inconsistent model
              (assoc inc :op op))))

        :purge
        ;; 5 Purge resets state
        (PurgeableCPMapModel. {})

        ;; default: ignore unknown ops
        this))))

(defn purgeable-cp-map-model []
  (PurgeableCPMapModel. {}))

(defn cp-map-purge-client
  ([]
   (cp-map-purge-client nil nil))
  ([conn cp-map]
   (reify client/Client

     (open! [_ test node]
       (let [conn (connect-lite-member node test)
             cp-map (.getMap (.getCPSubsystem conn) purgeable-cp-map-name)]
         (cp-map-purge-client conn cp-map)))

     (setup! [_ test]
       ;; Populate CP map before the test starts
       (doseq [i (range 1000)]
         (.set cp-map (str "key-" i) i)))

     (invoke! [_ test op]
       (try
         (case (:f op)

           :put
           (do
             (.set cp-map (:key op) (:value op))
             (assoc op :type :ok))

           :get
             (assoc op :type :ok
                        :value (.get cp-map (:key op)))

           :purge
           (let [purged-count
                 (.join
                   (.purgeCPMap
                     (.getCPDataStructureManagementService
                       (.getCPSubsystem conn))
                     purgeable-cp-map-name
                     (Duration/ofSeconds 0)))]
             (assoc op :type :ok :value purged-count))

           (assoc op :type :fail :error :unknown-op))

         (catch Exception e
           (assoc op :type :fail :error (.getMessage e)))))

     (teardown! [_ test]
       (.shutdown conn))

     (close! [_ test]
       (.shutdown conn))

     client/Reusable
     (reusable? [_ test] true))))

(def purge-generator
  (->> (fn []
         (let [k (str "key-" (rand-int 2000))
               v (rand-int 100000)]
           (gen/mix
            [{:type :invoke :f :put :key k :value v}
             {:type :invoke :f :purge}
             {:type :invoke :f :get :key k}])))
       gen/each-thread
       (gen/stagger 0.3)
       (gen/limit 100)))

(defn create-atomic-long
  "Creates a new CP based AtomicLong"
  [client name]
  (.getAtomicLong (.getCPSubsystem client) name))


(defn create-atomic-reference
  "Creates a new CP based AtomicReference"
  [client name]
  (.getAtomicReference (.getCPSubsystem client) name))

(defn create-cp-map
  "Creates a new CPMap"
  [client name]
  (.getMap (.getCPSubsystem client) name))


(defn atomic-long-id-client
  "Generates unique IDs using a CP AtomicLong"
  [conn atomic-long cp-direct-to-leader-routing]
  (reify client/Client
    (open! [_ test node]
      (let [conn (connect node cp-direct-to-leader-routing)]
        (atomic-long-id-client conn (create-atomic-long conn "jepsen.atomic-long") cp-direct-to-leader-routing)))

    (setup! [this test]
        "Called to set up database state for testing.")

    (invoke! [this test op]
      (assert (= (:f op) :generate))
      (assoc op :type :ok, :value (.incrementAndGet atomic-long)))

    (teardown! [this test]
      (.shutdown conn))

    (close! [this test]
      (.shutdown conn))

    client/Reusable
    (reusable? [this test]
               true)
  ))

(defn cas-long-client
  "A CAS register using a CP AtomicLong"
  [conn atomic-long cp-direct-to-leader-routing]
  (reify client/Client
    (open! [_ test node]
      (let [conn (connect node cp-direct-to-leader-routing)]
        (cas-long-client conn (create-atomic-long conn "jepsen.cas-long") cp-direct-to-leader-routing)))

    (setup! [this test]
        "Called to set up database state for testing.")

    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (.get atomic-long))
        :write (do (.set atomic-long (:value op))
                   (assoc op :type :ok))
        :cas (let [[currentV newV] (:value op)]
               (if (.compareAndSet atomic-long currentV newV)
                 (assoc op :type :ok)
                 (assoc op :type :fail :error :cas-failed)))))

    (teardown! [this test]
       (.shutdown conn))

    (close! [this test]
       (.shutdown conn))

    client/Reusable
    (reusable? [this test]
               true)
  ))

(defn cas-reference-client
  "A CAS register using a CP AtomicReference"
  [conn atomic-ref cp-direct-to-leader-routing]
  (reify client/Client
    (open! [_ test node]
      (let [conn (connect node cp-direct-to-leader-routing)]
        (cas-reference-client conn (create-atomic-reference conn "jepsen.cas-register") cp-direct-to-leader-routing)))

    (setup! [this test]
                 "Called to set up database state for testing.")

    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (.get atomic-ref))
        :write (do (.set atomic-ref (:value op))
                   (assoc op :type :ok))
        :cas (let [[currentV newV] (:value op)]
               (if (.compareAndSet atomic-ref currentV newV)
                 (assoc op :type :ok)
                 (assoc op :type :fail :error :cas-failed)))))

    (teardown! [this test]
       (.shutdown conn))

    (close! [this test]
       (.shutdown conn))

    client/Reusable
    (reusable? [this test]
               true)
  ))

(defn cas-cp-map-client
  "A CAS register using a CPMap"
  [conn cp-map cp-direct-to-leader-routing]
  (reify client/Client
    (open! [_ test node]
      (let [conn (connect node cp-direct-to-leader-routing)]
        (cas-cp-map-client conn (create-cp-map conn "jepsen.cas-cp-map") cp-direct-to-leader-routing)))

    (setup! [this test]
                 "Called to set up database state for testing.")

    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (.get cp-map "key"))
        :write (do (.set cp-map "key" (:value op))
                   (assoc op :type :ok))
        :cas (let [[currentV newV] (:value op)]
               (if (.compareAndSet cp-map "key" currentV newV)
                 (assoc op :type :ok)
                 (assoc op :type :fail :error :cas-failed)))))

    (teardown! [this test]
       (.shutdown conn))

    (close! [this test]
           (.shutdown conn))

    client/Reusable
    (reusable? [this test]
                   true)
))

(defn random-string
  "Generates a random alphanumeric string of a given length."
  [length]
  (let [chars (map char (concat (range 48 58)   ; 0-9
                                (range 65 91)   ; A-Z
                                (range 97 123)))] ; a-z
    (apply str (repeatedly length #(rand-nth chars)))))

(def random-values (vec (repeatedly 3 #(random-string 100))))

(defn snapshot-stress-client [conn cp-map routing]
  (reify client/Client
    (open! [_ test node]
      (let [conn (connect node routing)
            cp-map (create-cp-map conn "jepsen.snapshot-test")]
        (snapshot-stress-client conn cp-map routing)))

    (setup! [_ test]
      (doseq [i (range 100000)]
        (.set cp-map (str "key-" i) (rand-nth random-values))))

    (invoke! [_ test op]
      (try
        (case (:f op)
          :read (assoc op :type :ok
                       :value (.get cp-map (:key op)))
          :write (do (.set cp-map (:key op) (:value op))
                     (assoc op :type :ok :value (:value op)))
          (assoc op :type :fail :error "Unknown op"))
        (catch Exception e
          (assoc op :type :fail :error (.getMessage e)))))

    (teardown! [_ test] (.shutdown conn))
    (close! [_ test] (.shutdown conn))
    client/Reusable
    (reusable? [_ test] true)))

(def queue-poll-timeout
  "How long to wait for items to become available in the queue, in ms"
  1)

(defn queue-client
  "Uses :enqueue, :dequeue, and :drain events to interact with a Hazelcast
  queue."
  ([]
   (queue-client nil nil))
  ([conn queue]
   (reify client/Client
     (open! [_ test node]
       (let [conn (connect node)]
         (queue-client conn (.getQueue conn "jepsen.queue"))))

     (invoke! [this test op]
       (case (:f op)
         :enqueue (do (.put queue (:value op))
                      (assoc op :type :ok))
         :dequeue (if-let [v (.poll queue
                                    queue-poll-timeout TimeUnit/MILLISECONDS)]
                    (assoc op :type :ok, :value v)
                    (assoc op :type :fail, :error :empty))
         :drain (loop [values []]
                  (if-let [v (.poll queue
                                    queue-poll-timeout TimeUnit/MILLISECONDS)]
                    (recur (conj values v))
                    (assoc op :type :ok, :value values)))))

     (teardown! [this test]
       (.shutdown conn)))))

(defn queue-gen
  "A generator for queue operations. Emits enqueues of sequential integers."
  []
  (let [next-element (atom -1)]
    (->> (gen/mix [(fn enqueue-gen [_ _]
                     {:type  :invoke
                      :f     :enqueue
                      :value (swap! next-element inc)})
                   {:type :invoke, :f :dequeue}])
         (gen/stagger 1))))

(defn queue-client-and-gens
  "Constructs a queue client and generator. Returns {:client
  ..., :generator ...}."
  []
  {:client          (queue-client)
   :generator       (queue-gen)
   :final-generator (->> {:type :invoke, :f :drain}
                         gen/once
                         gen/each-thread)})

(defn log-ok
  [clientName op fence]
  (do (info (str "ok " clientName " -> " (:value op)))
      (assoc op :type :ok :value {:client clientName :fence fence :uid (:value op)})))

(defn log-fail
  [clientName op]
  (do (info (str "fail " clientName " -> " (:value op)))
      (assoc op :type :fail)))

(defn log-maybe
  [clientName op exception]
  (do (warn (str "maybe " clientName " -> " (:value op) " exception class: " (.getClass exception) " message: " (.getMessage exception)))
      (assoc op :type :info)))

(defn invoke-acquire [clientName lock op]
  (let [fence (.tryLockAndGetFence lock 5000 TimeUnit/MILLISECONDS)]
    (if (not= fence invalid-fence)
      (log-ok clientName op fence)
      (log-fail clientName op))))

(defn fenced-lock-client
  ([lock-name cp-direct-to-leader-routing] (fenced-lock-client nil nil lock-name cp-direct-to-leader-routing))
  ([conn lock lock-name cp-direct-to-leader-routing]
   (reify client/Client
     (open! [_ test node]
       (let [conn (connect node cp-direct-to-leader-routing)]
         (fenced-lock-client conn (.getLock (.getCPSubsystem conn) lock-name) lock-name cp-direct-to-leader-routing)))

     (setup! [this test]
                  "Called to set up database state for testing.")

     (invoke! [this test op]
       (let [clientName (.getName conn)]
         (try
           (info (str " " clientName " -> " op))
           (swap! (:client-uids-to-client-names test) assoc (:value op) clientName)
           (case (:f op)
             :acquire (invoke-acquire clientName lock op)
             :release (do
                        (.unlock lock)
                        (log-ok clientName op invalid-fence)))
           (catch IllegalMonitorStateException _
             (assoc (log-fail clientName op) :error :not-lock-owner))
           (catch IOException e
             (condp re-find (.getMessage e)
               ; This indicates that the Hazelcast client doesn't have a remote
               ; peer available, and that the message was never sent.
               #"Packet is not send to owner address"
               (assoc (log-fail clientName op) :error :client-down)
               (assoc (log-maybe clientName op e) :error :io-exception)))
           (catch Exception e
             (assoc (log-maybe clientName op e) :error :exception)))))

     (teardown! [this test]
       (.terminate (.getLifecycleService conn)))

     (close! [this test]
       (.terminate (.getLifecycleService conn)))

     client/Reusable
     (reusable? [this test]
                true)
   )))

(defn semaphore-client
  ([] (semaphore-client nil nil "false"))
  ([conn semaphore cp-direct-to-leader-routing]
   (reify client/Client
     (open! [_ test node]
       (let [conn (connect node cp-direct-to-leader-routing)
             sem (.getSemaphore (.getCPSubsystem conn) "jepsen.cpSemaphore")
             _ (.init sem num-permits)]
         (semaphore-client conn sem cp-direct-to-leader-routing)))

     (setup! [this test]
                  "Called to set up database state for testing.")

     (invoke! [this test op]
       (let [clientName (.getName conn)]
         (try
           (info (str clientName " -> " op))
           (swap! (:client-uids-to-client-names test) assoc (:value op) clientName)
           (case (:f op)
             :acquire (if (.tryAcquire semaphore 5000 TimeUnit/MILLISECONDS)
                        (do
                          (log-ok clientName op invalid-fence))
                        (assoc (log-fail clientName op) :debug {:client clientName :uid (:value op)}))
             :release (do
                        (.release semaphore)
                        (log-ok clientName op invalid-fence))
             )
           (catch IllegalArgumentException _
             (assoc (log-fail clientName op) :error :not-permit-owner :debug {:client clientName :uid (:value op)}))
           (catch IOException e
             (condp re-find (.getMessage e)
               ; This indicates that the Hazelcast client doesn't have a remote
               ; peer available, and that the message was never sent.
               #"Packet is not send to owner address"
               (assoc (log-fail clientName op) :error :client-down :debug {:client clientName :uid (:value op)})
               (assoc (log-maybe clientName op e) :error :io-exception :debug {:client clientName :uid (:value op)})))
           (catch Exception e
             (assoc (log-maybe clientName op e) :error :exception :debug {:client clientName :uid (:value op)})))))

     (teardown! [this test]
       (.terminate (.getLifecycleService conn)))

     (close! [this test]
       (.terminate (.getLifecycleService conn)))

     client/Reusable
       (reusable? [this test]
                  true)
   )))

(def map-name "jepsen.map")
(def crdt-map-name "jepsen.crdt-map")

(defn map-client
  "Options:
   :crdt? - If true, use CRDTs for merging divergent maps."
  ([opts] (map-client nil nil opts))
  ([conn m opts]
   (reify client/Client
     (open! [_ test node]
       (let [conn (connect node)]
         (map-client conn
                     (.getMap conn (if (:crdt? opts)
                                     crdt-map-name
                                     map-name))
                     opts)))
     (invoke! [this test op]
       (case (:f op)
         ; Note that Hazelcast serialization doesn't seem to know how to
         ; replace types like HashSet, so we're storing our sets as sorted long
         ; arrays instead.
         :add (if-let [v (.get m "hi")]
                ; We have a current set.
                (let [s (into (sorted-set) v)
                      s' (conj s (:value op))
                      v' (long-array s')]
                  (if (.replace m "hi" v v')
                    (assoc op :type :ok)
                    (assoc op :type :fail, :error :cas-failed)))

                ; We're starting fresh.
                (let [v' (long-array (sorted-set (:value op)))]
                  ; Note that replace and putIfAbsent have opposite senses for
                  ; their return values.
                  (if (.putIfAbsent m "hi" v')
                    (assoc op :type :fail, :error :cas-failed)
                    (assoc op :type :ok))))

         :read (assoc op :type :ok, :value (into (sorted-set) (.get m "hi")))))

     (teardown! [this test]
       (.shutdown conn)))))

(defn map-workload
  "A workload for map tests, with the given client options."
  [client-opts]
  {:client          (map-client client-opts)
   :generator       (->> (range)
                         (map (fn [x] {:type  :invoke
                                       :f     :add
                                       :value x}))
                         gen/once
                         (gen/stagger 1/10))
   :final-generator (->> {:type :invoke, :f :read}
                         gen/once
                         gen/each-thread)
   :checker         (checker/set)})



(defn get-client [client-uids-to-client-names-map op]
  (let [val (:value op)]
    (if (map? val) (:client val) (get @client-uids-to-client-names-map (:value op)))))

(defrecord ReentrantMutex [client-uids-to-client-names-map owner lockCount]
  Model
  (step [this op]
    (let [client (get-client client-uids-to-client-names-map op)]
      (if (nil? client)
        (knossos.model/inconsistent "no owner!")
        (condp = (:f op)
          :acquire (if (and (< lockCount reentrant-lock-acquire-count) (or (nil? owner) (= owner client)))
                     (ReentrantMutex. client-uids-to-client-names-map client (+ lockCount 1))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (ReentrantMutex. client-uids-to-client-names-map (if (= lockCount 1) nil owner) (- lockCount 1)))))))

  Object
  (toString [this] (str "owner: " owner ", lockCount: " lockCount " client-uids-to-client-names-map: " @client-uids-to-client-names-map)))

(defn create-reentrant-mutex [client-uids-to-client-names-map]
  "A single reentrant mutex responding to :acquire and :release messages"
  (ReentrantMutex. client-uids-to-client-names-map nil 0))



(defrecord OwnerAwareMutex [client-uids-to-client-names-map owner]
  Model
  (step [this op]
    (let [client (get-client client-uids-to-client-names-map op)]
      (if (nil? client)
        (knossos.model/inconsistent "no owner!")
        (condp = (:f op)
          :acquire (if (nil? owner)
                     (OwnerAwareMutex. client-uids-to-client-names-map client)
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (OwnerAwareMutex. client-uids-to-client-names-map nil))))))

  Object
  (toString [this] (str "owner: " owner " client-uids-to-client-names-map: " @client-uids-to-client-names-map)))

(defn create-owner-aware-mutex [client-uids-to-client-names-map]
  "A single non-reentrant mutex responding to :acquire and :release messages and tracking mutex owner"
  (OwnerAwareMutex. client-uids-to-client-names-map nil))



(defn get-fence [op]
  (let [val (:value op)]
    (if (map? val) (:fence val) invalid-fence)))

(defrecord FencedMutex [client-uids-to-client-names-map owner lockFence prevOwner]
  Model
  (step [this op]
    (let [client (get-client client-uids-to-client-names-map op) fence (get-fence op)]
      (if (nil? client)
        (knossos.model/inconsistent "no owner!")
        (condp = (:f op)
          :acquire (cond
                     (some? owner) (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (= fence invalid-fence) (FencedMutex. client-uids-to-client-names-map client lockFence owner)
                     (> fence lockFence) (FencedMutex. client-uids-to-client-names-map client fence owner)
                     :else (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (FencedMutex. client-uids-to-client-names-map nil lockFence owner))))))

  Object
  (toString [this] (str "owner: " owner " lock fence: " lockFence " prev owner: " prevOwner " client-uids-to-client-names-map: " @client-uids-to-client-names-map)))

(defn create-fenced-mutex [client-uids-to-client-names-map]
  "A fenced mutex responding to :acquire and :release messages and tracking monotonicity of observed fences"
  (FencedMutex. client-uids-to-client-names-map nil invalid-fence nil))



(defrecord ReentrantFencedMutex [client-uids-to-client-names-map owner lockCount currentFence highestObservedFence highestObservedFenceOwner]
  Model
  (step [this op]
    (let [client (get-client client-uids-to-client-names-map op) fence (get-fence op)]
      (if (nil? client)
        (knossos.model/inconsistent "no owner!")
        (condp = (:f op)
          :acquire (cond
                     ; if the lock is not held
                     (nil? owner)
                      (cond
                        ; I can have an invalid fence or a fence larger than highestObservedFence
                        (or (= fence invalid-fence) (> fence highestObservedFence))
                        (ReentrantFencedMutex. client-uids-to-client-names-map client 1 fence (max fence highestObservedFence) highestObservedFenceOwner)
                        :else
                        (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
                     ; if the new acquire does not match to the current lock owner, or the lock is already acquired twice, we cannot acquire anymore
                     (or (not= owner client) (= lockCount reentrant-lock-acquire-count)) (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     ; if the lock is acquired without a fence, and the new acquire has no fence or a fence larger than highestObservedFence
                     (= currentFence invalid-fence) (cond (or (= fence invalid-fence) (> fence highestObservedFence))
                                                      (ReentrantFencedMutex. client-uids-to-client-names-map client (+ lockCount 1) fence (max fence highestObservedFence) highestObservedFenceOwner)
                                                    :else
                                                      (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
                     ; if the lock is acquired with a fence, and the new acquire has no fence or the same fence
                     (or (= fence invalid-fence) (= fence currentFence)) (ReentrantFencedMutex. client-uids-to-client-names-map client (+ lockCount 1) currentFence highestObservedFence highestObservedFenceOwner)
                     :else (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (cond (= lockCount 1) (ReentrantFencedMutex. client-uids-to-client-names-map nil 0 invalid-fence highestObservedFence (if (= currentFence invalid-fence) highestObservedFenceOwner owner))
                           :else (ReentrantFencedMutex. client-uids-to-client-names-map owner (- lockCount 1) currentFence highestObservedFence highestObservedFenceOwner)))))))

  Object
  (toString [this] (str "owner: " owner " lock count: " lockCount " lock fence: " currentFence " highest observed fence: " highestObservedFence " highest observed fence owner: " highestObservedFenceOwner " client-uids-to-client-names-map: " @client-uids-to-client-names-map)))

(defn create-reentrant-fenced-mutex [client-uids-to-client-names-map]
  "A reentrant fenced mutex responding to :acquire and :release messages and tracking monotonicity of observed fences"
  (ReentrantFencedMutex. client-uids-to-client-names-map nil 0 invalid-fence invalid-fence nil))



(defrecord AcquiredPermitsModel [client-uids-to-client-names-map acquired]
  Model
  (step [this op]
    (let [client (get-client client-uids-to-client-names-map op)]
      (if (nil? client)
        (knossos.model/inconsistent "no owner!")
        (condp = (:f op)
          :acquire (if (< (reduce + (vals acquired)) num-permits)
                     (AcquiredPermitsModel. client-uids-to-client-names-map (update acquired client (fnil inc 0)))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (> (get acquired client 0) 0)
                     (AcquiredPermitsModel. client-uids-to-client-names-map (update acquired client (fnil dec 0)))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))))))

  Object
  (toString [this] (str "acquired: " acquired " client-uids-to-client-names-map: " @client-uids-to-client-names-map)))

(defn create-acquired-permits-model [client-uids-to-client-names-map]
  "A model that assign permits to multiple nodes via :acquire and :release messages"
  (AcquiredPermitsModel. client-uids-to-client-names-map {}))


(defn workloads
  "The workloads we can run. Each workload is a map like

      {:generator         a generator of client ops
       :final-generator   a generator to run after the cluster recovers
       :client            a client to execute those ops
       :checker           a checker
       :model             for the checker}

  Note that workloads are *stateful*, since they include generators; that's why
  this is a function, instead of a constant--we may need a fresh workload if we
  run more than one test."
  [client-uids-to-client-names-map opts]
  (let [cp-direct-to-leader-routing (:cp-direct-to-leader-routing opts)]
  {:crdt-map                  (map-workload {:crdt? true})
     :map                       (map-workload {:crdt? false})
     :non-reentrant-lock        {:client    (fenced-lock-client "jepsen.cpLock1" cp-direct-to-leader-routing)
                                 :generator (->> (fn [] [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                         {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}])
                                                 gen/each-thread
                                                 (gen/stagger 0.25))
                                 :checker   (checker/linearizable {:model (create-owner-aware-mutex client-uids-to-client-names-map)})}
     :reentrant-lock            {:client    (fenced-lock-client "jepsen.cpLock2" cp-direct-to-leader-routing)
                                 :generator (->> (fn [] [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                         {:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                         {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}
                                                         {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}])
                                                 gen/each-thread
                                                 (gen/stagger 0.25))
                                 :checker   (checker/linearizable {:model (create-reentrant-mutex client-uids-to-client-names-map)})}
     :non-reentrant-fenced-lock {:client    (fenced-lock-client "jepsen.cpLock1" cp-direct-to-leader-routing)
                                 :generator (->> (fn [] [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                         {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}])
                                                 gen/each-thread
                                                 (gen/stagger 0.5))
                                 :checker   (checker/linearizable {:model (create-fenced-mutex client-uids-to-client-names-map)})}
     :reentrant-fenced-lock     {:client    (fenced-lock-client "jepsen.cpLock2" cp-direct-to-leader-routing)
                                 :generator (->> (fn [] [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                         {:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                         {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}
                                                         {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}])
                                                 gen/each-thread
                                                 (gen/stagger 0.5))
                                 :checker   (checker/linearizable {:model (create-reentrant-fenced-mutex client-uids-to-client-names-map)})}
     :semaphore                 {:client    (semaphore-client)
                                 :generator (->> (fn [] [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                         {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}])
                                                 gen/each-thread
                                                 (gen/stagger 0.25))
                                 :checker   (checker/linearizable {:model (create-acquired-permits-model client-uids-to-client-names-map)})}
     :id-gen-long               {:client    (atomic-long-id-client nil nil cp-direct-to-leader-routing)
                                 :generator (->> [{:type :invoke, :f :generate}]
                                                 cycle
                                                 (gen/stagger 0.25))
                                 :checker   (checker/unique-ids)}
     :cas-long                  {:client    (cas-long-client nil nil cp-direct-to-leader-routing)
                                 :generator (->> (fn [] (gen/mix [{:type :invoke, :f :read}
                                                                  {:type :invoke, :f :write, :value (rand-int 5)}
                                                                  {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]}]))
                                                 gen/each-thread
                                                 (gen/stagger 0.25))
                                 :checker   (checker/linearizable {:model (model/cas-register 0)})}
     :cas-reference             {:client    (cas-reference-client nil nil cp-direct-to-leader-routing)
                                 :generator (->> (fn [] (gen/mix [{:type :invoke, :f :read}
                                                                  {:type :invoke, :f :write, :value (rand-int 5)}
                                                                  {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]}]))
                                                 gen/each-thread
                                                 (gen/stagger 0.25))
                                 :checker   (checker/linearizable {:model (model/cas-register 0)})}
     :cas-cp-map                {:client    (cas-cp-map-client nil nil cp-direct-to-leader-routing)
                                  :generator (->> (fn [] (gen/mix [{:type :invoke, :f :read}
                                                                 {:type :invoke, :f :write, :value (rand-int 5)}
                                                                 {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]}]))
                                                gen/each-thread
                                                (gen/stagger 0.25))
                                :checker   (checker/linearizable {:model (model/cas-register 0)})}
     :cp-map-purge-lite-member {:client  (cp-map-purge-client)
                                :generator purge-generator
                                :final-generator
                                (->> (gen/once
                                      (fn []
                                        (let [k (str "key-" (rand-int 2000))]
                                          {:type :invoke :f :get :key k})))
                                     gen/each-thread)
                                :checker (checker/linearizable
                                          {:model (purgeable-cp-map-model)})}
     :snapshot-stress           {:client (snapshot-stress-client nil nil cp-direct-to-leader-routing)
                                 :generator (->> (fn []
                                                  (let [k (str "key-" (rand-int 100000))
                                                        v (rand-nth random-values)]
                                                    (gen/mix [{:type :invoke :f :read :key k}
                                                              {:type :invoke :f :write :key k :value v}])))
                                                gen/each-thread
                                                (gen/stagger 0.25)
                                                 (gen/limit 100))
                                :final-generator (->> (fn []
                                                        (let [k (str "key-" (rand-int 100000))]
                                                          {:type :invoke :f :read :key k}))
                                                      gen/each-thread
                                                      (gen/limit 100))
                                :checker (independent/checker
                                          (checker/linearizable {:model (model/register) :key :key}))}
     :queue                     (assoc (queue-client-and-gens)
                                  :checker (checker/total-queue))
                                  }))

(defn select-majority
  "Select majority from nodes"
  [coll]
  (take (util/majority (count coll)) (shuffle coll)))

(defn select-minority
  "Select minority from nodes"
  [coll]
  (let [c (count coll)]
   (take (- c (util/majority c)) (shuffle coll))))

(defn restart-nodes
  "Kills random nodes on start, restarts them on stop."
  [selector-fn]
  (nemesis/node-start-stopper
    selector-fn
    (fn start [test node] (stop! test node))
    (fn stop [test node] (start! test node))))

(defn parse-nemesis
  "Parses nemesis argument"
  [nemesis-arg]
  (case nemesis-arg
    "partition" (nemesis/partition-majorities-ring)
    "restart-majority" (restart-nodes select-majority)
    "restart-minority" (restart-nodes select-minority)
    "hammer-time" (nemesis/hammer-time "java")
    (nemesis/partition-majorities-ring)
    )
  )
(defn hazelcast-test
  "Constructs a Jepsen test map from CLI options"
  [opts]
  (let [client-uids-to-client-names-map (atom {})
        {:keys [generator
                final-generator
                client
                checker
                model]}
        (get (workloads client-uids-to-client-names-map opts) (:workload opts))
        generator (->> generator
                       (gen/nemesis (start-stop 20 20))
                       (gen/time-limit (:time-limit opts)))
        generator (if-not final-generator
                    generator
                    (gen/phases generator
                                (gen/log "Healing cluster")
                                (gen/nemesis
                                  (gen/once {:type :info, :f :stop}))
                                (gen/log "Waiting for quiescence")
                                (gen/sleep 500)
                                (gen/clients final-generator)))]
    (merge tests/noop-test
           opts
           {:name      (str "hazelcast " (name (:workload opts)))
            :os        debian/os
            :db        (db)
            :client    client
            :nemesis   (parse-nemesis (:nemesis opts))
            :generator generator
            :checker   (checker/compose
                         {:perf     (checker/perf)
                          :stats    (checker/stats)
                          :unhandled-exceptions (checker/unhandled-exceptions)
                          :timeline (timeline/html)
                          :workload checker})
            :model     model
            :client-uids-to-client-names client-uids-to-client-names-map})))

(def opt-spec
  "Additional command line options"
  [[nil "--workload WORKLOAD" "Test workload to run, e.g. atomic-long-ids."
    :parse-fn keyword
    :missing (str "--workload " (cli/one-of (workloads nil {})))
    :validate [(workloads nil {}) (cli/one-of (workloads nil {}))]],
   [nil "--nemesis NEMESIS" "Nemesis type, e.g. partition, restart-majority"],
   [nil "--persistent PERSISTENT" "Is persistence enabled?"],
   [nil "--license LICENSE" "Hazelcast Enterprise License"],
   [nil "--cp-direct-to-leader-routing ROUTING" "Enable CP direct to leader routing"]])

(defn -main
  "Command line runner."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  hazelcast-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
