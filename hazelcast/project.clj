(defproject jepsen.hazelcast "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Hazelcast IMDG"
  :url "http://jepsen.io/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.7"]
                 [com.hazelcast/hazelcast-enterprise "5.7.0-SNAPSHOT"]]
  :main jepsen.hazelcast)
