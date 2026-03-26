(defproject jepsen.hazelcast-server "0.1.0-SNAPSHOT"
  :description "A basic Hazelcast server"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src"]
  :java-source-paths ["java"]
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [org.clojure/tools.cli "1.1.230"]
                 [org.clojure/tools.logging "1.3.0"]
                 [spootnik/unilog "0.7.32"]
                 [com.hazelcast/hazelcast-enterprise "5.7.0-SNAPSHOT"]]
  :profiles {:uberjar {:uberjar-name "hazelcast-server.jar"}}
  :main jepsen.hazelcast-server
  :aot [jepsen.hazelcast-server])
