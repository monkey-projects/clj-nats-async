(defproject clj-nats-async "1.2.1"
  :description "an async client for NATS, wrapping java-nats"
  :url "https://github.com/monkey-projects/clj-nats-async"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :dependencies [[io.nats/jnats "2.21.0"]
                 [manifold "0.4.3"]]

  :profiles {:provided {:dependencies [[org.clojure/clojure "1.12.0"]]}})
