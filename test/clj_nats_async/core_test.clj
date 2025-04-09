(ns clj-nats-async.core-test
  (:require [clojure.test :refer [deftest testing is]]
            [babashka.fs :as fs]
            [clj-nats-async.core :as sut]
            [config.core :as cc]
            [manifold.stream :as ms]))

(def url (:nats-url cc/env))
(def creds (:nats-creds cc/env))

(defn- add-creds [conf]
  (let [k (if (fs/exists? creds) :credential-path :static-creds)]
    (assoc conf k creds)))

(deftest make-options
  (testing "handles url sequence"
    (is (= ["nats://url-1:4222" "nats://url-2:4222"]
           (->> ["url-1" "url-2"]
                (sut/make-options)
                (.getServers)
                (map str)))))

  (testing "parses string as url list"
    (is (= ["nats://url-1:4222" "nats://url-2:4222"]
           (->> (sut/make-options ["url-1,url-2"])
                (.getServers)
                (map str)))))

  (testing "passes urls from map"
    (let [urls ["nats://test-url:4222"]]
      (is (= urls
             (->> (sut/make-options [{:urls urls}])
                  (.getServers)
                  (map str))))))

  (testing "accepts static credentials"
    (is (some? (-> (sut/make-options [{:static-creds "test-creds"}])
                   (.getAuthHandler)))))

  (testing "accepts file credentials"
    (is (some? (-> (sut/make-options [{:credential-path "test-path"}])
                   (.getAuthHandler))))))

(deftest integration-test
  (let [conn (sut/create-nats (-> {:urls [url]
                                   :secure? true
                                   :verbose? true}
                                  (add-creds)))]
    (testing "can connect to server"
      (is (sut/connection? conn)))

    (testing "subscription"
      (let [msg {:message "Test message"}
            subj "test.subject"
            in (sut/subscribe conn subj)]
        (testing "can send and receive"
          (is (nil? (sut/publish conn subj msg)))
          (is (= msg (-> (deref (ms/take! in) 1000 :timeout)
                         (sut/msg-body)))))

        (testing "can close"
          (is (nil? (ms/close! in))))))

    (testing "pubsub"
      (let [ps (sut/pubsub conn "test.subject.2")
            msg {:message "Other message"}]
        (is (ms/stream? ps))
        (testing "can send and receive on same stream"
          (is (true? @(ms/put! ps msg)))
          (is (= msg (-> (ms/take! ps)
                         (deref 1000 :timeout)
                         (sut/msg-body)))))

        (testing "can close"
          (is (nil? (ms/close! ps))))))

    (testing "can close connection"
      (is (nil? (.close conn))))))
