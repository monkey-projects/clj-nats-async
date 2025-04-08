(ns clj-nats-async.core-test
  (:require [clojure.test :refer [deftest testing is]]
            [babashka.fs :as fs]
            [clj-nats-async.core :as sut]
            [config.core :as cc]
            [manifold.stream :as ms]))

(defn- as-file
  "If `creds` points to a file, returns it.  Otherwise writes the contents to a temp
   file, which will be deleted on exit."
  [creds]
  (if (fs/exists? creds)
    creds
    (let [t (fs/create-temp-file)]
      (spit (fs/file t) creds)
      (str (fs/delete-on-exit t)))))

(def url (:nats-url cc/env))
(def creds (as-file (:nats-creds cc/env)))

(deftest integration-test
  (let [conn (sut/create-nats {:urls [url]
                               :secure? true
                               :verbose? true
                               :credential-path creds})]
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
