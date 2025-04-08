(ns build-test
  (:require [clojure.test :refer [deftest testing is]]
            [build :as sut]
            [monkey.ci.test :as mt]
            [monkey.ci.build.v2 :as m]))

(deftest test-job
  (testing "runs unit tests"
    (mt/with-build-params {"NATS_URL" "test-url"}
      (let [job (sut/test-job mt/test-ctx)]
        (is (m/container-job? job))))))

(deftest deploy-job
  (mt/with-build-params {}
    (testing "`nil` if not on main branch"
      (is (nil? (sut/deploy-job mt/test-ctx))))

    (testing "deploys when on main branch"
      (is (m/container-job? (sut/deploy-job (-> mt/test-ctx
                                                (mt/with-git-ref "refs/heads/main"))))))))
