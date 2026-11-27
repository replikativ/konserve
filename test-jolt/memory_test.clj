(ns memory-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is run-tests]]
            [konserve.core :as k]))

(deftest public-memory-api
  (doseq [sync? [true false]]
    (let [opts {:sync? sync?}
          result (fn [op]
                   (let [v (if sync? op (async/<!! op))]
                     (if (instance? Throwable v) (throw v) v)))
          config {:backend :memory :id (random-uuid)}
          store (result (k/create-store config opts))]
      (try
        (result (k/assoc store :item 42 (assoc opts :expected-revision k/absent)))
        (is (= 42 (result (k/get store :item nil opts))))
        (is (= 42 (result (k/get (result (k/connect-store config opts)) :item nil opts))))
        (is (= :konserve/revision-mismatch
               (try
                 (result (k/assoc store :item 0 (assoc opts :expected-revision k/absent)))
                 nil
                 (catch Exception e (:type (ex-data e))))))
        (is (= 42 (result (k/get store :item nil opts))))
        (result (k/multi-assoc store [[:a 1] [:b 2]] opts))
        (is (= {:a 1 :b 2} (result (k/multi-get store [:a :b :missing] opts))))
        (result (k/multi-dissoc store [:a :b] opts))
        (is (= {} (result (k/multi-get store [:a :b] opts))))
        (finally (result (k/delete-store config opts)))))))

(deftest repeated-key-lock-releases
  (let [store {:locks (atom {})}]
    (dotimes [_ 16]
      (let [lock (k/get-lock store :key)]
        (try
          (is (= :unlocked (async/poll! lock)))
          (is (true? (async/offer! lock :unlocked)))
          (is (not (async/offer! lock :unlocked)))
          (finally (k/release-lock store :key)))))
    (is (empty? @(:locks store)))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'memory-test)]
    (shutdown-agents)
    (System/exit (if (zero? (+ fail error)) 0 1))))
