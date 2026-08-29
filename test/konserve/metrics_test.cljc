(ns konserve.metrics-test
  "Every operation reports one event to the sink, at the level it happened —
   without any store being wrapped."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [#?(:clj <!!)]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.metrics :as metrics]
            #?(:clj [konserve.filestore :refer [delete-store]])
            #?(:clj [konserve.store :as ks])))

(defn- recording!
  "Install a sink collecting into an atom; returns the atom."
  []
  (let [events (atom [])]
    (metrics/set-sink! #(swap! events conj %))
    events))

(defn- ops [events level] (->> @events (filter #(= level (:level %))) (map :op) set))

#?(:clj
   (deftest api-events-sync-and-async
     (let [events (recording!)
           store  (<!! (new-mem-store))]
       (try
         (k/assoc-in store [:a] 1 {:sync? true})
         (<!! (k/assoc-in store [:b] 2))
         (k/get-in store [:a] nil {:sync? true})
         (<!! (k/get-in store [:b]))
         (k/update-in store [:a] inc {:sync? true})
         (k/exists? store :a {:sync? true})
         (<!! (k/dissoc store :b))
         (k/bassoc store :bin (byte-array [1 2 3]) {:sync? true})
         (testing "one :api event per call, sync and async alike"
           (is (= #{:assoc-in :get-in :update-in :exists? :dissoc :bassoc} (ops events :api)))
           (is (= 8 (count (filter #(= :api (:level %)) @events))))
           (is (every? #(and (integer? (:nanos %)) (<= 0 (:nanos %))) @events)))
         (testing "labels come from the store"
           (is (every? #(= :memorystore (:backend %)) @events)))
         (testing "a failing operation reports its error and still throws"
           (is (thrown? Exception (k/update-in store [:a] (fn [_] (throw (ex-info "boom" {}))) {:sync? true})))
           (is (= "ExceptionInfo" (:error (last @events))))
           (is (= :update-in (:op (last @events)))))
         (finally
           (metrics/set-sink! nil))))))

#?(:clj
   (deftest no-sink-no-events
     (let [events (atom [])
           store  (<!! (new-mem-store))]
       (metrics/set-sink! nil)
       (k/assoc-in store [:a] 1 {:sync? true})
       (is (empty? @events)))))

#?(:clj
   (deftest io-and-lock-events-on-the-filestore
     (let [path   (str (System/getProperty "java.io.tmpdir") "/konserve-metrics-" (random-uuid))
           events (recording!)
           id     (random-uuid)
           store  (ks/create-store {:backend :file :path path :id id} {:sync? true})]
       (try
         (k/assoc-in store [:a] {:x 1} {:sync? true})
         (k/get-in store [:a] nil {:sync? true})
         (k/dissoc store :a {:sync? true})
         (testing "the blob level reports the backend's own work, per blob operation"
           (is (= #{:write-edn :read-edn :delete} (ops events :io))))
         (testing "bytes written are their own event"
           (let [b (first (filter :bytes @events))]
             (is (= :write-edn (:op b)))
             (is (pos? (:bytes b)))))
         (testing "the lock wait is reported"
           (is (contains? (ops events :lock) :lock)))
         (testing "every event carries the store's backend and id"
           (is (every? #(= :file (:backend %)) @events))
           (is (every? #(= id (:store-id %)) @events)))
         (finally
           (metrics/set-sink! nil)
           (delete-store path))))))
