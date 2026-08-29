(ns konserve.metrics-test
  "Every operation reports one event to every registered sink, at the level
   it happened — without any store being wrapped — and a sink can never harm
   the operation it observes."
  (:require [clojure.test :refer [deftest is testing]]
            #?(:clj [clojure.core.async :refer [<!!]])
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.metrics :as metrics]
            #?(:clj [konserve.filestore :refer [delete-store]])
            #?(:clj [konserve.store :as ks])))

(defn- recording!
  "Register a recording sink under `id`; returns the atom it fills."
  [id]
  (let [events (atom [])]
    (metrics/add-sink! id #(swap! events conj %))
    events))

(defn- ops [events level] (->> @events (filter #(= level (:level %))) (map :op) vec))

(deftest sync-api-events-on-a-memory-store
  ;; Synchronous only, so it runs on every platform.
  (let [events (recording! ::t)
        store  (new-mem-store (atom {}) {:sync? true})]
    (try
      (k/assoc-in store [:a] 1 {:sync? true})
      (k/get-in store [:a] nil {:sync? true})
      (k/update-in store [:a] inc {:sync? true})
      (k/exists? store :a {:sync? true})
      (k/dissoc store :a {:sync? true})
      (k/keys store {:sync? true})
      (testing "one :api event per call, in call order, with the store's labels"
        (is (= [:assoc-in :get-in :update-in :exists? :dissoc :keys] (ops events :api)))
        (is (every? #(and (number? (:nanos %)) (<= 0 (:nanos %))) @events)))
      (testing "a failing operation reports its error and still throws"
        (is (thrown? #?(:clj Exception :cljs js/Error)
                     (k/update-in store [:a] (fn [_] (throw (ex-info "boom" {}))) {:sync? true})))
        (is (= :update-in (:op (last @events))))
        (is (some? (:error (last @events)))))
      (finally
        (metrics/remove-sink! ::t)))))

(deftest a-sink-cannot-harm-the-operation
  (let [events (recording! ::good)
        store  (new-mem-store (atom {}) {:sync? true})]
    (metrics/add-sink! ::bad (fn [_] (throw (ex-info "sink-boom" {}))))
    (try
      (testing "a throwing sink is logged and skipped; the operation returns its value and the other sink still sees it"
        (is (= [nil 1] (k/assoc-in store [:a] 1 {:sync? true})))
        (is (= 1 (k/get-in store [:a] nil {:sync? true})))
        (is (= [:assoc-in :get-in] (ops events :api))))
      (finally
        (metrics/remove-sink! ::bad)
        (metrics/remove-sink! ::good)))))

(deftest no-sink-no-events
  (let [store (new-mem-store (atom {}) {:sync? true})]
    (is (empty? @metrics/sinks))
    (is (= [nil 1] (k/assoc-in store [:a] 1 {:sync? true})))))

#?(:clj
   (deftest async-api-events
     (let [events (recording! ::t)
           store  (<!! (new-mem-store))]
       (try
         (<!! (k/assoc-in store [:a] 1))
         (<!! (k/get-in store [:a]))
         (<!! (k/bassoc store :bin (byte-array [1 2 3])))
         (is (= [:assoc-in :get-in :bassoc] (ops events :api)))
         (testing "an exception delivered on the channel is reported, and still delivered"
           (let [r (<!! (k/update-in store [:a] (fn [_] (throw (ex-info "boom" {})))))]
             (is (instance? Throwable r))
             (is (= "ExceptionInfo" (:error (last @events))))))
         (testing "a throwing sink does not close the channel without a value"
           (metrics/add-sink! ::bad (fn [_] (throw (ex-info "sink-boom" {}))))
           (is (= 1 (<!! (k/get-in store [:a]))))
           (metrics/remove-sink! ::bad))
         (finally
           (metrics/remove-sink! ::t))))))

#?(:clj
   (deftest io-lock-and-bytes-events-on-the-filestore
     (let [path   (str (System/getProperty "java.io.tmpdir") "/konserve-metrics-" (random-uuid))
           events (recording! ::t)
           id     (random-uuid)
           store  (ks/create-store {:backend :file :path path :id id} {:sync? true})]
       (try
         (k/assoc-in store [:a] {:x 1} {:sync? true})
         (testing "a fresh write: lock, bytes, the write itself, the API call"
           (is (= [{:level :lock :op :lock}
                   {:level :io :op :write-edn :bytes? true}
                   {:level :io :op :write-edn :bytes? false}
                   {:level :api :op :assoc-in}]
                  (mapv #(cond-> (select-keys % [:level :op]) (= :io (:level %)) (assoc :bytes? (contains? % :bytes))) @events))))
         (reset! events [])
         (k/update-in store [:a :x] inc {:sync? true})
         (testing "an update reads the old value first, labelled as such"
           (is (= [:read-old :write-edn :write-edn] (ops events :io))))
         (reset! events [])
         (k/get-in store [:a] nil {:sync? true})
         (k/get-in store [:missing] nil {:sync? true})
         (testing "a read; a miss is answered by the existence probe and reaches no blob"
           (is (= [:read-edn] (ops events :io)))
           (is (= [:get-in :get-in] (ops events :api)))
           (is (every? #(not (contains? % :error)) @events)))
         (reset! events [])
         (k/bassoc store :bin (byte-array 100) {:sync? true})
         (testing "binary writes report their bytes too"
           (is (<= 100 (:bytes (first (filter :bytes @events)))))
           (is (some #(= :write-binary (:op %)) @events)))
         (reset! events [])
         (k/dissoc store :a {:sync? true})
         (is (= [:delete] (ops events :io)))
         (testing "every event carries the store's backend and id"
           (is (every? #(= :file (:backend %)) @events))
           (is (every? #(= id (:store-id %)) @events)))
         (finally
           (metrics/remove-sink! ::t)
           (delete-store path))))))

#?(:clj
   (deftest multi-key-io-events
     (let [events (recording! ::t)
           store  (<!! (new-mem-store))]
       (try
         (<!! (k/multi-assoc store {:a 1 :b 2}))
         (<!! (k/multi-get store [:a :b]))
         (<!! (k/multi-dissoc store [:a :b]))
         (is (= [:multi-assoc :multi-get :multi-dissoc] (ops events :api)))
         (finally
           (metrics/remove-sink! ::t))))))
