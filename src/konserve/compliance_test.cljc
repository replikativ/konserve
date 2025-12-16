(ns konserve.compliance-test
  (:require [clojure.core.async :refer [#?(:clj <!!) <! go]]
            [konserve.core :as k]
            [konserve.utils :as utils]
            #?(:cljs [cljs.test :refer [is]])
            #?(:clj [clojure.test :refer [are is testing]])))

#_(deftype UnknownType [])

#_(:clj (defn exception? [thing]
          (instance? Throwable thing)))

#?(:clj
   (defn compliance-test [store]
     (doseq [opts [{:sync? false} {:sync? true}]
             :let [<!! (if (:sync? opts) identity <!!)]]

       (testing "Testing the append store functionality."
         (<!! (k/append store :foolog {:bar 42} opts))
         (<!! (k/append store :foolog {:bar 43} opts))
         (is (= (<!! (k/log store :foolog opts))
                '({:bar 42}
                  {:bar 43})))
         (is (= (<!! (k/reduce-log store
                                   :foolog
                                   (fn [acc elem]
                                     (conj acc elem))
                                   []
                                   opts))
                [{:bar 42} {:bar 43}]))
         (let [{:keys [key type last-write]} (<!! (k/get-meta store :foolog nil opts))]
           (are [x y] (= x y)
             :foolog        key
             :append-log    type
             java.util.Date (clojure.core/type last-write))))

       (testing "Test the core API."
         (is (= nil (<!! (k/get store :foo nil opts))))
         (is (false? (<!! (k/exists? store :foo opts))))
         (<!! (k/assoc store :foo :bar opts))
         (is (<!! (k/exists? store :foo opts)))
         (is (= :bar (<!! (k/get store :foo nil opts))))
         (<!! (k/assoc-in store [:foo] :bar2 opts))
         (is (= :bar2 (<!! (k/get store :foo nil opts))))
         (is (= :default (<!! (k/get-in store [:fuu] :default opts))))
         (is (= :bar2 (<!! (k/get store :foo nil opts))))
         (is (= :default (<!! (k/get-in store [:fuu] :default opts))))
         (<!! (k/update-in store [:foo] name opts))
         (is (= "bar2" (<!! (k/get store :foo nil opts))))
         (<!! (k/assoc-in store [:baz] {:bar 42} opts))
         (<!! (k/assoc-in store [:baz :barf] 43 opts))
         (is (= 42 (<!! (k/get-in store [:baz :bar] nil opts))))
         (<!! (k/update-in store [:baz :bar] inc opts))
         (is (= 43 (<!! (k/get-in store [:baz :bar] nil opts))))
         (<!! (k/update-in store [:baz :bar] (fn [x] (+ x 2 3)) opts))
         (is (= 48 (<!! (k/get-in store [:baz :bar] nil opts))))
         (is (= true (<!! (k/dissoc store :foo opts))))
         (is (= false (<!! (k/dissoc store :not-there opts))))
         (is (= nil (<!! (k/get-in store [:foo] nil opts))))
         (<!! (k/bassoc store :binbar (byte-array (range 10)) opts))
         (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                                      (go
                                        (is (= (map byte (slurp input-stream))
                                               (range 10)))
                                        true))
                      opts))
         (let  [list-keys (<!! (k/keys store opts))]
           (are [x y] (= x y)
             #{{:key :baz
                :type :edn}
               {:key :binbar
                :type :binary}
               {:key :foolog
                :type :append-log}}
             (->> list-keys (map #(clojure.core/dissoc % :last-write)) set)
             true
             (every?
              (fn [{:keys [:last-write]}]
                (= (type (java.util.Date.)) (type last-write)))
              list-keys)))

         (doseq [to-delete [:baz :binbar :foolog]]
           (<!! (k/dissoc store to-delete opts)))

        ;; TODO fix by adding spec to core and cache namespace
         #_(let [params (clojure.core/keys store)
                 corruptor (fn [s k]
                             (if (= (type (k s)) clojure.lang.Atom)
                               (clojure.core/assoc-in s [k] (atom {}))
                               (clojure.core/assoc-in s [k] (UnknownType.))))
                 corrupt (reduce corruptor store params)]
             (is (exception? (<!! (get corrupt :bad))))
             (is (exception? (<!! (get-meta corrupt :bad))))
             (is (exception? (<!! (assoc corrupt :bad 10))))
             (is (exception? (<!! (dissoc corrupt :bad))))
             (is (exception? (<!! (assoc-in corrupt [:bad :robot] 10))))
             (is (exception? (<!! (update-in corrupt [:bad :robot] inc))))
             (is (exception? (<!! (exists? corrupt :bad))))
             (is (exception? (<!! (keys corrupt))))
             (is (exception? (<!! (bget corrupt :bad (fn [_] nil)))))
             (is (exception? (<!! (bassoc corrupt :binbar (byte-array (range 10))))))))

       ;; Optional test for multi-key operations - runs if store supports it
       (when (utils/multi-key-capable? store)
         (testing "Testing multi-key operations"
             ;; Test multi-assoc with flat keys
           (let [result (<!! (k/multi-assoc store {:multi1 42 :multi2 "value"} opts))]
             (is (= result {:multi1 true :multi2 true}))
             (is (= 42 (<!! (k/get store :multi1 nil opts))))
             (is (= "value" (<!! (k/get store :multi2 nil opts)))))

             ;; Test multi-dissoc with existing keys
           (let [result (<!! (k/multi-dissoc store [:multi1 :multi2] opts))]
             (is (= result {:multi1 true :multi2 true}))
             (is (= nil (<!! (k/get store :multi1 nil opts))))
             (is (= nil (<!! (k/get store :multi2 nil opts)))))

             ;; Test multi-dissoc with non-existing keys
           (let [result (<!! (k/multi-dissoc store [:nonexistent1 :nonexistent2] opts))]
             (is (= result {:nonexistent1 false :nonexistent2 false})))

             ;; Test multi-dissoc with mix of existing and non-existing keys
           (<!! (k/multi-assoc store {:multi3 "test3" :multi4 "test4"} opts))
           (let [result (<!! (k/multi-dissoc store [:multi3 :multi4 :nonexistent3] opts))]
             (is (= result {:multi3 true :multi4 true :nonexistent3 false}))
             (is (= nil (<!! (k/get store :multi3 nil opts))))
             (is (= nil (<!! (k/get store :multi4 nil opts)))))

             ;; Test multi-get with existing keys
           (<!! (k/multi-assoc store {:multi5 "value5" :multi6 {:nested "data"} :multi7 123} opts))
           (let [result (<!! (k/multi-get store [:multi5 :multi6 :multi7] opts))]
             (is (= result {:multi5 "value5" :multi6 {:nested "data"} :multi7 123})))

             ;; Test multi-get with some missing keys (sparse map behavior)
           (let [result (<!! (k/multi-get store [:multi5 :nonexistent :multi7] opts))]
             ;; Should only contain found keys
             (is (= result {:multi5 "value5" :multi7 123}))
             ;; Verify missing key is not in result
             (is (not (contains? result :nonexistent))))

             ;; Test multi-get with all missing keys
           (let [result (<!! (k/multi-get store [:missing1 :missing2 :missing3] opts))]
             ;; Should return empty map
             (is (= result {})))

             ;; Test multi-get with empty key list
           (let [result (<!! (k/multi-get store [] opts))]
             ;; Should return empty map
             (is (= result {})))

             ;; Clean up multi-get test keys
           (<!! (k/multi-dissoc store [:multi5 :multi6 :multi7] opts))))

      ;; Optional test for write hooks - runs if store supports it
       (when (utils/write-hooks-capable? store)
         (testing "Testing write hooks"
           (let [hook-events (atom [])]
            ;; Register a hook that captures events
             (k/add-write-hook! store ::test-hook
                                (fn [event]
                                  (swap! hook-events conj event)))

            ;; Test assoc-in triggers hook
             (<!! (k/assoc-in store [:hook-test] {:value 42} opts))
             (is (= 1 (count @hook-events)))
             (is (= :assoc-in (:api-op (first @hook-events))))
             (is (= :hook-test (:key (first @hook-events))))
             (is (= {:value 42} (:value (first @hook-events))))
             (is (= [:hook-test] (:key-vec (first @hook-events))))

            ;; Test update-in triggers hook
             (<!! (k/update-in store [:hook-test :value] inc opts))
             (is (= 2 (count @hook-events)))
             (is (= :update-in (:api-op (second @hook-events))))
             (is (= :hook-test (:key (second @hook-events))))
             (is (= [:hook-test :value] (:key-vec (second @hook-events))))

            ;; Test dissoc triggers hook
             (<!! (k/dissoc store :hook-test opts))
             (is (= 3 (count @hook-events)))
             (is (= :dissoc (:api-op (nth @hook-events 2))))
             (is (= :hook-test (:key (nth @hook-events 2))))

            ;; Test bassoc triggers hook
             (<!! (k/bassoc store :hook-bin (byte-array [1 2 3]) opts))
             (is (= 4 (count @hook-events)))
             (is (= :bassoc (:api-op (nth @hook-events 3))))
             (is (= :hook-bin (:key (nth @hook-events 3))))

            ;; Test multi-assoc triggers hook (if supported)
             (when (utils/multi-key-capable? store)
               (<!! (k/multi-assoc store {:hook-m1 1 :hook-m2 2} opts))
               (is (= 5 (count @hook-events)))
               (is (= :multi-assoc (:api-op (nth @hook-events 4))))
               (is (= {:hook-m1 1 :hook-m2 2} (:kvs (nth @hook-events 4))))
              ;; Clean up multi-assoc keys
               (<!! (k/dissoc store :hook-m1 opts))
               (<!! (k/dissoc store :hook-m2 opts)))

            ;; Test removing hook - should stop receiving events
             (k/remove-write-hook! store ::test-hook)
             (let [count-before (count @hook-events)]
               (<!! (k/assoc-in store [:hook-after-remove] "test" opts))
               (is (= count-before (count @hook-events))
                   "No new events after hook removal"))

            ;; Clean up
             (<!! (k/dissoc store :hook-bin opts))
             (<!! (k/dissoc store :hook-after-remove opts))))))))

(defn async-compliance-test [store]
  (go
    (and
     (is (= nil (<! (k/get store :foo))))
     (is (= [nil :bar] (<! (k/assoc store :foo :bar))))
     (is (= :bar (<! (k/get store :foo))))
     (is (= [nil :bar2] (<! (k/assoc-in store [:foo] :bar2))))
     (is (= :bar2 (<! (k/get store :foo))))
     (is (= :default (<! (k/get-in store [:fuu] :default))))
     (<! (k/update-in store [:foo] name))
     (is (= "bar2" (<! (k/get store :foo))))
     (<! (k/assoc-in store [:baz] {:bar 42}))
     (is (= (<! (k/get-in store [:baz :bar])) 42))
     (<! (k/update-in store [:baz :bar] inc))
     (is (= (<! (k/get-in store [:baz :bar])) 43))
     (<! (k/update-in store [:baz :bar] #(+ % 2 3)))
     (is (= (<! (k/get-in store [:baz :bar])) 48))
     (<! (k/dissoc store :foo))
     (is (= (<! (k/get-in store [:foo])) nil))

     ;; Optional test for multi-key operations - runs if store supports it
     (if (utils/multi-key-capable? store)
       (do
         ;; Test multi-assoc
         (is (= {:multi1 true :multi2 true}
                (<! (k/multi-assoc store {:multi1 42 :multi2 "value"}))))
         (is (= 42 (<! (k/get store :multi1))))
         (is (= "value" (<! (k/get store :multi2))))

         ;; Test multi-get with existing keys
         (<! (k/multi-assoc store {:multi5 "value5" :multi6 {:nested "data"} :multi7 123}))
         (is (= {:multi5 "value5" :multi6 {:nested "data"} :multi7 123}
                (<! (k/multi-get store [:multi5 :multi6 :multi7]))))

         ;; Test multi-get with some missing keys (sparse map)
         (is (= {:multi5 "value5" :multi7 123}
                (<! (k/multi-get store [:multi5 :nonexistent :multi7]))))

         ;; Test multi-get with all missing keys
         (is (= {} (<! (k/multi-get store [:missing1 :missing2]))))

         ;; Test multi-dissoc
         (is (= {:multi1 true :multi2 true}
                (<! (k/multi-dissoc store [:multi1 :multi2]))))
         (is (= nil (<! (k/get store :multi1))))
         (is (= nil (<! (k/get store :multi2))))

         ;; Clean up
         (<! (k/multi-dissoc store [:multi5 :multi6 :multi7]))
         true)
       true))))
