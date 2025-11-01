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
             (is (= nil (<!! (k/get store :multi4 nil opts))))))))))

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
     (is (= (<! (k/get-in store [:foo])) nil)))))
