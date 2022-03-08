(ns konserve.compliance-test
  (:require [clojure.core.async :refer [#?(:clj <!!) go chan <!]]
            [konserve.core :as k]
            #?(:cljs [cljs.test :refer [deftest is testing async]])
            #?(:clj [clojure.test :refer :all])
            #?(:clj [konserve.serializers :refer [key->serializer]])
            #?(:clj [konserve.utils :refer [async+sync]])
            [konserve.memory :refer [new-mem-store]]))

(deftype UnknownType [])

#?(:clj (defn exception? [thing]
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
         (is (= 42 (<!! (k/get-in store [:baz :bar] nil opts))))
         (<!! (k/update-in store [:baz :bar] inc opts))
         (is (= 43 (<!! (k/get-in store [:baz :bar] nil opts))))
         (<!! (k/update-in store [:baz :bar] (fn [x] (+ x 2 3)) opts))
         (is (= 48 (<!! (k/get-in store [:baz :bar] nil opts))))
         (= true (<!! (k/dissoc store :foo opts)))
         (= false (<!! (k/dissoc store :not-there opts)))
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
             (is (exception? (<!! (bassoc corrupt :binbar (byte-array (range 10)))))))))))

#?(:cljs (deftest compliance-test-cljs
           (async done
                  (go
                    (let [store (<! (new-mem-store))]
                      (is (= (<! (k/get store :foo)) nil))
                      (<! (k/assoc store :foo :bar))
                      (is (= :bar (<! (k/get store :foo))))
                      (<! (k/assoc-in store [:foo] :bar2))
                      (is (= :bar2 (<! (k/get store :foo))))
                      (is (= :default
                             (<! (k/get-in store [:fuu] :default))))
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
                      (done))))))

