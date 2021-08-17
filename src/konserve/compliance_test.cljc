(ns konserve.compliance-test
  (:require [clojure.core.async :as async :refer [#?(:clj <!!) go chan <!]]
            [konserve.core :as k]
            [konserve.storage-layout :refer [PLinearLayout read-header header-size -get-raw]]
            #?(:cljs [cljs.test :refer [deftest is testing async]])
            #?(:clj [clojure.test :refer :all])
            [konserve.memory :refer [new-mem-store]]))

(deftype UnknownType [])

#?(:clj (defn exception? [thing]
          (instance? Throwable thing)))

#?(:clj
   (defn compliance-test [store]
     (when (extends? PLinearLayout (type store))
       (testing "Testing linear layout."
         (<!! (k/assoc-in store [:foo] 42))
         (let [[layout-id serializer-id compressor-id encryptor-id metadata-size]
               (read-header (byte-array (take header-size (seq (<!! (-get-raw store :foo))))))]
           (is (= [layout-id serializer-id compressor-id encryptor-id]
                  [1 1 0 0]))
           (is (= metadata-size 52)))
         (<!! (k/dissoc store :foo))))


     (testing "Testing the append store functionality."
       (<!! (k/append store :foolog {:bar 42}))
       (<!! (k/append store :foolog {:bar 43}))
      (is (= (<!! (k/log store :foolog))
             '({:bar 42}
               {:bar 43})))
      (is (= (<!! (k/reduce-log store
                                :foolog
                                (fn [acc elem]
                                  (conj acc elem))
                                []))
             [{:bar 42} {:bar 43}]))
      (let [{:keys [key type :timestamp]} (<!! (k/get-meta store :foolog))]
        (are [x y] (= x y)
          :foolog        key
          :append-log    type
          java.util.Date (clojure.core/type timestamp))))

     (testing "Test the core API."
       (is (= (<!! (k/get store :foo))
              nil))
       (<!! (k/assoc store :foo :bar))
       (is (= (<!! (k/get store :foo))
              :bar))
       (<!! (k/assoc-in store [:foo] :bar2))
       (is (= :bar2 (<!! (k/get store :foo))))
       (is (= :default
              (<!! (k/get-in store [:fuu] :default))))
       (is (= :bar2 (<!! (k/get store :foo))))
       (is (= :default
              (<!! (k/get-in store [:fuu] :default))))
       (<!! (k/update-in store [:foo] name))
       (is (= "bar2"
              (<!! (k/get store :foo))))
       (<!! (k/assoc-in store [:baz] {:bar 42}))
       (is (= (<!! (k/get-in store [:baz :bar]))
              42))
       (<!! (k/update-in store [:baz :bar] inc))
       (is (= (<!! (k/get-in store [:baz :bar]))
              43))
       (<!! (k/update-in store [:baz :bar] (fn [x] (+ x 2 3))))
       (is (= (<!! (k/get-in store [:baz :bar]))
              48))
       (<!! (k/dissoc store :foo))
       (is (= (<!! (k/get-in store [:foo]))
              nil))
       (<!! (k/bassoc store :binbar (byte-array (range 10))))
       (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                                  (go
                                    (is (= (map byte (slurp input-stream))
                                           (range 10)))
                                    true))))
       (let  [list-keys (<!! (k/keys store))]
         (are [x y] (= x y)
           #{{:key :baz
              :type :edn}
             {:key :binbar
              :type :binary}
             {:key :foolog
              :type :append-log}}
           (->> list-keys (map #(clojure.core/dissoc % :timestamp)) set)
           true
           (every?
            (fn [{:keys [:timestamp]}]
              (= (type (java.util.Date.)) (type timestamp)))
            list-keys)))


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
           (is (exception? (<!! (bassoc corrupt :binbar (byte-array (range 10))))))))))

#?(:cljs (deftest compliance-test-cljs
           (testing "this is a test"
             (async done
                    (go
                      (let [store (<! (new-mem-store))]
                        (is (= (<! (get store :foo)) nil))
                        (<!  (assoc store :foo :bar))
                        (is (= :bar (<! (get store :foo))))
                        (<! (assoc-in store [:foo] :bar2))
                        (is (= :bar2 (<! (get store :foo))))
                        (is (= :default
                               (<! (get-in store [:fuu] :default))))
                        (<! (update-in store [:foo] name))
                        (is (= "bar2" (<! (get store :foo))))
                        (<! (assoc-in store [:baz] {:bar 42}))
                        (is (= (<! (get-in store [:baz :bar])) 42))
                        (<! (update-in store [:baz :bar] inc))
                        (is (= (<! (get-in store [:baz :bar])) 43))
                        (<! (update-in store [:baz :bar] + 2 3))
                        (is (= (<! (get-in store [:baz :bar])) 48))
                        (<! (dissoc store :foo))
                        (is (= (<! (get-in store [:foo])) nil))
                        (done)))))))

