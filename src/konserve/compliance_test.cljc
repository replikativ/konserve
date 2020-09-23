(ns konserve.compliance-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? keys])
  (:require [clojure.core.async :refer [<!! go chan]]
            [konserve.core :refer :all]
            #?(:clj [clojure.test :refer :all]
               :cljs [cljs.test :refer :all :include-macros true])))



(deftype UnknownType [])

(defn exception? [thing]
  (instance? Throwable thing))
  
(defn compliance-test [store]
  (testing "Test the core API."
    (is (= (<!! (get store :foo))
           nil))
    (<!! (assoc store :foo :bar))
    (is (= (<!! (get store :foo))
           :bar))
    (<!! (assoc-in store [:foo] :bar2))
    (is (= :bar2 (<!! (get store :foo))))
    (is (= :default
           (<!! (get-in store [:fuu] :default))))
    (is (= :bar2 (<!! (get store :foo))))
    (is (= :default
           (<!! (get-in store [:fuu] :default))))
    (<!! (update-in store [:foo] name))
    (is (= "bar2"
           (<!! (get store :foo))))
    (<!! (assoc-in store [:baz] {:bar 42}))
    (is (= (<!! (get-in store [:baz :bar]))
           42))
    (<!! (update-in store [:baz :bar] inc))
    (is (= (<!! (get-in store [:baz :bar]))
           43))
    (<!! (update-in store [:baz :bar] + 2 3))
    (is (= (<!! (get-in store [:baz :bar]))
           48))
    (<!! (dissoc store :foo))
    (is (= (<!! (get-in store [:foo]))
           nil))
    (<!! (bassoc store :binbar (byte-array (range 10))))
    (<!! (bget store :binbar (fn [{:keys [input-stream]}]
                               (go
                                 (is (= (map byte (slurp input-stream))
                                        (range 10)))))))
    (let  [list-keys (<!! (keys store))]
      (are [x y] (= x y)
        #{{:key :baz,
           :type :edn}
          {:key :binbar,
           :type :binary}}
        (->> list-keys (map #(clojure.core/dissoc % :konserve.core/timestamp)) set)
        true
        (every?
         (fn [{:keys [:konserve.core/timestamp]}]
           (= (type (java.util.Date.)) (type timestamp)))
         list-keys)))
    
    (let [params (clojure.core/keys store)
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
      (is (exception? (<!! (bassoc corrupt :binbar (byte-array (range 10)))))))))


