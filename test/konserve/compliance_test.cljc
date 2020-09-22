(ns konserve.compliance-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? keys])
  (:require [clojure.core.async :as async :refer [#?(:clj <!!) go chan <!]]
            [konserve.core :refer [get assoc assoc-in get-in update-in dissoc bassoc bget keys]]
            #?(:cljs [cljs.test :refer [deftest is testing async]])
            #?(:clj [clojure.test :refer :all])
            [konserve.memory :refer [new-mem-store]]))

#?(:clj (defn compliance-test [store]
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
                 list-keys))))))

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