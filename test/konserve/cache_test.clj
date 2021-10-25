(ns konserve.cache-test
  (:require [konserve.cache :as k]
            [konserve.filestore :as fstore]
            [clojure.core.async :refer [<!! go] :as async]
            [clojure.test :refer [deftest testing is are]]))

(deftest cache-test

  (doseq [opts [{:sync? false} {:sync? true}]
          :let [<!! (if (:sync? opts) identity <!!)]]
    (let [_ (fstore/delete-store "/tmp/cache-store")
          test-store (<!! (fstore/connect-fs-store "/tmp/cache-store" :opts opts))
          store (k/ensure-cache test-store)]

      (testing "Test the cache API."
        (is (= (<!! (k/get store :foo nil opts)) nil))
        (is (false? (<!! (k/exists? store :foo opts))))
        (<!! (k/assoc store :foo :bar opts))
        (is (<!! (k/exists? store :foo)))
        (is (= (<!! (k/get store :foo nil opts))
               :bar))
        (<!! (k/assoc-in store [:foo] :bar2 opts))
        (is (= :bar2 (<!! (k/get store :foo nil opts))))
        (is (= :default
               (<!! (k/get-in store [:fuu] :default opts))))
        (is (= :bar2 (<!! (k/get store :foo nil opts))))
        (is (= :default
               (<!! (k/get-in store [:fuu] :default opts))))
        (<!! (k/update-in store [:foo] name opts))
        (is (= "bar2"
               (<!! (k/get store :foo nil opts))))
        (<!! (k/assoc-in store [:baz] {:bar 42} opts))
        (is (= (<!! (k/get-in store [:baz :bar] nil opts))
               42))
        (<!! (k/update-in store [:baz :bar] inc opts))
        (is (= (<!! (k/get-in store [:baz :bar] nil opts))
               43))
        (<!! (k/update-in store [:baz :bar] (fn [x] (+ x 2 3)) opts))
        (is (= (<!! (k/get-in store [:baz :bar] nil opts))
               48))
        (= true (<!! (k/dissoc store :foo opts)))
        (is (= (<!! (k/get-in store [:foo] nil opts))
               nil))
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
               :type :binary}}
            (->> list-keys (map #(clojure.core/dissoc % :last-write)) set)
            true
            (every?
             (fn [{:keys [:last-write]}]
               (= (type (java.util.Date.)) (type last-write)))
             list-keys)))

        (doseq [to-delete [:baz :binbar :foolog]]
          (<!! (k/dissoc store to-delete opts)))))))


