(ns konserve.boring-serializer-test
  "The boring (CBOR) serializer, byte 3.

  Two things these tests pin that the clj-cbor serializer (byte 2) could not
  do at all: it accepts read handlers rather than throwing on them, and it runs
  on both platforms from one implementation."
  (:require [clojure.test :refer [deftest testing is]]
            [konserve.core :as k]
            [konserve.memory :refer [map->MemoryStore]]
            [konserve.serializers :as ser]))

(defrecord BPoint [x y])

(defn- store-with
  "A synchronous in-memory store using the boring serializer. Built the way
  konserve's own memory tests build one -- a MemoryStore needs its locks and
  handler atoms supplied, not defaulted."
  [read-handlers]
  (map->MemoryStore {:state (atom {})
                     :read-handlers (atom read-handlers)
                     :write-handlers (atom {})
                     :locks (atom {})
                     :default-serializer :BoringSerializer}))

(deftest boring-serializer-is-registered-as-byte-3
  (testing "the id is persisted in every blob header, so it must be stable"
    (is (= :BoringSerializer (get ser/byte->key 3)))
    (is (instance? konserve.serializers.BoringSerializer
                   (get ser/byte->serializer 3)))
    (testing "and it must not have displaced an existing id"
      (is (= :StringSerializer   (get ser/byte->key 0)))
      (is (= :FressianSerializer (get ser/byte->key 1)))
      #?(:clj (is (= :CBORSerializer (get ser/byte->key 2)))))))

(deftest round-trips-clojure-values
  (let [store (store-with {})]
    (doseq [[label v] [["scalar"      42]
                       ["string"      "hello"]
                       ["keyword"     :some.ns/kw]
                       ["map"         {:a 1 :b "two" :c [1 2 3]}]
                       ["set"         #{:x :y :z}]
                       ["nested"      {:xs (vec (range 50)) :m {:k :v}}]
                       ["uuid"        #uuid "9682952b-fafa-4b41-8e4a-31ae948d6f08"]
                       ["nil-value"   nil]
                       #?@(:clj [["bigdec" 1.50M]
                                 ["ratio"  (/ 22 7)]])]]
      (testing label
        (k/assoc store label v {:sync? true})
        (is (= v (k/get store label nil {:sync? true})) label)))))

#?(:clj
   (deftest bigdecimal-keeps-its-scale
     (testing "1.50M and 1.5M must stay distinguishable — `=` does NOT check
               scale on BigDecimal, so this compares the string form"
       (let [store (store-with {})]
         (k/assoc store "d" 1.50M {:sync? true})
         (is (= "1.50" (str (k/get store "d" nil {:sync? true}))))))))

(deftest records-round-trip-with-a-read-handler
  (testing "incognito-style handlers are keyed by the normalized type symbol,
            which is exactly boring's own wire name — so the bridge is a rename"
    (let [store (store-with {'konserve.boring_serializer_test.BPoint map->BPoint})]
      (k/assoc store "p" (->BPoint 3 4) {:sync? true})
      (let [back (k/get store "p" nil {:sync? true})]
        (is (= (->BPoint 3 4) back))
        (is (= BPoint (type back)))))))

(deftest records-survive-without-a-read-handler
  (testing "an unregistered record must not be LOST. boring writes the type
            name natively via tag 27, so it comes back as an inert value
            carrying the same name and fields rather than vanishing into a
            plain map — which is the failure incognito exists to prevent."
    (let [store (store-with {})]
      (k/assoc store "p" (->BPoint 3 4) {:sync? true})
      (let [back (k/get store "p" nil {:sync? true})]
        (is (some? back))
        (is (= 3 (:x back)))
        (is (= 4 (:y back)))))))

(deftest handlers-are-accepted-not-rejected
  (testing "the clj-cbor serializer THREW on any handler, which is why it could
            never serialize a record or an index node. This one must not."
    (is (some? (store-with {'some.Thing identity})))
    (let [store (store-with {'some.Thing identity})]
      (k/assoc store "k" {:a 1} {:sync? true})
      (is (= {:a 1} (k/get store "k" nil {:sync? true}))))))
