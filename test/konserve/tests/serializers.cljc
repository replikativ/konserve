(ns konserve.tests.serializers
  (:require [clojure.core.async :as a :refer [<! go #?(:clj <!!)]]
            [clojure.test :refer [is testing]]
            [fress.api :as fress]
            [konserve.core :as k]
            [konserve.serializers :refer [fressian-serializer]]
            [incognito.base #?@(:cljs [:refer [IncognitoTaggedLiteral]])])
  #?(:clj
     (:import [org.fressian.handlers WriteHandler ReadHandler]
              [incognito.base IncognitoTaggedLiteral]
              (java.util Date))))

(deftype MyType [a b]
  #?@(:clj
     (Object
      (equals [this other]
              (and (instance? MyType other)
                   (= a (.a ^MyType other))
                   (= b (.b ^MyType other))))
      (hashCode [this] (hash [a b])))
     :cljs
     (IEquiv
      (-equiv [this other]
              (and (instance? MyType other)
                   (= a (.-a ^MyType other))
                   (= b (.-b ^MyType other)))))))

(defrecord MyRecord [a b])

(def custom-read-handlers
  {"my-type"
   #?(:clj
      (reify ReadHandler
        (read [_ rdr _ _]
          (MyType. (fress/read-object rdr)
                   (fress/read-object rdr))))
      :cljs
      (fn [rdr _ _]
        (MyType. (fress.api/read-object rdr)
                 (fress.api/read-object rdr))))
   "custom-tag"
   #?(:clj
      (reify ReadHandler
        (read [_ rdr _ _]
              (Date. ^long (.readObject rdr))))
      :cljs
      (fn [rdr _ _]
        (doto (js/Date.)
          (.setTime (fress/read-object rdr)))))
   "my-record"
   #?(:clj
      (reify ReadHandler
        (read [_ rdr _ _]
              (MyRecord. (fress/read-object rdr)
                         (fress/read-object rdr))))
      :cljs
      (fn [rdr _ _]
        (MyRecord. (fress/read-object rdr)
                   (fress/read-object rdr))))})

(def custom-write-handlers
  {#?(:clj Date :cljs js/Date)
   {"custom-tag"
    #?(:clj
       (reify WriteHandler
         (write [_ writer instant]
                (fress/write-tag    writer "custom-tag" 1)
                (fress/write-object writer (.getTime ^Date instant))))
       :cljs
       (fn [wrt date]
         (fress/write-tag wrt "custom-tag" 1)
         (fress/write-object wrt (.getTime date))))}
   MyType {"my-type"
           #?(:clj
              (reify WriteHandler
                (write [_ writer o]
                       (fress/write-tag writer "my-type" 2)
                       (fress/write-object writer ^MyType (.-a o))
                       (fress/write-object writer ^MyType (.-b o))))
              :cljs
              (fn [writer o]
                (fress/write-tag writer "my-type" 2)
                (fress/write-object writer (.-a o))
                (fress/write-object writer (.-b o))))}
   MyRecord {"my-record"
             #?(:clj
                (reify WriteHandler
                  (write [_ writer o]
                         (fress/write-tag writer "my-record" 2)
                         (fress/write-object writer (.-a o))
                         (fress/write-object writer (.-b o))))
                :cljs
                (fn [writer o]
                  (fress/write-tag writer "my-record" 2)
                  (fress/write-object writer (.-a o))
                  (fress/write-object writer (.-b o))))}})

(defn test-fressian-serializers-async
  "Test roundtripping custom types and records using the :FressianSerializer.
   `locked-cb` is used to verify fressian bytes, needs to simply pass through
   the input-stream on jvm or a full realized byte array in js"
  [store-name connect-store delete-store-async locked-cb]
  (go
   (and
    (testing ":serializers arg to connect-store"
      (let [serializers {:FressianSerializer (fressian-serializer custom-read-handlers
                                                                  custom-write-handlers)}
            _(assert (nil? (<! (delete-store-async store-name))))
            store (<! (connect-store store-name :serializers serializers))
            d #?(:clj (Date.) :cljs (js/Date.))
            my-type (MyType. "a" "b")
            my-record (map->MyRecord {:a 0 :b 1})
            res (and
                 (is [nil 42] (<! (k/assoc-in store [:foo] 42)))
                 (is (= 42 (<! (k/get-in store [:foo]))))
                 (is (= [nil d] (<! (k/assoc-in store [:foo] d))))
                 (testing "should be written with custom-tag not built-in"
                   (let [bytes (<! (k/bget store :foo locked-cb))
                         o (fress/read bytes)]
                     (and (is (fress/tagged-object? o))
                          (is (= "custom-tag" (fress/tag o))))))
                 (is (= d (<! (k/get-in store  [:foo]))))
                 (is (= [nil my-type] (<! (k/assoc-in store [:foo] my-type))))
                 (is (= my-type (<! (k/get-in store [:foo]))))
                 (testing "custom write-handler takes precedent over incognito"
                   (is (= [nil my-record] (<! (k/assoc-in store [:foo] my-record))))
                   (let [bytes (<! (k/bget store :foo locked-cb))
                         o (fress/read bytes)]
                     (and (is (fress/tagged-object? o))
                          (is (= "my-record" (fress/tag o)))))
                   (is (= my-record (<! (k/get-in store [:foo]))))))]
        #?(:cljs (when (.-close (:backing store)) (<! (.close (:backing store)))))
        (assert (nil? (<! (delete-store-async store-name))))
        res))
    (testing "records are intercepted by incognito by default"
      (let [store (<! (connect-store store-name))
            my-record (map->MyRecord {:a 0 :b 1})
            res (and
                 (is (= [nil my-record] (<! (k/assoc-in store [:bar] my-record))))
                 (is (instance? IncognitoTaggedLiteral (<! (k/get-in store [:bar])))))]
        #?(:cljs (when (.-close (:backing store)) (<! (.close (:backing store)))))
        (assert (nil? (<! (delete-store-async store-name))))
        res))
    (testing ":read-handlers arg to connect-store let's us recover records"
        (let [read-handlers {'konserve.tests.serializers.MyRecord map->MyRecord}
              store (<! (connect-store store-name
                                       :read-handlers (atom read-handlers)))
              my-record (map->MyRecord {:a 0 :b 1})]
          (and
           (is (nil? (<! (k/get-in store [:foo]))))
           (is (= [nil my-record] (<! (k/assoc-in store [:foo] my-record))))
           (is (= my-record (<! (k/get-in store [:foo])))))
          #?(:cljs (when (.-close (:backing store)) (<! (.close (:backing store)))))
          (assert (nil? (<! (delete-store-async store-name)))))))))

#?(:clj
   (defn cbor-serializer-test [store-name connect-store delete-store-async]
     (testing "Test CBOR serializer functionality."
       (let [_      (<!! (delete-store-async store-name))
             store  (<!! (connect-store store-name :default-serializer :CBORSerializer))]
         (is (nil? (<!! (k/get-in store [:foo]))))
         (<!! (k/assoc-in store [:foo] (Date.)))
         (is (= java.time.Instant (type (<!! (k/get-in store [:foo])))))
         (<!! (k/dissoc store :foo))
         (is (nil? (<!! (k/get-in store [:foo]))))
         (<!! (delete-store-async store-name))))))