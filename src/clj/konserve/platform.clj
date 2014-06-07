(ns konserve.platform
  "Platform specific io operations clj."
  (:require [konserve.protocols :refer [IEDNAsyncKeyValueStore -get-in -assoc-in -update-in]]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [clojure.core.async :as async
             :refer [<! >! timeout chan alt! go go-loop]]
            [com.ashafa.clutch :refer [couch create!] :as cl]))

(def log println)

;; is this binding a good idea? should it be the same in cljs?
(def ^:dynamic *read-opts* nil)

(defn read-string-safe [s]
  (when s (edn/read-string (or *read-opts* {}) s)))

(defrecord CouchKeyValueStore [db]
  IEDNAsyncKeyValueStore
  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec]
      (go (get-in (->> fkey
                       pr-str
                       (cl/get-document db)
                       :edn-value
                       read-string-safe)
                  rkey))))
  ;; TODO, cleanup and unify with update-in
  (-assoc-in [this key-vec value]
    (go (let [[fkey & rkey] key-vec
              doc (cl/get-document db (pr-str fkey))]
          (cond (and (not doc) value)
                (cl/put-document db {:_id (pr-str fkey)
                                     :edn-value (pr-str (if-not (empty? rkey)
                                                          (assoc-in nil rkey value)
                                                          value))})

                (not value)
                (cl/delete-document db doc)

                :else
                ((fn trans [doc attempt]
                   (try (cl/update-document db
                                            doc
                                            (fn [{v :edn-value :as old}]
                                              (assoc old
                                                :edn-value (pr-str (if-not (empty? rkey)
                                                                     (assoc-in (read-string-safe v) rkey value)
                                                                     value)))))
                        (catch clojure.lang.ExceptionInfo e
                          (if (< attempt 10)
                            (trans (cl/get-document db (pr-str fkey)) (inc attempt))
                            (do
                              (log e)
                              (.printStackTrace e)
                              (throw e)))))) doc 0))
          nil)))
  (-update-in [this key-vec up-fn]
    (go (let [[fkey & rkey] key-vec
              doc (cl/get-document db (pr-str fkey))
              old (when doc (-> doc :edn-value read-string-safe))
              new (if-not (empty? rkey)
                    (update-in old rkey up-fn)
                    (up-fn old))]
          (cond (and (not doc) new)
                [nil (-> (cl/put-document db {:_id (pr-str fkey) ;; TODO might throw on race condition to creation
                                              :edn-value (pr-str new)})
                         :edn-value
                         read-string-safe
                         (get-in rkey))]

                (not new)
                (do (cl/delete-document db doc) [(get-in old rkey) nil])

                :else
                ((fn trans [doc attempt]
                   (let [old (-> doc :edn-value read-string-safe (get-in rkey))
                         new* (try (cl/update-document db
                                                       doc
                                                       (fn [{v :edn-value :as old}]
                                                         (assoc old
                                                           :edn-value (pr-str (if-not (empty? rkey)
                                                                                (update-in (read-string-safe v) rkey up-fn)
                                                                                (up-fn (read-string-safe v)))))))
                                   (catch clojure.lang.ExceptionInfo e
                                     (if (< attempt 10)
                                       (trans (cl/get-document db (pr-str fkey)) (inc attempt))
                                       (do
                                         (log e)
                                         (.printStackTrace e)
                                         (throw e)))))
                         new (-> new* :edn-value read-string-safe (get-in rkey))]
                     [old new])) doc 0))))))


(defn new-couch-store [db]
  (let [db (if (string? db) (couch db) db)]
    (go (create! db)
        (CouchKeyValueStore. db))))


(comment
  (go (def couch-store (<! (new-couch-store "geschichte"))))

  (go (println (<! (-get-in couch-store ["john"]))))
  (get-in (:db couch-store) ["john"])
  (go (println (<! (-assoc-in couch-store ["john"] 42))))
  (go (println (<! (-update-in couch-store ["john"] inc))))

  (defrecord Test [a])
  (go (println (<! (-assoc-in couch-store ["peter"] (Test. 5)))))

  (binding [*read-opts* {:readers {'konserve.platform.Test
                                   (fn [data] (println "READ:" data))}
                         :default (fn [tag data] (println "DEFAULT:" tag data))}]
    #_(read-string-safe "#konserve.platform.Test{:a 3}")
    (go (println (<! (-get-in couch-store ["peter"])))))

  (go (println (<! (-update-in couch-store ["hans" :a] (fnil inc 0))))))
