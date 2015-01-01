(ns konserve.couch
  "CouchDB store implemented with Clutch."
  (:use konserve.literals)
  (:require [konserve.platform :refer [read-string-safe]]
            [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go go-loop]]
            [clojure.edn :as edn]
            [com.ashafa.clutch :refer [couch create!] :as cl]
            [konserve.protocols :refer [IEDNAsyncKeyValueStore
                                        -exists? -get-in -assoc-in -update-in]]))


(defrecord CouchKeyValueStore [db tag-table]
  IEDNAsyncKeyValueStore
  (-exists? [this key]
    (go (try (cl/document-exists? db (pr-str key))
             (catch Exception e
               (ex-info "Could not access edn value."
                        {:type :access-error
                         :key key
                         :exception e})))))
  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec]
      (go (try (get-in (->> fkey
                            pr-str
                            (cl/get-document db)
                            :edn-value
                            (read-string-safe @tag-table))
                       rkey)
               (catch Exception e
                 (ex-info "Could not read edn value."
                          {:type :read-error
                           :key fkey
                           :exception e}))))))
  (-assoc-in [this key-vec value]
    (go (try
          (let [[fkey & rkey] key-vec
                doc (cl/get-document db (pr-str fkey))]
            ((fn trans [doc attempt]
               (try (cond (and (not doc) value)
                          (cl/put-document db {:_id (pr-str fkey)
                                               :edn-value (pr-str (if-not (empty? rkey)
                                                                    (assoc-in nil rkey value)
                                                                    value))})
                          (and (not doc) (not value))
                          nil

                          (not value)
                          (cl/delete-document db doc)

                          :else
                          (cl/update-document db
                                              doc
                                              (fn [{v :edn-value :as old}]
                                                (assoc old
                                                  :edn-value (pr-str (if-not (empty? rkey)
                                                                       (assoc-in (read-string-safe @tag-table v) rkey value)
                                                                       value))))))
                    (catch clojure.lang.ExceptionInfo e
                      (if (< attempt 10)
                        (trans (cl/get-document db (pr-str fkey)) (inc attempt))
                        (throw e))))) doc 0)
            nil)
          (catch Exception e
            (ex-info "Could not write edn value."
                     {:type :write-error
                      :key (first key)
                      :exception e})))))
  (-update-in [this key-vec up-fn]
    (go (try
          (let [[fkey & rkey] key-vec
                doc (cl/get-document db (pr-str fkey))]
            ((fn trans [doc attempt]
               (let [old (->> doc :edn-value (read-string-safe @tag-table))
                     new (if-not (empty? rkey)
                           (update-in old rkey up-fn)
                           (up-fn old))]
                 (cond (and (not doc) new)
                       [nil (get-in (->> (cl/put-document db {:_id (pr-str fkey)
                                                              :edn-value (pr-str new)})
                                         :edn-value
                                         (read-string-safe @tag-table))
                                    rkey)]

                       (and (not doc) (not new))
                       [nil nil]

                       (not new)
                       (do (cl/delete-document db doc) [(get-in old rkey) nil])

                       :else
                       (let [old* (get-in old rkey)
                             new (try (cl/update-document db
                                                          doc
                                                          (fn [{v :edn-value :as old}]
                                                            (assoc old
                                                              :edn-value (pr-str (if-not (empty? rkey)
                                                                                   (update-in (read-string-safe @tag-table v) rkey up-fn)
                                                                                   (up-fn (read-string-safe @tag-table v)))))))
                                      (catch clojure.lang.ExceptionInfo e
                                        (if (< attempt 10)
                                          (trans (cl/get-document db (pr-str fkey)) (inc attempt))
                                          (throw e))))
                             new* (-> (read-string-safe @tag-table (:edn-value new))
                                      (get-in rkey))]
                         [old* new*])))) doc 0))
          (catch Exception e
            (ex-info "Could not write edn value."
                     {:type :write-error
                      :key (first key)
                      :exception e}))))))


(defn new-couch-store
  "Constructs a CouchDB store either with name for db or a clutch DB
object and a tag-table atom, e.g. {'namespace.Symbol (fn [val] ...)}."
  [db tag-table]
  (let [db (if (string? db) (couch db) db)]
    (go (try
          (create! db)
          (CouchKeyValueStore. db tag-table)
          (catch Exception e
            (ex-info "Cannot open CouchDB."
                     {:type :db-error
                      :db db
                      :exception e}))))))



(comment
  (def couch-store
    (<!! (new-couch-store "geschichte"
                          (atom {'konserve.platform.Test
                                 (fn [data] (println "READ:" data))}))))

  (reset! (:tag-table couch-store) {})
  (<!! (-get-in couch-store ["john"]))
  (<!! (-exists? couch-store  "johns"))
  (get-in (:db couch-store) ["john"])
  (<!! (-assoc-in couch-store ["john"] 42))
  (<!! (-update-in couch-store ["john"] inc))

  (defrecord Test [a])
  (<!! (-assoc-in couch-store ["peter"] (Test. 5)))
  (<!! (-get-in couch-store ["peter"]))

  (<!! (-update-in couch-store ["hans" :a] (fnil inc 0)))
  (<!! (-get-in couch-store ["hans"])))
