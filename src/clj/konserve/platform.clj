(ns konserve.platform
  "Platform specific io operations clj."
  (:use konserve.literals)
  (:require [konserve.protocols :refer [IEDNAsyncKeyValueStore -get-in -assoc-in -update-in]]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go go-loop]]
            [com.ashafa.clutch :refer [couch create!] :as cl]))

(def log println)

(defmethod print-method konserve.literals.TaggedLiteral [v ^java.io.Writer w]
  (.write w (str "#" (:tag v) " " (:value v))))

(defn read-string-safe [tag-table s]
  (when s
    (edn/read-string  {:readers tag-table
                       :default (fn [tag literal]
                                  (konserve.literals.TaggedLiteral. tag literal))}
                      s)))

(defrecord CouchKeyValueStore [db tag-table]
  IEDNAsyncKeyValueStore
  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec]
      (go (get-in (->> fkey
                       pr-str
                       (cl/get-document db)
                       :edn-value
                       (read-string-safe @tag-table))
                  rkey))))
  ;; TODO, cleanup and unify with update-in
  (-assoc-in [this key-vec value]
    (go (let [[fkey & rkey] key-vec
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
                      (do
                        (log e)
                        (.printStackTrace e)
                        (throw e)))))) doc 0)
          nil)))
  (-update-in [this key-vec up-fn]
    (go (let [[fkey & rkey] key-vec
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
                                        (do
                                          (log e)
                                          (.printStackTrace e)
                                          (throw e)))))
                           new* (-> (read-string-safe @tag-table (:edn-value new))
                                    (get-in rkey))]
                       [old* new*])))) doc 0)))))


(defn new-couch-store
  "Constructs a CouchDB store either with name for db or a clutch DB
object and a tag-table atom, e.g. {'namespace.Symbol (fn [val] ...)}."
  [db tag-table]
  (let [db (if (string? db) (couch db) db)]
    (go (create! db)
        (CouchKeyValueStore. db tag-table))))


(comment
  (def couch-store
    (<!! (new-couch-store "geschichte"
                          (atom {'konserve.platform.Test
                                 (fn [data] (println "READ:" data))}))))

  (reset! (:tag-table couch-store) {})

  (<!! (-get-in couch-store ["john"]))
  (get-in (:db couch-store) ["john"])
  (<!! (-assoc-in couch-store ["john"] 42))
  (<!! (-update-in couch-store ["john"] inc))

  (defrecord Test [a])
  (<!! (-assoc-in couch-store ["peter"] (Test. 5)))
  (<!! (-get-in couch-store ["peter"]))

  (<!! (-update-in couch-store ["hans" :a] (fnil inc 0)))
  (<!! (-get-in couch-store ["hans"])))
