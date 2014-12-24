(ns konserve.platform
  "Platform specific io operations clj."
  (:require [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go go-loop]]
            [clojure.edn :as edn]))

(def ^:dynamic log println)

(defmethod print-method konserve.literals.TaggedLiteral [v ^java.io.Writer w]
  (.write w (str "#" (:tag v) " " (:value v))))

(defn read-string-safe [tag-table s]
  (when s
    (edn/read-string  {:readers tag-table
                       :default (fn [tag literal]
                                  (konserve.literals.TaggedLiteral. tag literal))}
                      s)))
