(ns konserve.platform
  "Platform specific operations for Clojure."
  (:use [konserve.literals])
  (:require [clojure.edn :as edn]))

(defmethod print-method konserve.literals.KTaggedLiteral [v ^java.io.Writer w]
  (.write w (str "#" (:tag v) " " (:value v))))

(defn read-string-safe [tag-table s]
  (when s
    (edn/read-string  {:readers tag-table
                       :default (fn [tag literal]
                                  (konserve.literals.KTaggedLiteral. tag literal))}
                      s)))
