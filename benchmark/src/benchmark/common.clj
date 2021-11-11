(ns benchmark.common
  (:require [konserve.core :as k]
            [konserve.filestore :refer [new-fs-store delete-store]]
            [konserve.memory :refer [new-mem-store]]
            [clojure.core.async :refer [<!!] :as async]))

;; Store

(def fs-store-path "/tmp/konserve-fs-bench")

(defmulti get-store
  (fn [type] type))

(defmethod get-store :file [_]
  (delete-store fs-store-path)
  (<!! (new-fs-store fs-store-path)))

(defmethod get-store :memory [_]
  (<!! (new-mem-store)))

(defn setup-store [type n]
  (let [population (range n)
        store (get-store type)]
    (run! #(<!! (k/assoc store % nil))
          population)
    store))


; time measurements


(defmacro timed
  "Evaluates expr. Returns the value of expr and the time in a map."
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     (/ (double (- (. System (nanoTime)) start#)) 1000000.0)))

(defn statistics [times]
  (let [vtimes (vec times)
        n (count vtimes)
        mean (/ (apply + times) n)]

    {:mean mean
     :median (nth (sort times) (int (/ n 2)))
     :sd (->> times
              (map #(* (- % mean) (- % mean)))
              (apply +)
              (* (/ 1.0 n))
              Math/sqrt)
     :observations vtimes}))

;; other

(defn transpose [vec-of-vecs]
  (apply map vector vec-of-vecs))

(defmulti benchmark
  (fn [function _stores _store-sizes _iterations] function))

(defmulti plots
  (fn [function _data] function))
