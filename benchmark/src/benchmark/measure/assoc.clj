(ns benchmark.measure.assoc
  (:require [benchmark.common :refer [setup-store statistics timed transpose plots benchmark]]
            [benchmark.plot :refer [plot]]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [oz.core :as oz]))

(defn time-assoc [store-type n {:keys [sync?] :as opts}] ;; later differentiate between async and sync
  (let [mid-el (int (/ n 2))
        store (setup-store store-type 0)
        times (if sync?
                [(timed (k/assoc store 0 nil opts))
                 (timed (k/assoc store mid-el nil opts))
                 (timed (k/assoc store (dec n) nil opts))]
                [(timed (<!! (k/assoc store 0 nil opts)))
                 (timed (<!! (k/assoc store mid-el nil opts)))
                 (timed (<!! (k/assoc store (dec n) nil opts)))])]
    times))

(defmethod benchmark :assoc [_function stores store-sizes iterations]
  (->> (for [store-type stores
             n store-sizes
             opts [{:sync? true} {:sync? false}]]
         (let [[t-get-first t-get-mid t-get-last] (transpose (repeatedly iterations #(time-assoc store-type n opts)))]
           [(assoc (statistics t-get-first) :store store-type :size n :function :assoc :position :first :sync (:sync? opts))
            (assoc (statistics t-get-mid)   :store store-type :size n :function :assoc :position :mid :sync (:sync? opts))
            (assoc (statistics t-get-last)  :store store-type :size n :function :assoc :position :last :sync (:sync? opts))]))
       doall
       (apply concat)))

;; plots

(defn assoc-time-store-plot [data]
  (plot {:title      "Median Assoc Performance of Different Stores"
         :vals       (sort-by :size data)
         :flattened  (->> data
                          (map (fn [summary] (map (fn [obs] (assoc summary :obs obs))
                                                  (:observations summary))))
                          (apply concat)
                          vec)
         :x          {:key :size :title "Elements in Store"}
         :y          {:key :median :title "Time (in ms)"}
         :color      {:key :store :title "Store"}
         :selections [{:key :position :title "Element Position" :init :mid}
                      {:key :sync :title "Synchronous Execution" :init true}]
         :zero-y     true}))

(defn assoc-time-pos-plot [data]
  (plot {:title      "Median Assoc Performance of Different Element Positions"
         :vals       (sort-by :size data)
         :flattened  (->> data
                          (map (fn [summary] (map (fn [obs] (assoc summary :obs obs))
                                                  (:observations summary))))
                          (apply concat)
                          vec)
         :x          {:key :size :title "Elements in Store"}
         :y          {:key :median :title "Time (in ms)"}
         :color      {:key :position :title "Element Position"}
         :selections [{:key :store :title "Store" :init :memory}
                      {:key :sync :title "Synchronous Execution" :init true}]
         :zero-y     true}))

(defn assoc-time-sync-plot [data]
  (plot {:title      "Median Assoc Performance of Synchronous vs. Asynchronous Execution"
         :vals       (sort-by :size data)
         :flattened  (->> data
                          (map (fn [summary] (map (fn [obs] (assoc summary :obs obs))
                                                  (:observations summary))))
                          (apply concat)
                          vec)
         :x          {:key :size :title "Elements in Store"}
         :y          {:key :median :title "Time (in ms)"}
         :color      {:key :sync :title "Synchronous Execution"}
         :selections [{:key :store :title "Store" :init :memory}
                      {:key :position :title "Element Position" :init :mid}]
         :zero-y     true}))

(defmethod plots :assoc [_function data]
  [["assoc-time-store" (assoc-time-store-plot data)]
   ["assoc-time-pos" (assoc-time-pos-plot data)]
   ["assoc-time-sync" (assoc-time-sync-plot data)]])

(comment

  (def data (benchmark :assoc [:memory :file] [1 10 100] 5))

  (println data)

  (oz/start-server!)
  (oz/view! (assoc-time-store-plot data) :mode :vega)
  (oz/view! (assoc-time-pos-plot data) :mode :vega)
  (oz/view! (assoc-time-sync-plot data) :mode :vega))
