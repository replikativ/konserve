(ns benchmark.measure.get
  (:require [benchmark.common :refer [setup-store statistics timed transpose plots benchmark]]
            [benchmark.plot :refer [plot]]
            [clojure.core.async :refer [<!!] :as async]
            [konserve.core :as k]
            [oz.core :as oz]))

(defn time-get [store-type n {:keys [sync?] :as opts}] ;; later differentiate between async and sync
  (let [mid-el (int (/ n 2))
        store (setup-store store-type n)
        times (if sync?
          [(timed (k/get store 0 nil opts))
           (timed (k/get store mid-el nil opts))
           (timed (k/get store (dec n) nil opts))]
          [(timed (<!! (k/get store 0 nil opts)))
           (timed (<!! (k/get store mid-el nil opts)))
           (timed (<!! (k/get store (dec n) nil opts)))])]
    times))

(defmethod benchmark :get [_function stores store-sizes iterations]
  (->> (for [store-type stores
             n store-sizes
             opts [{:sync? true} {:sync? false}]]
         (let [[t-get-first t-get-mid t-get-last] (transpose (repeatedly iterations #(time-get store-type n opts)))]
           [(assoc (statistics t-get-first) :store store-type :size n :function :get :position :first :sync (:sync? opts))
            (assoc (statistics t-get-mid)   :store store-type :size n :function :get :position :mid :sync (:sync? opts))
            (assoc (statistics t-get-last)  :store store-type :size n :function :get :position :last :sync (:sync? opts))]))
       doall
       (apply concat)
       vec))

(defn get-time-store-plot [data]
  (plot {:title      "Median Get Performance of Different Stores"
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
                      {:key :sync? :title "Synchronous Execution" :init true}]
         :zero-y     true}))

(defn get-time-pos-plot [data]
  (plot {:title      "Median Get Performance of Different Element Positions"
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

(defn get-time-sync-plot [data]
  (plot {:title      "Median Get Performance of Synchronous vs. Asynchronous Execution"
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

(defmethod plots :get [_function data]
  [["get-time-store" (get-time-store-plot data)]
   ["get-time-pos" (get-time-pos-plot data)]
   ["get-time-sync" (get-time-sync-plot data)]])

(comment

  (def data (benchmark :get [:memory :file] [1 10 100] 5))

  (println data)

  (oz/view! (get-time-store-plot data) :mode :vega)
  (oz/view! (get-time-pos-plot data) :mode :vega)
  (oz/view! (get-time-sync-plot data) :mode :vega))