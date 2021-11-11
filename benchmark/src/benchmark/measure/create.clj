(ns benchmark.measure.create
  (:require [benchmark.common :refer [setup-store statistics timed plots benchmark fs-store-path]]
            [benchmark.plot :refer [plot bar]]
            [clojure.core.async :refer [<!!]]
            [konserve.filestore :refer [new-fs-store]]
            [konserve.memory :refer [new-mem-store]]
            [oz.core :as oz]))

(defn time-create [store-type store-size {:keys [sync?] :as opts}]
  (setup-store store-type store-size)
  (case store-type
    :memory (if sync? (timed (new-mem-store opts))
                      (timed (<!! (new-mem-store opts))))
    :file (if sync? (timed (new-fs-store fs-store-path :opts opts))
                    (timed (<!! (new-fs-store fs-store-path :opts opts))))) )

(defmethod benchmark :create [_function stores store-sizes iterations]
  (->> (for [store-type stores
             n store-sizes
             opts [{:sync? true} {:sync? false}]
             :when (or (not= store-type :memory) (zero? n))]
         (let [t-create (repeatedly iterations #(time-create store-type n opts))]
           [(assoc (statistics t-create) :store store-type :size n :function :create)]))
       doall
       (apply concat)
       vec))

(defn create-time-plot [data]
  (plot {:title      "Median Create Performance"
         :vals       (sort-by :size data)
         :flattened  (->> data
                          (map (fn [summary] (map (fn [obs] (assoc summary :obs obs))
                                                  (:observations summary))))
                          (apply concat)
                          vec)
         :x          {:key :size :title "Elements in Store"}
         :y          {:key :median :title "Time (in ms)"}
         :color      {:key :store :title "Store"}
         :selections [{:key :store :title "Store" :init :memory}
                      {:key :sync :title "Synchronous Execution" :init true}]
         :zero-y     true}))

(defn create-empty-time-plot [data]
  (bar {:title      "Median Create Performance of Empty Store"
         :vals       (filter #(= 0 (:size %)) data)
         :x          {:key :store :title "Store"}
         :y          {:key :median :title "Time (in ms)"}
         :zero-y     true}))

(defmethod plots :create [_function data]
  [["create-time" (create-time-plot data)]
   ["create-empty-time" (create-empty-time-plot data)]])

(comment

  (def data (benchmark :create [:memory :file] [0] 10))

  (oz/start-server!)
  (oz/view! (create-time-plot data) :mode :vega)
  (oz/view! (create-empty-time-plot data) :mode :vega)
  
  )
