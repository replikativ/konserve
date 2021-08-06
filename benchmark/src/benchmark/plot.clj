(ns benchmark.plot
  (:require [clojure.string :as s]))

(defn plot [description]
  (let [{:keys [title vals flattened x y color selections zero-y]} description
        tooltip (str "{\"" (:title x) "\":datum." (name (:key x)) ", "
                     "\"" (:title y) "\":datum." (name (:key y)) ", "
                     "\"" (:title color) "\":datum." (name (:key color)) ", "
                     (s/join ", " (map #(str "\"" (:title %) "\":datum." (name (:key %)))
                                       selections))
                     "}")]
    {:title   {:text  title
               :frame "group"}
     :width   600
     :height  400
     :padding 50

     :signals (conj (mapv #(hash-map :name (name (:key %))
                                     :value (:init %)
                                     :bind {:options (vec (sort (set (map (:key %) vals))))
                                            :input   "select"
                                            :name    (str (:title %) ":   ")})
                          selections)
                    {:name  "errorbars"
                     :value true
                     :bind  {:input "checkbox"
                             :name  "Show error bars:  "}}
                    {:name  "scatterplot"
                     :value true
                     :bind  {:input "checkbox"
                             :name  "Show all observations:  "}})

     :data    [{:name   "results"
                :values vals}
               {:name   "observations"
                :values flattened}
               {:name      "specific"
                :source    "results"
                :transform (conj (mapv #(hash-map :type "filter"
                                                  :expr (str "datum." (name (:key %))
                                                             " === " (name (:key %))))
                                       selections)
                                 {:type "formula", :as "lo", :expr "datum.mean - datum.sd"}
                                 {:type "formula", :as "hi", :expr "datum.mean + datum.sd"})}
               {:name      "specific-observations"
                :source    "observations"
                :transform (mapv #(hash-map :type "filter"
                                            :expr (str "datum." (name (:key %))
                                                       " === " (name (:key %))))
                                 selections)}]

     :scales  [{:name   "x"
                :type   "linear"
                :round  true
                :nice   true
                :zero   true
                :domain {:data "specific", :field (name (:key x))}
                :range  "width"}
               {:name   "y"
                :type   "linear"
                :round  true
                :nice   true
                :zero   zero-y
                :domain {:data "specific-observations" :fields ["obs", "hi", "lo"]}
                :range  "height"}
               {:name   "color"
                :type   "ordinal"
                :range  "category"
                :domain {:data "results" :field (name (:key color))}}]

     :legends [{:fill   "color"
                :title  (:title color)
                :encode {:symbols {:enter {:fillOpacity {:value 1}}}
                         :labels  {:update {:text {:field "value"}}}}}]
     :axes    [{:scale        "x"
                :grid         true
                :orient       "bottom"
                :titlePadding 5
                :title        (:title x)}
               {:scale        "y"
                :grid         true
                :orient       "left"
                :titlePadding 5
                :title        (:title y)}]

     :marks   [{:type "symbol"
                :from {:data "specific-observations"}
                :encode
                {:update {:x       {:scale "x" :field (name (:key x))}
                          :y       {:scale "y" :field "obs"}
                          :shape   "circle"
                          :opacity {:value 0.5}
                          :fill    {:value "transparent"}
                          :stroke  {:scale "color" :field (name (:key color))}
                          :update  {:tooltip {:signal tooltip}}
                          :hover   {:fillOpacity   {:value 0.5}
                                    :strokeOpacity {:value 0.5}}
                          :strokeOpacity {:signal "scatterplot"}}}}
               {:type "group"
                :from {:facet
                       {:name    "series"
                        :data    "specific"
                        :groupby [(name (:key color))]}}
                :marks
                [{:type "rect"
                  :from {:data "series"}
                  :encode
                  {:enter  {:fill  {:scale "color" :field (name (:key color))}
                            :width {:value 1}}
                   :update {:x           {:scale "x" :field (name (:key x)) :band 0.5}
                            :y           {:scale "y" :field "lo"}
                            :y2          {:scale "y" :field "hi"}
                            :fillOpacity {:signal "errorbars"}}}}
                 {:type "rect"
                  :from {:data "series"}
                  :encode
                  {:enter  {:fill   {:scale "color" :field (name (:key color))}
                            :height {:value 1}}
                   :update {:x           {:scale "x" :field (name (:key x)) :band 0.5 :offset -3}
                            :x2          {:scale "x" :field (name (:key x)) :band 0.5 :offset 4}
                            :y           {:scale "y" :field "lo"}
                            :fillOpacity {:signal "errorbars"}}}}
                 {:type "rect"
                  :from {:data "series"}
                  :encode
                  {:enter  {:fill   {:scale "color" :field (name (:key color))}
                            :height {:value 1}}
                   :update {:x           {:scale "x" :field (name (:key x)) :band 0.5 :offset -3}
                            :x2          {:scale "x" :field (name (:key x)) :band 0.5 :offset 4}
                            :y           {:scale "y" :field "hi"}
                            :fillOpacity {:signal "errorbars"}}}}

                 {:type "symbol"
                  :from {:data "series"}
                  :encode
                  {:update {:x      {:scale "x" :field (name (:key x))}
                            :y      {:scale "y" :field (name (:key y))}
                            :fill   {:scale "color" :field (name (:key color))}
                            :update {:tooltip {:signal tooltip}}
                            :hover  {:fillOpacity   {:value 0.5}
                                     :strokeOpacity {:value 0.5}}}}}
                 {:type "line"
                  :from {:data "series"}
                  :encode
                  {:enter  {:x             {:scale "x" :field (name (:key x))}
                            :y             {:scale "y" :field (name (:key y))}
                            :stroke        {:scale "color" :field (name (:key color))}
                            :shape         "circle"
                            :strokeWidth   {:value 2}
                            :strokeOpacity {:value 1}}
                   :update {:tooltip {:signal tooltip}}
                   :hover  {:strokeOpacity {:value 0.5}}}}]}]}))


(defn bar [description]
  (let [{:keys [x y vals title]} description]
    {:description
     "A basic bar chart example, with value labels shown upon mouse hover."
     :title {:text title}
     :width 400
     :height 200
     :data
     [{:name "table"
       :values vals}]
     :axes
     [{:orient "bottom", :scale "xscale" :title (:title x)}
      {:orient "left", :scale "yscale" :title (:title y)}]
     :scales
     [{:name "xscale"
       :type "band"
       :domain {:data "table", :field (name (:key x))}
       :range "width"
       :padding 0.5
       :round true}
      {:name "yscale"
       :domain {:data "table", :field (name (:key y))}
       :nice true
       :range "height"}]
     :padding 5
     :marks
     [{:type "rect"
       :from {:data "table"}
       :encode
       {:enter
        {:x {:scale "xscale", :field (name (:key x))}
         :width {:scale "xscale", :band 1}
         :y {:scale "yscale", :field (name (:key y))}
         :y2 {:scale "yscale", :value 0}}
        :update {:fill {:value "steelblue"}}
        :hover {:fill {:value "red"}}}}
      {:type "text"
       :encode
       {:enter
        {:align {:value "center"}
         :baseline {:value "bottom"}
         :fill {:value "#333"}}
        :update
        {:x {:scale "xscale", :signal (str "tooltip." (name (:key x))), :band 0.5}
         :y {:scale "yscale", :signal (str "tooltip." (name (:key y))), :offset -2}
         :text {:signal (str "tooltip." (name (:key y)))}
         :fillOpacity
         [{:test "datum === tooltip", :value 0} {:value 1}]}}}]
     :signals
     [{:name "tooltip"
       :value {}
       :on
       [{:events "rect:mouseover", :update "datum"}
        {:events "rect:mouseout", :update "{}"}]}]}))
