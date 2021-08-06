(ns benchmark.core
  (:require [benchmark.common :refer [plots benchmark]]
            [benchmark.measure.assoc]
            [benchmark.measure.create]
            [benchmark.measure.get]
            [clojure.pprint :refer [pprint]]
            [clojure.set :refer [difference]]
            [clojure.string :refer [join]]
            [clojure.tools.cli :as cli]
            [oz.core :as oz]))

(def all-stores #{:memory :file})
(def all-functions #{:assoc :get :create})
(def output-formats #{:csv :edn :html})

(def cli-options
  [["-t" "--tag TAG" "Add tag to measurements"
    :default #{}
    :assoc-fn (fn [m k v] (assoc m k (conj (get m k) v)))]

   ["-i" "--iterations ITERATIONS"
    (str "Number of iterations of each measurement.")
    :default 5
    :parse-fn read-string
    :validate [nat-int? "Must be a non-negative integer."]]

   ["-f" "--only-function FUNCTION" (str "Function to measure. Available are " all-functions)
    :validate [all-functions (str "Must be one of: " all-functions)]
    :default #{}
    :parse-fn read-string
    :assoc-fn (fn [m k v] (assoc m k (conj (get m k) v)))]
   ["-F" "--except-function FUNCTION" (str "Function not to measure. Available are " all-functions)
    :validate [all-functions (str "Must be one of: " all-functions)]
    :default #{}
    :parse-fn read-string
    :assoc-fn (fn [m k v] (assoc m k (conj (get m k) v)))]

   ["-s" "--only-store STORE" (str "Name of store to measure. Available are " all-stores)
    :validate [all-stores (str "Must be one of: " all-stores)]
    :default #{}
    :assoc-fn (fn [m k v] (assoc m k (conj (get m k) v)))]
   ["-S" "--except-store STORE" (str "Name of store not to measure. Available are " all-stores)
    :validate [all-stores (str "Must be one of: " all-stores)]
    :default #{}
    :parse-fn read-string
    :assoc-fn (fn [m k v] (assoc m k (conj (get m k) v)))]

   ["-n" "--store-sizes VECTOR"
    (str "Numbers of entries in store for which benchmarks should be run. "
         "Must be given as a clojure vector of non-negative integers like '[0 1 10 100]'.")
    :default [0 1 10 100]
    :parse-fn read-string
    :validate [vector? "Must be a vector of non-negative integers."
               #(every? nat-int? %) "Vector must consist of non-negative integers."]]

   ["-o" "--output-format FORMAT"
    (str "Determines how the results will be processed. "
         "Available are " output-formats)
    :default :edn
    :parse-fn read-string
    :validate [output-formats  #(str "Format " % " has not been implemented. "
                                     "Available formats are " output-formats)]]

   ["-h" "--help"]])

(defn print-usage-info [summary]
  (println (str "Usage: clj -M:benchmark [OPTIONS] [path]\n"
                "\n"
                "  Options:\n"
                summary)))

(defn save-measurements-to-file [path measurements]
  (let [path (if (empty? path) "konserve-benchmarks.edn" path)]
    (try
      (spit path measurements)
      (println "Measurements successfully saved to" path)
      (catch Exception e
        (pprint measurements)
        (println "Something went wrong while trying to save measurements to file " path ":")
        (.printStackTrace e)))))

(defn save-measurements-to-csv [path measurements]
  (let [unique-keys (->> (map keys measurements)
                         (apply concat)
                         distinct
                         vec)
        csv (str (join "\t" (map name unique-keys)) "\n"
                 (join "\n" (for [measurement measurements]
                              (join "\t" (map (fn [key] (get measurement key ""))
                                              unique-keys)))))
        path (if (empty? path) "konserve-benchmarks.csv" path)]
    (save-measurements-to-file path csv)))

(defn parse-multi [only except all]
  (let [included (if (not-empty only) only all)]
    (set (map keyword (difference included except)))))

(defn -main [& args]
  (let [{:keys [options errors summary] :as parsed-opts} (cli/parse-opts args cli-options)
        path (first (:arguments parsed-opts))
        {:keys [tag output-format iterations store-sizes]} options
        functions (parse-multi (:only-functions options) (:except-functions options) all-functions)
        stores (parse-multi (:only-stores options) (:except-stores options) all-stores)]
    (cond
      (some? errors)
      (do (println "Errors:" errors)
          (print-usage-info summary))

      (:help options)
      (print-usage-info summary)

      :else (do (println "Run for options:")
                (pprint options)
                (let [measurements (cond->> functions
                                     true            (map (fn [function] (benchmark function stores store-sizes iterations)))
                                     true            (apply concat)
                                     (not-empty tag) (map (fn [measurement] (assoc measurement :tag (join " " tag))))
                                     true            vec)]

                  (case output-format
                    :csv (save-measurements-to-csv path measurements)
                    :edn (save-measurements-to-file path measurements)
                    :html (run! (fn [[function data]]
                                  (run! (fn [[name plot]]
                                          (let [filename (str name (when (not-empty tag) (str "-" (join "-" tag))) ".html")]
                                            (save-measurements-to-file filename (oz/html plot {:mode :vega}))))
                                        (plots function data)))
                                (group-by :function measurements))
                    (do (pprint measurements)
                        (println "Unknown output format. Results printed.")))))))

  (shutdown-agents))

(comment
  (let [functions [:get :assoc]
        stores [:memory :file]
        store-sizes [1]
        iterations 1
        tag #{"tag"}
        output-format :edn
        path "res.edn"
        measurements (cond->> functions
                       true            (map (fn [function] (benchmark function stores store-sizes iterations)))
                       true            (apply concat)
                       (not-empty tag) (map (fn [measurement] (assoc measurement :tag (join " " tag))))
                       true            vec)]

    (case output-format
      :csv (save-measurements-to-csv path measurements)
      :edn (save-measurements-to-file path measurements)
      :html (run! (fn [[function data]]
                    (for [[name plot] (plots function data)]
                      (let [filename (str name "-" (join "-" tag) ".html")]
                        (oz/export! plot filename))))
                  (group-by :function measurements))
      (do (pprint measurements)
          (println "Unknown output format. Results printed.")))))
