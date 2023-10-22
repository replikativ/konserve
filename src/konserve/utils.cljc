(ns konserve.utils
  (:require [clojure.walk]))

(defn invert-map [m]
  (->> (map (fn [[k v]] [v k]) m)
       (into {})))

(defn now []
  #?(:clj (java.util.Date.)
     :cljs (js/Date.)))

(defn meta-update
  "Metadata has following 'edn' format
  {:key 'The stored key'
   :type 'The type of the stored value binary or edn'
   :last-write Date timestamp in milliseconds.}
  Returns the meta value of the stored key-value tupel. Returns metadata if the key
  value not exist, if it does it will update the last-write to date now. "
  [key type old]
  (if (empty? old)
    {:key key :type type :last-write (now)}
    (clojure.core/assoc old :last-write (now))))

(defmacro async+sync
 [sync? async->sync async-code]
 (let [async->sync (if (symbol? async->sync)
                     (or (resolve async->sync)
                         (when-let [_ns (or (get-in &env [:ns :use-macros async->sync])
                                            (get-in &env [:ns :uses async->sync]))]
                           (resolve (symbol (str _ns) (str async->sync)))))
                     async->sync)]
   (assert (some? async->sync))
   `(if ~sync?
      ~(clojure.walk/postwalk (fn [n]
                                (if-not (meta n)
                                  (async->sync n n) ;; primitives have no metadata
                                  (with-meta (async->sync n n)
                                    (update (meta n) :tag (fn [t] (async->sync t t))))))
                              async-code)
      ~async-code)))

(def ^:dynamic *default-sync-translation*
  '{go-try try
    <? do
    go-try- try
    <!- do
    <?- do
    go-locked locked})

(defmacro with-promise [sym & body]
  `(let [~sym (cljs.core.async/promise-chan)]
     ~@body
     ~sym))
