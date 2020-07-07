(ns konserve.utils)

(defn invert-map [m]
  (->> (map (fn [[k v]] [v k]) m)
       (into {})))


