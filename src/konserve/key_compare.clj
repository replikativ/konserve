(ns konserve.key-compare
  "Comparator for arbitrary types.")

(defn key-compare
  [k1 k2]
  (cond
    (nil? k1) (if (nil? k2)
                0
                -1)

    (and (number? k1) (number? k2)) (compare k1 k2)

    :else (if (nil? k2)
            1
            (if (or (number? k1) (instance? Comparable k1))
              (let [cmp (compare (.getName (type k1)) (.getName (type k2)))]
                (cond
                  (neg? cmp) -1
                  (pos? cmp) 1
                  :else (compare k1 k2)))
              (compare (pr-str k1) (pr-str k2))))))