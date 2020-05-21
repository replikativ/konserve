(ns konserve.gc)

(defn sweep!
  "Walk all storage addresses from seq all-addresses, calling delete-fn on
  each key that for which accept-fn returns true, AND is not contained
  in the set addresses."
  [addresses accept-fn all-addresses delete-fn]
  (loop [addrs all-addresses]
    (when-let [address (first addrs)]
      (when (accept-fn address) 
        (delete-fn address))
      (recur (rest addrs)))))

