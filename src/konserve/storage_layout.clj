(ns konserve.storage-layout
  "One of these protocols must be implemented by each store to provide low level
  access depending on the low-level storage layout chosen. Stores can support
  multiple layouts.")

(defprotocol Layout1
  (-get-raw [store key])
  (-put-raw [store key blob]))

(defprotocol Layout2
  (-get-raw-meta [store key])
  (-put-raw-meta [store key blob])
  (-get-raw-value [store key])
  (-put-raw-value [store key blob]))
