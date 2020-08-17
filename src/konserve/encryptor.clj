(ns konserve.encryptor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]]))

(defrecord NullEncryptor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (-deserialize serializer read-handlers bytes))
  (-serialize [_ bytes write-handlers val]
    (-serialize serializer bytes write-handlers val)))

(defn null-encryptor [serializer]
  (NullEncryptor. serializer))

(def byte->encryptor
  {0 null-encryptor})

(def encryptor->byte
  (invert-map byte->encryptor))


