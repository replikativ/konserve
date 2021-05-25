(ns konserve.compressor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]])
  (:import [net.jpountz.lz4 LZ4FrameOutputStream LZ4FrameInputStream]))

(defrecord NullCompressor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (-deserialize serializer read-handlers bytes))
  (-serialize [_ bytes write-handlers val]
    (-serialize serializer bytes write-handlers val)))

(defrecord UnsupportedCompressor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (throw (ex-info "Unsupported compressor." {:bytes bytes})))
  (-serialize [_ bytes write-handlers val]
    (throw (ex-info "Unsupported compressor." {:bytes bytes}))))

(defrecord Lz4Compressor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (let [lz4-byte (LZ4FrameInputStream. bytes)]
      (-deserialize serializer read-handlers lz4-byte)))
  (-serialize [_ bytes write-handlers val]
    (let [lz4-byte (LZ4FrameOutputStream. bytes)]
      (-serialize serializer lz4-byte write-handlers val)
      (.flush lz4-byte))))

(defn null-compressor [serializer]
  (NullCompressor. serializer))

(defn unsupported-compressor [serializer]
  (UnsupportedCompressor. serializer))

(defn lz4-compressor [serializer]
  (Lz4Compressor. serializer))

(def byte->compressor
  {0 null-compressor
   1 #?(:clj (try
              ;; LZ4 requires native code that breaks the native-image executable atm.
               (if (org.graalvm.nativeimage.ImageInfo/inImageBuildtimeCode)
                 unsupported-compressor
                 lz4-compressor)
               (catch Exception _
                 lz4-compressor))
        :cljs unsupported-compressor)})

(def compressor->byte
  (invert-map byte->compressor))

