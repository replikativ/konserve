(ns konserve.compressor.zstd
  "Zstandard compressor, compressor byte **2**.

  Loaded reflectively by `konserve.compressor` so that `com.github.luben/zstd-jni`
  stays an OPTIONAL dependency: it ships native binaries for every supported
  platform, and konserve should not put that in the graph of every user for a
  compressor most will never select. If the dependency is absent, byte 2
  resolves to a compressor that throws an actionable message instead of the
  namespace failing to load.

  Why it exists alongside LZ4 (byte 1): measured on boring's CBOR output for a
  1000-datom vector, 21 844 B raw ->

      lz4 (fast)   12 225 B
      lz4-hc        9 014 B
      zstd-3        ~8 800 B, and faster than lz4-hc to produce
      zstd-19       ~7 500 B

  Level 3 is zstd's own default and the right default here: it beats LZ4-HC on
  both ratio and speed."
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]])
  (:import [com.github.luben.zstd ZstdInputStream ZstdOutputStream]))

(defrecord ZstdCompressor [serializer level]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (-deserialize serializer read-handlers (ZstdInputStream. bytes)))
  (-serialize [_ bytes write-handlers val]
    ;; close, not flush. zstd frames carry an epilogue that only close() emits;
    ;; a flushed-but-unclosed frame is truncated and ZstdInputStream rejects it.
    ;; The wrapper owns the wrapper stream, not the caller's stream -- closing a
    ;; ZstdOutputStream does close the underlying one, which is why konserve's
    ;; callers hand this a fresh per-blob stream.
    (let [^ZstdOutputStream o (ZstdOutputStream. bytes (int level))]
      (-serialize serializer o write-handlers val)
      (.close o))))

(defn zstd-compressor
  "A zstd compressor wrapping `serializer`. `level` defaults to 3, zstd's own
  default -- higher levels cost encode time for a few percent of size."
  ([serializer] (zstd-compressor serializer 3))
  ([serializer level] (->ZstdCompressor serializer level)))
