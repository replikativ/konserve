(ns konserve.compressor.zstd
  "Zstandard compressor, compressor byte **2**.

  Loaded reflectively by `konserve.compressor` so that `com.github.luben/zstd-jni`
  stays an OPTIONAL dependency: it ships native binaries for every supported
  platform, and konserve should not put that in the graph of every user for a
  compressor most will never select. If the dependency is absent, byte 2
  resolves to a compressor that throws an actionable message instead of the
  namespace failing to load.

  Why it exists alongside LZ4 (byte 1): the two occupy genuinely different
  points, and after measuring, nothing sensible sits between them.

  On one 512-datom konserve blob, encode time and resulting size:

      lz4 (fast)     fast, weak
      lz4-hc       1602 us -> 4767 B
      zstd-3         69 us -> 2507 B

  zstd-3 is 23x faster than lz4-hc AND roughly half the size, which is why
  konserve's LZ4 stays the FAST compressor rather than the high one: whoever
  selects :lz4 wants speed, and ratio is what this compressor is for. Level 3
  is zstd's own default and the right default here; the higher levels trade
  encode time for a few percent."
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]])
  (:import [com.github.luben.zstd ZstdInputStream ZstdOutputStream]
           [java.io OutputStream]))

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
    (let [^ZstdOutputStream o (ZstdOutputStream. ^OutputStream bytes (int level))]
      (-serialize serializer o write-handlers val)
      (.close o))))

(defn zstd-compressor
  "A zstd compressor wrapping `serializer`. `level` defaults to 3, zstd's own
  default -- higher levels cost encode time for a few percent of size."
  ([serializer] (zstd-compressor serializer 3))
  ([serializer level] (->ZstdCompressor serializer level)))
