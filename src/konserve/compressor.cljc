(ns konserve.compressor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]])
  #?(:clj (:import [net.jpountz.lz4 LZ4FrameOutputStream LZ4FrameInputStream
                    LZ4Factory LZ4FrameOutputStream$BLOCKSIZE
                    LZ4FrameOutputStream$FLG$Bits]
                   [net.jpountz.xxhash XXHashFactory])))

(defrecord NullCompressor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (-deserialize serializer read-handlers bytes))
  (-serialize [_ bytes write-handlers val]
    (-serialize serializer bytes write-handlers val)))

(defrecord UnsupportedLZ4Compressor [serializer]
  PStoreSerializer
  (-deserialize [_ _read-handlers bytes]
    (throw (ex-info "Unsupported LZ4 compressor." {:bytes bytes})))
  (-serialize [_ bytes _write-handlers _val]
    (throw (ex-info "Unsupported LZ4 compressor." {:bytes bytes}))))

(defrecord UnsupportedZstdCompressor [serializer]
  PStoreSerializer
  (-deserialize [_ _read-handlers _bytes]
    (throw (ex-info "zstd compressor unavailable: add com.github.luben/zstd-jni to your dependencies"
                    {:type :konserve/missing-optional-dependency
                     :dependency 'com.github.luben/zstd-jni})))
  (-serialize [_ _bytes _write-handlers _val]
    (throw (ex-info "zstd compressor unavailable: add com.github.luben/zstd-jni to your dependencies"
                    {:type :konserve/missing-optional-dependency
                     :dependency 'com.github.luben/zstd-jni}))))

#?(:clj
   ;; The HIGH compressor, not the fast one.
   ;;
   ;; LZ4FrameOutputStream's convenience constructor uses the fast compressor,
   ;; which on real serialized output is weak enough to be counterproductive:
   ;; measured on boring's CBOR for a 1000-datom vector against fressian's,
   ;; lz4-fast left CBOR 17.3% larger where the RAW gap was 12.4% -- i.e. it
   ;; compressed the tighter input better and WIDENED the difference. lz4-hc
   ;; closes it to +0.2%.
   ;;
   ;; This does not change what compressor byte 1 means. LZ4-HC emits standard
   ;; LZ4 Frame; the unchanged LZ4FrameInputStream reads it, so blobs written by
   ;; older konserve versions stay readable and blobs written now stay readable
   ;; by them. Verified both directions before making the switch.
   ;;
   ;; BLOCK_INDEPENDENCE is required by LZ4FrameOutputStream's own validation
   ;; once an explicit compressor is supplied; the convenience constructor sets
   ;; it implicitly.
   (def ^:private lz4-bits
     (into-array LZ4FrameOutputStream$FLG$Bits
                 [LZ4FrameOutputStream$FLG$Bits/BLOCK_INDEPENDENCE])))

#?(:clj
   (defrecord Lz4Compressor [serializer]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (let [lz4-byte (LZ4FrameInputStream. bytes)]
         (-deserialize serializer read-handlers lz4-byte)))
     (-serialize [_ bytes write-handlers val]
       (let [lz4-byte (LZ4FrameOutputStream.
                       bytes LZ4FrameOutputStream$BLOCKSIZE/SIZE_4MB (long -1)
                       (.highCompressor (LZ4Factory/fastestInstance))
                       (.hash32 (XXHashFactory/fastestInstance))
                       lz4-bits)]
         (-serialize serializer lz4-byte write-handlers val)
         (.flush lz4-byte)))))

(defn null-compressor [serializer]
  (NullCompressor. serializer))

(defn unsupported-lz4-compressor [serializer]
  (UnsupportedLZ4Compressor. serializer))

(defn unsupported-zstd-compressor [serializer]
  (UnsupportedZstdCompressor. serializer))

#?(:clj
   (def ^:private zstd-ctor
     ;; Resolved once, reflectively, so zstd-jni stays optional. delay rather
     ;; than a top-level try so a classpath without it costs nothing at load.
     (delay
       (try (requiring-resolve 'konserve.compressor.zstd/zstd-compressor)
            (catch Throwable _ nil)))))

(defn zstd-compressor
  "A zstd compressor (byte 2). Requires com.github.luben/zstd-jni on the
  classpath; without it this returns a compressor that throws a typed error
  naming the missing dependency, rather than failing at namespace load."
  [serializer]
  #?(:clj (if-let [f @zstd-ctor]
            (f serializer)
            (unsupported-zstd-compressor serializer))
     :cljs (unsupported-zstd-compressor serializer)))

#?(:clj
   (defn lz4-compressor [serializer]
     (Lz4Compressor. serializer)))

#?(:clj
   (defmacro native-image-build?
     "True only while a GraalVM native image is actually being BUILT.

     This used to test `(Class/forName \"org.graalvm.nativeimage.ImageInfo\")`
     alone. That class is present on every GraalVM JDK, not just during an image
     build, so on a GraalVM JVM the test was always true and `byte->compressor`
     mapped compressor id 1 to the UNSUPPORTED lz4 stub. The consequences were
     silent and total: `compressor->byte` could no longer find the real
     lz4-compressor, so writing an lz4 blob died with a NullPointerException
     from `create-header` (nil compressor id -> `byte-array`), and reading an
     existing lz4 blob threw \"Unsupported LZ4 compressor\". LZ4 simply did not
     work on GraalVM, in either direction.

     `inImageBuildtimeCode` is the predicate that actually answers the question,
     and it has to be evaluated at RUNTIME -- the class being on the compile-time
     classpath is exactly the thing that misled the old version. The TODO left in
     the original code proposed precisely this."
     []
     (if (try (Class/forName "org.graalvm.nativeimage.ImageInfo")
              (catch Throwable _ nil))
       `(try (org.graalvm.nativeimage.ImageInfo/inImageBuildtimeCode)
             (catch Throwable _# false))
       false)))

(def byte->compressor
  "Compressor id -> constructor. THE ID IS PERSISTED IN EVERY BLOB HEADER, so
  this map is durable state: an id's meaning must never change and a new
  compressor takes the next free id. Byte 1 stays LZ4 Frame -- switching its
  writer to the high compressor does not change the format it emits."
  {0 null-compressor
   1 #?(:clj (if (native-image-build?)
               unsupported-lz4-compressor
               lz4-compressor)
        :cljs unsupported-lz4-compressor)
   2 zstd-compressor})

(def compressor->byte
  (invert-map byte->compressor))

(defn get-compressor [type]
  (case type
    :lz4 #?(:clj lz4-compressor
            :cljs unsupported-lz4-compressor)
    :zstd zstd-compressor
    null-compressor))
