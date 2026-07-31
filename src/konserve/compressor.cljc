(ns konserve.compressor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]])
  #?(:clj (:import [net.jpountz.lz4 LZ4FrameOutputStream LZ4FrameInputStream])))

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
   ;; LZ4 stays the FAST compressor.
   ;;
   ;; This was briefly switched to the high compressor, on the argument that
   ;; lz4-fast is weak enough on serialized output to be counterproductive --
   ;; measured against fressian it left CBOR 17.3% larger where the raw gap was
   ;; 12.4%, i.e. it compressed the tighter input better and WIDENED the
   ;; difference. That observation is real but the conclusion was wrong,
   ;; because the speed of lz4-hc was never measured. On one 512-datom konserve
   ;; blob, encode:
   ;;
   ;;   lz4-hc   1602 us -> 4767 B
   ;;   zstd-3     69 us -> 2507 B
   ;;
   ;; zstd-3 is 23x faster AND roughly half the size. lz4-hc is dominated on
   ;; both axes at once, so it has no niche: whoever picks :lz4 wants speed,
   ;; and whoever wants ratio should pick :zstd (byte 2).
   (defrecord Lz4Compressor [serializer]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (let [lz4-byte (LZ4FrameInputStream. bytes)]
         (-deserialize serializer read-handlers lz4-byte)))
     (-serialize [_ bytes write-handlers val]
       (let [lz4-byte (LZ4FrameOutputStream. bytes)]
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
