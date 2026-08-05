(ns konserve.mmap
  "**EXPERIMENTAL.** Navigate a filestore value in place, without reading it.

  A konserve blob written by the boring serializer is ordinary CBOR sitting at
  a known offset in a file, so it can be memory-mapped and walked with
  `boring.nav` — reaching one key without materialising the rest, and without
  faulting in the pages that hold the parts you skipped.

      (require '[konserve.mmap :as kmm] '[boring.nav :as nav])

      (kmm/with-mmap-value [c store \"customers\"]
        (nav/value (get-in c [\"customer-137\" \"name\"])))

  ## Why this is its own namespace

  `boring.mmap` touches `java.lang.foreign`, which is final in JDK 22.
  `konserve.filestore` must keep loading on older JVMs, so this cannot be a
  require there — and it is resolved dynamically here for the same reason, so
  that merely loading this namespace does not fail on JDK 21. The failure
  arrives when you call it, naming the reason.

  ## What it requires of the blob, and why it refuses otherwise

  Three header bytes have to line up, and all three are checked rather than
  assumed:

  - **serializer 3 (boring).** Fressian and clj-cbor blobs are not navigable;
    reading them as CBOR would not error, it would return nonsense.
  - **compressor 0** and **encryptor 0**. A compressed or encrypted blob has
    to be decompressed whole before anything can navigate it, which is exactly
    the cost this exists to avoid.

  It THROWS rather than falling back to an ordinary read. A silent fallback
  would hide the one thing a caller came here for — they would get correct
  answers at full cost and no way to tell. `konserve.core/get` is the
  fallback, and it is one line away.

  ## Lifetime

  The mapping dies with the macro's body. Nothing derived from the cursor may
  escape it: the arena is closed on the way out and touching a cursor
  afterwards throws a typed FFM error rather than reading freed memory. This
  is why the API is a macro and not a function returning a cursor — the scope
  is the contract, and a `get-cursor` that handed one back would make
  use-after-free a thing callers had to remember.

  ## Status

  Experimental. The shape of this — filestore-only, macro-scoped — may change,
  and an in-memory variant that works for every backend is a separate
  question: konserve's read path already slices the value bytes out, so that
  one needs no offset at all, but it saves only decode and not IO."
  (:require [konserve.impl.defaults :refer [key->store-key]]
            [konserve.impl.storage-layout :refer [header-size]]
            [konserve.serializers :as ser])
  (:import [java.io File FileInputStream]))

(def ^:private boring-serializer-byte
  "Byte 3 in the blob header. Read from the registry rather than written as a
  literal, so it cannot drift from `konserve.serializers`."
  (some (fn [[b k]] (when (= :BoringSerializer k) b)) ser/byte->key))

(defn- read-header
  "The blob's 20-byte header, or nil if the file is too short to have one."
  ^bytes [^File f]
  (when (>= (.length f) header-size)
    (with-open [in (FileInputStream. f)]
      (let [b (byte-array header-size)]
        (when (= header-size (.read in b)) b)))))

(defn- be-int
  "The big-endian 32-bit int at `off`. That is how `create-header` writes
  meta-size, at bytes 4-7."
  ^long [^bytes b ^long off]
  (loop [i 0 acc 0]
    (if (= i 4)
      acc
      (recur (inc i) (+ (* acc 256) (bit-and (aget b (+ off i)) 0xff))))))

(defn value-location
  "`[path offset]` for the value of `key` in a filestore, or a thrown
  explanation of why it cannot be navigated.

  Public because it is the useful half on its own: a caller who wants to hand
  the offset to something other than `boring.mmap` — a ranged reader, a
  checksum — needs exactly this and not a cursor."
  [store key]
  (let [base  (or (:base (:backing store))
                  (throw (ex-info (str "konserve.mmap: this store has no :base, "
                                       "so it is not a filestore. Only the "
                                       "filestore keeps values in files this "
                                       "can map.")
                                  {:type :konserve/not-a-filestore})))
        f     (File. (str base "/" (key->store-key key)))
        _     (when-not (.exists f)
                (throw (ex-info (str "konserve.mmap: no blob for key " (pr-str key)
                                     " at " (.getPath f))
                                {:type :konserve/key-not-found :key key
                                 :path (.getPath f)})))
        hdr   (or (read-header f)
                  (throw (ex-info (str "konserve.mmap: " (.getPath f) " is shorter "
                                       "than a " header-size "-byte header")
                                  {:type :konserve/malformed-blob
                                   :path (.getPath f) :size (.length f)})))
        [_ sb cb eb] (map #(bit-and (aget hdr (int %)) 0xff) (range 4))]
    ;; All three are refused rather than worked around -- see the namespace
    ;; docstring on why a silent fallback would be worse than an error.
    (when-not (= sb boring-serializer-byte)
      (throw (ex-info (str "konserve.mmap: this blob was written by serializer "
                           sb " (" (get ser/byte->key sb) "), not "
                           boring-serializer-byte " (:BoringSerializer). Only "
                           "boring's output is CBOR a cursor can walk; reading "
                           "another codec's bytes as CBOR would return nonsense "
                           "rather than fail. Use konserve.core/get.")
                      {:type :konserve/not-navigable :serializer sb :key key})))
    (when-not (zero? cb)
      (throw (ex-info (str "konserve.mmap: this blob is compressed (compressor "
                           cb "), and a compressed blob must be decompressed "
                           "whole before anything can navigate it -- which is "
                           "the cost this avoids. Encoding is per BLOB, not per "
                           "store, so values written while no compressor was "
                           "configured stay navigable; this one was not. Use "
                           "konserve.core/get for it.")
                      {:type :konserve/not-navigable :compressor cb :key key})))
    (when-not (zero? eb)
      (throw (ex-info (str "konserve.mmap: this blob is encrypted (encryptor "
                           eb "). Same reason as compression: it must be "
                           "decrypted whole first. Use konserve.core/get.")
                      {:type :konserve/not-navigable :encryptor eb :key key})))
    [(.getPath f) (+ header-size (be-int hdr 4))]))

(defn navigable?
  "Whether `key`'s blob can be navigated in place, without throwing.

  ENCODING IS PER BLOB, not per store. A store's serializer and compressor are
  applied to what it writes NEXT; blobs already on disk keep the encoding they
  were written with, and every read dispatches on that blob's own header. So a
  store that switches to boring holds a mix, indefinitely and by design --
  nothing needs migrating.

  Which makes a mixed store the normal case rather than an error, and asking
  cheaper than catching:

      (if (navigable? store k)
        (with-mmap-value [c store k] (nav/value (get-in c path)))
        (get-in (k/get store k nil {:sync? true}) path))

  Reads only the 20-byte header."
  [store key]
  (try (boolean (value-location store key))
       (catch Exception _ false)))

(defn mmap-value
  "`[cursor arena]` over the value of `key`. **Prefer `with-mmap-value`**,
  which closes the arena for you; this is here for a caller who genuinely
  needs to manage the lifetime themselves.

  The caller MUST close the arena, and nothing derived from the cursor may be
  used afterwards."
  ([store key] (mmap-value store key nil))
  ([store key opts]
   (let [[path offset] (value-location store key)
         open! (try (requiring-resolve 'boring.mmap/mmap-source)
                    (catch Throwable t
                      (throw (ex-info (str "konserve.mmap needs boring.mmap, "
                                           "which requires JDK 22+ for "
                                           "java.lang.foreign. This JVM is "
                                           (.feature (Runtime/version)) ".")
                                      {:type :konserve/mmap-unavailable
                                       :jdk (.feature (Runtime/version))}
                                      t))))]
     (open! path (assoc opts :offset offset)))))

(defmacro with-mmap-value
  "Bind `binding` to a `boring.nav` cursor over the value of `key`, and close
  the mapping after `body`.

      (with-mmap-value [c store \"customers\"]
        (nav/value (get-in c [\"customer-137\" \"name\"])))

  Do not let the cursor, or anything derived from it, escape the body."
  [[binding store key & [opts]] & body]
  `(let [[c# arena#] (mmap-value ~store ~key ~opts)]
     (with-open [a# arena#]
       (let [~binding c#]
         ~@body))))
