(ns konserve.mmap
  "**EXPERIMENTAL.** Navigate a filestore value in place, without reading it.

  A konserve blob written by the boring serializer is ordinary CBOR sitting at
  a known offset in a file, so it can be memory-mapped and walked with
  `boring.nav` — reaching one key without materialising the rest, and without
  faulting in the pages that hold the parts you skipped. `with-mmap-payload`
  exposes a selected byte string or primitive array as a read-only
  `MemorySegment`, with no decode or payload copy.

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
  (:require [konserve.core :as k]
            [boring.nav :as nav]
            [konserve.impl.defaults :refer [key->store-key]]
            [konserve.impl.storage-layout :refer [header-size]]
            [konserve.serializers :as ser])
  (:import [java.io File FileInputStream FileOutputStream]
           [java.lang AutoCloseable Class]
           [java.nio.channels FileChannel]
           [java.nio.file Files Path StandardCopyOption CopyOption OpenOption]))

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
        ^bytes hdr (or (read-header f)
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

;; ---------------------------------------------------------------------------
;; No-copy bulk payload access (EXPERIMENTAL)

(def ^:private typed-payloads
  "The RFC 8746 arrays Boring itself emits. Their payload is always little-endian."
  {77 {:element-type :int16 :element-size 2}
   78 {:element-type :int32 :element-size 4}
   79 {:element-type :int64 :element-size 8}
   85 {:element-type :float32 :element-size 4}
   86 {:element-type :float64 :element-size 8}})

(defn- segment-slice
  "Call MemorySegment.asSlice without naming the JDK 22 class at load time."
  ([segment offset]
   (.invoke (.getMethod (Class/forName "java.lang.foreign.MemorySegment")
                        "asSlice" (into-array Class [Long/TYPE]))
            segment (object-array [(long offset)])))
  ([segment offset length]
   (.invoke (.getMethod (Class/forName "java.lang.foreign.MemorySegment") "asSlice"
                        (into-array Class [Long/TYPE Long/TYPE]))
            segment (object-array [(long offset) (long length)]))))

(defn- open-arena []
  (let [arena-class (Class/forName "java.lang.foreign.Arena")]
    (clojure.lang.Reflector/invokeStaticMethod arena-class "ofShared" (object-array 0))))

(defn- unsigned-byte ^long [source ^long offset]
  (bit-and (long (.at ^org.replikativ.boring.ByteSource source offset)) 0xff))

(defn- cbor-head
  "The definite CBOR head at `offset`, as {:major :argument :next}."
  [source ^long offset]
  (let [size (.size ^org.replikativ.boring.ByteSource source)]
    (when-not (< offset size)
      (throw (ex-info "konserve.mmap: payload head is outside the stored value"
                      {:type :konserve/malformed-payload :offset offset :size size})))
    (let [initial (unsigned-byte source offset)
          major (unsigned-bit-shift-right initial 5)
          info (bit-and initial 0x1f)
          width (case info 24 1, 25 2, 26 4, 27 8, 0)
          next (+ offset 1 width)]
      (when (or (#{28 29 30 31} info) (> next size))
        (throw (ex-info "konserve.mmap: payload has an indefinite, reserved or truncated CBOR head"
                        {:type :konserve/malformed-payload :offset offset :info info :size size})))
      (let [argument
            (case info
              24 (unsigned-byte source (inc offset))
              25 (bit-and (long (.i16 ^org.replikativ.boring.ByteSource source (inc offset)))
                          0xffff)
              26 (bit-and (long (.i32 ^org.replikativ.boring.ByteSource source (inc offset)))
                          0xffffffff)
              27 (let [n (.i64 ^org.replikativ.boring.ByteSource source (inc offset))]
                   (when (neg? n)
                     (throw (ex-info "konserve.mmap: payload length does not fit in a signed long"
                                     {:type :konserve/payload-too-large :offset offset})))
                   n)
              info)]
        {:major major :argument argument :next next}))))

(defn- payload-span
  "Locate the raw bytes addressed by `cursor`, without realising the value."
  [source cursor]
  (when-not (nav/cursor? cursor)
    (throw (ex-info "konserve.mmap: path does not address a navigable value"
                    {:type :konserve/payload-not-found})))
  (let [offset (nav/offset cursor)
        first-head (cbor-head source offset)
        [tag bytes-head]
        (if (= 6 (:major first-head))
          [(:argument first-head) (cbor-head source (:next first-head))]
          [nil first-head])
        spec (when tag (get typed-payloads tag))]
    (when (and tag (nil? spec))
      (throw (ex-info (str "konserve.mmap: CBOR tag " tag
                           " is not a Boring-emitted primitive array")
                      {:type :konserve/not-a-bulk-payload :tag tag})))
    (when-not (= 2 (:major bytes-head))
      (throw (ex-info "konserve.mmap: value is not a byte string or primitive array"
                      {:type :konserve/not-a-bulk-payload :major (:major bytes-head)
                       :tag tag})))
    (let [byte-size (long (:argument bytes-head))
          data-offset (long (:next bytes-head))
          end (+ data-offset byte-size)
          source-size (.size ^org.replikativ.boring.ByteSource source)
          element-size (long (or (:element-size spec) 1))]
      (when (or (< end data-offset) (> end source-size))
        (throw (ex-info "konserve.mmap: bulk payload extends past the stored value"
                        {:type :konserve/malformed-payload :offset data-offset
                         :byte-size byte-size :size source-size})))
      (when-not (zero? (rem byte-size element-size))
        (throw (ex-info "konserve.mmap: primitive-array payload has a partial element"
                        {:type :konserve/malformed-payload :tag tag
                         :byte-size byte-size :element-size element-size})))
      {:offset data-offset
       :byte-size byte-size
       :element-count (quot byte-size element-size)
       :element-size element-size
       :element-type (or (:element-type spec) :uint8)
       :byte-order (when spec :little-endian)
       :tag tag})))

(defn mmap-payload
  "Map a byte string or Boring primitive array without copying its payload.

  `path` addresses a nested value inside the Konserve value. It defaults to
  `[]`, which addresses the value itself. The supported tagged values are the
  five RFC 8746 arrays Boring emits: int16/int32/int64/float32/float64,
  little-endian. Plain byte strings are reported as `:uint8`.

  Returns `[payload arena]`, where `payload` contains `:segment`, `:byte-size`,
  `:element-count`, `:element-size`, `:element-type`, `:byte-order` and `:tag`.
  The segment points directly into the filestore's read-only mapping and is
  valid only until the caller closes `arena`. Prefer `with-mmap-payload`.

  Throws rather than decoding when the path is absent, the value is not a bulk
  payload, or the blob is not mmap-compatible. Requires JDK 22+."
  ([store key] (mmap-payload store key [] nil))
  ([store key path] (mmap-payload store key path nil))
  ([store key path opts]
   (let [[file value-offset] (value-location store key)
         arena (try (open-arena)
                    (catch Throwable t
                      (throw (ex-info (str "konserve.mmap needs JDK 22+ for no-copy payload access. "
                                           "This JVM is " (.feature (Runtime/version)) ".")
                                      {:type :konserve/mmap-unavailable
                                       :jdk (.feature (Runtime/version))}
                                      t))))]
     (try
       (let [map-file! (requiring-resolve 'boring.mmap/mmap-segment)
             segment-source (requiring-resolve 'boring.mmap/segment-source)
             segment (map-file! file arena)
             value-segment (segment-slice segment value-offset)
             source (segment-source value-segment)
             root (nav/root source opts)
             cursor (if (seq path) (get-in root path) root)
             span (payload-span source cursor)
             payload-segment (segment-slice value-segment (:offset span) (:byte-size span))]
         [(assoc span :segment payload-segment :path file
                 :file-offset (+ value-offset (:offset span)))
          arena])
       (catch Throwable t
         (.close ^AutoCloseable arena)
         (throw t))))))

(defmacro with-mmap-payload
  "Bind `binding` to a no-copy bulk-payload descriptor and close its mapping.

      (with-mmap-payload [p store :weights [:layers 0 :key]]
        ;; (:segment p) is a MemorySegment valid only in this body
        (consume! (:segment p)))

  An optional final options map is passed to Boring navigation. Nothing derived
  from `:segment` may escape the body."
  [[binding store key & [path opts]] & body]
  `(let [[payload# arena#] (mmap-payload ~store ~key ~(or path []) ~opts)]
     (with-open [a# ^AutoCloseable arena#]
       (let [~binding payload#]
         ~@body))))

;; ---------------------------------------------------------------------------
;; High-level in-place / splice edits (EXPERIMENTAL)
;;
;; These edit a filestore value WITHOUT decoding the whole thing: a same-length
;; change is poked in place through a memory mapping (the offset index stays
;; valid, only touched pages dirtied); any other change splices only the altered
;; bytes and rewrites the blob crash-safely via `.new` + atomic rename. An
;; ineligible blob -- not boring, compressed, encrypted, or stringref-wrapped --
;; falls back to the ordinary `konserve.core` op, so dispatch is always safe:
;; worst case is a correct answer at full cost. Synchronous; single-writer.
;;
;; CONFIGURE THE STORE FOR EDITING. A value is only eligible when it is boring,
;; uncompressed, unencrypted, AND not stringref-wrapped -- and boring's default
;; opens a stringref namespace on larger values, whose byte lengths depend on
;; everything encoded before them. So an editable store must use a deterministic,
;; stringref-off profile, keyed BY NAME (konserve keys serializers by keyword,
;; not by byte):
;;
;;   (connect-fs-store dir
;;     :serializers {:BoringSerializer
;;                   (konserve.serializers/boring-serializer (boring.core/tag-registry)
;;                                                           {:profile :archival})}
;;     :default-serializer :BoringSerializer
;;     :config {:id (random-uuid)})
;;
;; `:archival` sorts keys and turns stringref off. A store left on boring's
;; default writes stringref blobs, which these ops correctly refuse (falling
;; back), so nothing breaks -- the fast path simply never engages.
;;
;; MEASURED: on a 1.7 MB / 200k-key value, a same-length `update-in!` is ~2.7 ms
;; against ~375 ms for `konserve.core/update-in` (139x), because the whole value
;; is never decoded or re-encoded. The gap widens with value size.
;; ---------------------------------------------------------------------------

(defn- boring-serializer-instance [store]
  (get (:serializers store) (get ser/byte->key boring-serializer-byte)))

(defn- edit-eopts
  "The boring encode options this store writes byte-3 blobs with, forced
  stringref-off (an editable blob is never stringref-wrapped; see below). The
  serializer's `:registry` is carried through so a NEW value the caller supplies
  encodes exactly as the store's own serializer would -- otherwise a custom-typed
  value (records, tagged types) would throw or encode to different bytes than the
  store round-trips with."
  [store]
  (let [ser (boring-serializer-instance store)]
    (assoc (:encode-opts ser) :stringref false :registry (:registry ser))))

(defn- stringref-blob?
  "Whether the value at `voff` opens a stringref namespace (tag 256, `d9 01 00`).
  Such a value's byte lengths depend on everything encoded before it, so editing
  it in place is unsafe; those blobs fall back to a full read."
  [^String fpath ^long voff]
  (with-open [in (FileInputStream. fpath)]
    (.skip in voff)
    (let [b (byte-array 3)]
      (and (= 3 (.read in b))
           (= 0xd9 (bit-and (aget b 0) 0xff))
           (= 0x01 (bit-and (aget b 1) 0xff))
           (= 0x00 (bit-and (aget b 2) 0xff))))))

(defn edit-eligible?
  "Whether `key`'s blob can be edited in place or by splice: navigable (boring,
  uncompressed, unencrypted) AND not stringref-wrapped. Reads at most 23 bytes."
  [store key]
  (try
    (let [[fpath voff] (value-location store key)]
      (not (stringref-blob? fpath voff)))
    (catch Exception _ false)))

(defn- path-of ^Path [^String s] (Path/of s (into-array String [])))

(defn- sync-dir!
  "fsync the directory containing `fpath`, so a create/rename/delete of an entry
  in it is durable. konserve's own write path does this (`filestore/sync-base`);
  the atomic rename is only crash-DURABLE, not merely crash-atomic, once the
  directory entry is synced."
  [^String fpath]
  (let [dir (.getParent (File. fpath))]
    (when dir
      (with-open [fc (FileChannel/open (path-of dir)
                                       (into-array OpenOption []))]
        (.force fc true)))))

(defn- splice-write!
  "Transform the value region of `fpath` (bytes from `voff` to EOF) with `xform`
  and write the blob back crash-safely: `.new` + fsync + atomic rename + parent
  directory fsync. The 20-byte header and the metadata (both before `voff`) are
  copied unchanged."
  [^String fpath ^long voff xform]
  (let [p    (path-of fpath)
        blob (Files/readAllBytes p)
        value (java.util.Arrays/copyOfRange blob (int voff) (alength blob))
        nv   ^bytes (xform value)
        out  (byte-array (+ (int voff) (alength nv)))]
    (System/arraycopy blob 0 out 0 (int voff))
    (System/arraycopy nv 0 out (int voff) (alength nv))
    (let [tmp (str fpath ".new")]
      (with-open [o (FileOutputStream. tmp)]
        (.write o ^bytes out)
        (.force (.getChannel o) true))
      (Files/move (path-of tmp) p
                  (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE
                                          StandardCopyOption/REPLACE_EXISTING]))
      (sync-dir! fpath))
    true))

(defn- resolve-mmap [sym]
  (try (requiring-resolve sym) (catch Throwable _ nil)))

(defn- edit-fn
  "Resolve a `boring.edit` function by name, or throw a clear error naming the
  requirement. `boring.edit` is JDK-9 safe, but it ships in a newer boring than
  konserve's floor; resolving it dynamically keeps this namespace loadable on the
  boring already on the classpath and turns a missing dependency into a message
  rather than a load failure."
  [sym]
  (or (resolve-mmap (symbol "boring.edit" (name sym)))
      (throw (ex-info (str "konserve.mmap: the in-place edit ops need boring.edit, "
                           "which ships in a newer boring than the one on the "
                           "classpath. Upgrade org.replikativ/boring.")
                      {:type :konserve/boring-too-old :fn sym}))))

;; `boring.edit/absent` is the literal keyword `:boring.edit/absent`; comparing
;; against it needs no resolve.
(def ^:private edit-absent :boring.edit/absent)

(defn- recoverable-poke-miss? [e]
  (contains? #{:boring/not-pokeable :boring/path-absent} (:type (ex-data e))))

;; ---- durability -----------------------------------------------------------
;;
;; :rename (default) -- splice writes a `.new` file and atomic-renames it (with a
;;   parent-directory fsync), the crash-safe path konserve already uses.
;; :checked -- edit the blob IN PLACE (no copy) but wrap it in a dirty MARKER: a
;;   sidecar `<blob>.dirty` file is created (and the directory fsynced), the edit
;;   is msynced, then the marker is removed. A crash mid-edit leaves the marker,
;;   so `torn?` reports it and the caller reconstructs. O(1) detection, no value
;;   hashing. For reproducible data.
;; :raw -- edit in place with no marker. Cheapest; relies on nothing.
;;
;; THE MARKER IS A SIDECAR FILE, NOT A HEADER BYTE. konserve's header parser
;; treats ANY nonzero byte in the 20-byte header's bytes 8-19 as the signature of
;; a legacy 8-byte header (`storage-layout/header-not-zero-padded?`), so writing a
;; flag there would make every ordinary `konserve.core/get` read the value from
;; the wrong offset. The sidecar keeps the flag entirely outside the blob.
;;
;; CONCURRENCY. These ops take NO per-key lock, while `konserve.core` serializes
;; writes with a `FileLock` and an in-process lock. Do NOT mix them on the same
;; key concurrently, and do not run two in-place edits on one key at once: an
;; in-place edit mutates live bytes, so a concurrent reader or writer can observe
;; or clobber a half-written value. Single-writer per key is the contract; it is
;; not enforced here.
;;
;; `torn?` IS A MANUAL SIGNAL. Nothing consults it automatically -- a caller using
;; `:checked` is responsible for checking `torn?` on read and reconstructing.

(defn- locked-edit
  "Run `thunk` holding `store`/`key`'s in-process lock -- the SAME per-key lock
  `konserve.core` writes take (`go-locked`) -- so an mmap edit serializes against
  `konserve.core` writes and against other mmap edits on that key. On by default
  (~1 us uncontended, negligible against any file write); `:lock? false` opts out
  for a caller that manages its own exclusion.

  ONLY the mmap-write branch is wrapped, never the ineligible fallback: that
  fallback calls `konserve.core`, which locks the same key, and the registry is
  NOT reentrant -- double-locking one key on one thread would deadlock."
  [store key opts thunk]
  (if (get opts :lock? true)
    (k/locked store key (thunk))
    (thunk)))

(defn- durability [opts] (get opts :durability :rename))

(defn- in-place? [opts] (contains? #{:checked :raw} (durability opts)))

(defn- dirty-file ^String [^String fpath] (str fpath ".dirty"))

(defn- set-dirty! [^String fpath]
  (with-open [o (FileOutputStream. (dirty-file fpath))] (.getFD o))
  (sync-dir! fpath))

(defn- clear-dirty! [^String fpath]
  (Files/deleteIfExists (path-of (dirty-file fpath)))
  (sync-dir! fpath))

(defn torn?
  "Whether `key`'s blob is mid-edit -- an in-place `:checked` edit created the
  dirty marker and a crash prevented its removal. A true here means the value may
  be half-written and should be reconstructed, not trusted. This is a MANUAL
  signal: nothing checks it automatically."
  [store key]
  (let [[fpath _] (value-location store key)]
    (.exists (File. (dirty-file fpath)))))

(defn- with-dirty
  "Run `thunk` between creating and removing the dirty marker when durability is
  `:checked`; otherwise just run it. Guards every in-place edit under `:checked`,
  the same-length poke included."
  [fpath opts thunk]
  (if (= :checked (durability opts))
    (do (set-dirty! fpath)
        (try (thunk) (finally (clear-dirty! fpath))))
    (thunk)))

(defn- read-value
  "The value region of `fpath` (bytes from `voff` to EOF), copied out."
  ^bytes [^String fpath ^long voff]
  (let [blob (Files/readAllBytes (path-of fpath))]
    (java.util.Arrays/copyOfRange blob (int voff) (alength blob))))

(defn- rename-assoc!
  "Set `path`=`v` via the crash-safe rename path (splice only the changed bytes,
  write `.new`, atomic rename, dir fsync). Returns `v`."
  [fpath voff path v eopts]
  (splice-write! fpath voff #((edit-fn 'assoc-in-bytes) % path v (assoc eopts :index :maintain)))
  v)

(defn- rename-update!
  "Apply `f` at `path` via the crash-safe rename path. Returns `[old new]`."
  [fpath voff path f eopts]
  (let [value (read-value fpath voff)
        old0  ((edit-fn 'value-at-path) value path eopts)
        old   (if (= old0 edit-absent) nil old0)
        nv    (f old)]
    (splice-write! fpath voff #((edit-fn 'assoc-in-bytes) % path nv (assoc eopts :index :maintain)))
    [old nv]))

(defn- in-place-splice!
  "Size-changing LEAF replace of `path`=`v` IN PLACE via `boring.mmap/splice!`,
  wrapped in the dirty marker for `:checked`. Returns true, or nil when in-place
  is not possible here (no FFM, or the framed value's index cannot be maintained)
  so the caller can fall back to the rename path."
  [fpath voff path v eopts opts]
  (when-let [splice! (resolve-mmap 'boring.mmap/splice!)]
    (try
      (with-dirty fpath opts #(splice! fpath path v (assoc eopts :offset voff)))
      true
      (catch clojure.lang.ExceptionInfo e
        (when-not (= :boring/unmaintainable-index (:type (ex-data e)))
          (throw e))
        nil))))

(defn assoc-in!
  "Set the value at `key-vec` = `[store-key & path]` to `v`, editing the blob
  without decoding the whole value. A same-length change is poked in place; a
  size-changing LEAF change is spliced -- in place (no copy) when `:durability`
  is `:checked` or `:raw`, else via a crash-safe `.new` + atomic rename; and a
  structural change (a new key) re-encodes only the parent and renames. Falls
  back to `konserve.core/assoc-in` for an ineligible blob. Synchronous; returns
  `v`.

  `:durability` -- `:rename` (default, crash-safe by construction: NEVER mutates
  in place, always writes `.new` + atomic rename + dir fsync, O(file)), `:checked`
  (edits IN PLACE -- poke or splice -- guarded by a dirty marker so a crash is
  detectable via `torn?`; reconstruct on crash), `:raw` (in place, no marker).
  The instant same-length poke and the no-copy splice happen only under `:checked`
  and `:raw`; `:rename` trades that speed for crash-safety by construction.
  In-place needs FFM (JDK 22+); without it, or for a framed value whose index
  cannot be maintained, an in-place mode falls back to a rename for that edit.
  A structural change (new key) always renames -- it re-encodes the parent."
  ([store key-vec v] (assoc-in! store key-vec v {}))
  ([store key-vec v opts]
   (let [k (first key-vec) path (vec (rest key-vec))]
     (if-not (edit-eligible? store k)
       (do (k/assoc-in store key-vec v (assoc opts :sync? true)) v)
       (locked-edit store k opts
                    (fn []
                      (let [eopts (edit-eopts store)
                            [fpath voff] (value-location store k)]
                        (if-not (in-place? opts)
              ;; :rename -- always the crash-safe rename path, no in-place mutation
                          (rename-assoc! fpath voff path v eopts)
              ;; :checked / :raw -- poke (marker-wrapped for :checked), then splice
                          (let [poke! (resolve-mmap 'boring.mmap/poke!)
                                outcome (when (and poke! (seq path))
                                          (try (with-dirty fpath opts
                                                 #(poke! fpath path v (assoc eopts :offset voff)))
                                               ::poked
                                               (catch clojure.lang.ExceptionInfo e
                                                 (case (:type (ex-data e))
                                                   :boring/not-pokeable ::size-change
                                                   :boring/path-absent  ::structural
                                                   (throw e)))))]
                            (cond
                              (= outcome ::poked) v
                              (and (= outcome ::size-change) (in-place-splice! fpath voff path v eopts opts)) v
                              :else (rename-assoc! fpath voff path v eopts)))))))))))

(defn update-in!
  "Apply `f` to the value at `key-vec` = `[store-key & path]`. Under `:checked`/
  `:raw` a same-length result is poked in place (marker-wrapped for `:checked`);
  every other case, and all of `:rename`, takes the crash-safe rename path. Falls
  back to `konserve.core/update-in` for an ineligible blob or an empty path.
  Synchronous; returns `[old new]`.

  NOTE: on a size-changing result under `:checked`/`:raw`, `f` runs twice -- once
  by the in-place poke probe and once by the rename path. Keep `f` pure."
  ([store key-vec f] (update-in! store key-vec f {}))
  ([store key-vec f opts]
   (let [k (first key-vec) path (vec (rest key-vec))]
     (if (or (empty? path) (not (edit-eligible? store k)))
       (k/update-in store key-vec f (assoc opts :sync? true))
       (locked-edit store k opts
                    (fn []
                      (let [eopts (edit-eopts store)
                            [fpath voff] (value-location store k)]
                        (if-not (in-place? opts)
                          (rename-update! fpath voff path f eopts)
                          (let [poke-update! (resolve-mmap 'boring.mmap/poke-update!)]
                            (or (when poke-update!
                                  (try (with-dirty fpath opts
                                         #(poke-update! fpath path f (assoc eopts :offset voff)))
                                       (catch clojure.lang.ExceptionInfo e
                                         (if (recoverable-poke-miss? e) nil (throw e)))))
                                (rename-update! fpath voff path f eopts)))))))))))

(defn dissoc-in!
  "Remove the nested key at the end of `key-vec` = `[store-key & path]`, splicing
  only the parent container. `path` must be non-empty. Falls back to
  `konserve.core/update-in` (dissoc on the parent) for an ineligible blob.
  Synchronous; returns `true`."
  ([store key-vec] (dissoc-in! store key-vec {}))
  ([store key-vec opts]
   (let [k (first key-vec) path (vec (rest key-vec))]
     (when (empty? path)
       (throw (ex-info "konserve.mmap/dissoc-in! needs a nested path; use konserve.core/dissoc for a top-level key"
                       {:type :konserve/bad-argument :key-vec key-vec})))
     (if-not (edit-eligible? store k)
       (do (k/update-in store (into [k] (butlast path))
                        #(dissoc % (last path)) (assoc opts :sync? true))
           true)
       (locked-edit store k opts
                    (fn []
                      (let [eopts (edit-eopts store)
                            [fpath voff] (value-location store k)]
                        (splice-write! fpath voff #((edit-fn 'dissoc-in-bytes) % path (assoc eopts :index :maintain))))))))))
