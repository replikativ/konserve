(ns konserve.binary
  "Reading a stored binary back as bytes, on every backend and both platforms.

   `bget` hands its callback a map describing a platform-specific handle, and the
   callback IS the scope in which that handle is valid — Konserve still owns the
   backing object, and a streaming view lives only until the callback (or the
   channel it returns) completes. That contract is right, and it is why the handle
   is not simply returned.

   What was missing is the ordinary case. Almost every caller wants the bytes, and
   until now each one re-derived how to get them, because the handle has four
   different shapes:

     backend                      map
     ---------------------------- ------------------------------------------
     filestore (JVM)              {:input-stream <InputStream>}
     node-filestore, :sync? true  {:blob <js/Buffer>}
     node-filestore, async        {:input-stream <fs.ReadStream> :size n}
     indexeddb                    {:input-stream <web ReadableStream>}
     memory                       {:input-stream <the bytes that were bassoc'd>}

   Only ONE of those differences is inherent to the platform: `fs.ReadStream` and
   the WHATWG `ReadableStream` really are different APIs. The rest is this
   library's own inconsistency — the same backend hands back a different KEY
   depending on `:sync?`, and the memory store calls raw bytes an `:input-stream`,
   which survives on the JVM only because `clojure.java.io/copy` happens to accept
   a `byte[]`.

   ## The contract this namespace fixes in place

     `:blob`          bytes, already materialised — nothing to drain
     `:input-stream`  a streaming handle, valid for the callback's extent

   A backend supplies whichever it has, and may supply both when it has bytes
   anyway. `to-bytes` accepts any of them. This is additive: the map was always
   the extension point, so no existing callback that destructures `:input-stream`
   changes behaviour.

   ## What this does NOT replace

   Streaming. `to-bytes` materialises, so it is the wrong tool for a blob larger
   than you want in memory — that is what `:streaming? true` and a hand-written
   callback are for. The raw handle stays available; this is the opt-in for the
   common case, not a narrowing of the contract.

   ## Why it is a factory

   A `locked-cb` must synchronously return a CHANNEL when called asynchronously
   and a plain value when called with `{:sync? true}`, and the callback itself
   cannot tell which. So the mode is supplied up front:

     (k/bget store :k (kb/to-bytes opts) opts)"
  (:require [clojure.core.async :refer [promise-chan put! close!]])
  #?(:clj (:import [java.io InputStream ByteArrayOutputStream])))

#?(:cljs
   (defn- concat-u8
     "Join a JS array of Uint8Arrays into one.

      Written out rather than using `js/Buffer.concat`: Buffer is a Node global,
      and this namespace is loaded in browser builds too. Uint8Array is the one
      byte container both platforms have."
     [chunks]
     (let [total (areduce chunks i acc 0 (+ acc (.-length (aget chunks i))))
           out (js/Uint8Array. total)]
       (loop [i 0 off 0]
         (if (< i (alength chunks))
           (let [c (aget chunks i)]
             (.set out c off)
             (recur (inc i) (+ off (.-length c))))
           out)))))

#?(:cljs
   (defn- drain-web-stream
     "Drain a WHATWG ReadableStream into a promise-chan of one Uint8Array."
     [stream]
     (let [out (promise-chan)
           reader (.getReader stream)
           chunks #js []]
       (letfn [(step []
                 (-> (.read reader)
                     (.then (fn [res]
                              (if (.-done res)
                                (do (put! out (concat-u8 chunks)) (close! out))
                                (do (.push chunks (.-value res)) (step)))))
                     (.catch (fn [e] (put! out e) (close! out)))))]
         (step))
       out)))

#?(:cljs
   (defn- drain-node-stream
     "Drain a Node Readable into a promise-chan of one Uint8Array.

      PAUSED mode — `.read()` in a `\"readable\"` loop — and NOT a `\"data\"`
      handler, which is the obvious version and hangs forever.

      node-filestore calls the `bget` callback from inside its own `\"readable\"`
      listener (see `afread-binary`). Node's rule is that once a `\"readable\"`
      listener exists it controls the flow, and `\"data\"` is emitted only when
      something calls `.read()`. So a `\"data\"` handler registered here never
      fires, `\"end\"` never arrives, and the channel is never delivered: the
      caller waits on a read that has already been buffered for it.

      Absorbing that is a large part of why this helper exists.

      The first drain runs immediately rather than waiting for an event, because
      we are already inside the `\"readable\"` that woke the callback. Note the
      stream is NOT destroyed — node-filestore opened it on the fd konserve is
      managing, and destroying it raises."
     [stream]
     (let [out (promise-chan)
           chunks #js []
           pump (fn pump []
                  (loop []
                    (when-let [c (.read stream)]
                      (.push chunks (js/Uint8Array. c))
                      (recur))))]
       (.on stream "readable" pump)
       (.on stream "error" (fn [e] (put! out e) (close! out)))
       (.on stream "end" (fn [] (put! out (concat-u8 chunks)) (close! out)))
       (pump)
       out)))

#?(:clj
   (defn- drain-input-stream ^bytes [^InputStream in]
     (let [bos (ByteArrayOutputStream.)
           buf (byte-array 65536)]
       (loop []
         (let [n (.read in buf)]
           (when (pos? n)
             (.write bos buf 0 n)
             (recur))))
       (.toByteArray bos))))

(defn- already-bytes?
  "True when `x` needs no draining — it IS the payload."
  [x]
  #?(:clj  (bytes? x)
     ;; js/Buffer is a Uint8Array subclass, so this covers both.
     :cljs (instance? js/Uint8Array x)))

(defn to-bytes
  "Return a `bget` locked-cb that yields the stored bytes — `byte[]` on the JVM,
   `js/Uint8Array` on ClojureScript.

     (k/bget store :my-key (kb/to-bytes opts) opts)

   Yields `nil` for a key that holds no binary, matching `bget`'s own behaviour,
   so a missing blob is distinguishable from an empty one only by the caller's
   own bookkeeping — as it always was.

   With `{:sync? true}` the returned callback produces bytes directly. That is
   possible on every backend that HAS a sync mode: the JVM drains its
   `InputStream` in place, and on ClojureScript the sync-capable backends hand
   back materialised bytes already. A genuine stream in sync mode is a backend
   contract violation and throws rather than silently returning a handle."
  ([] (to-bytes {:sync? false}))
  ([opts]
   (let [sync? (:sync? opts)]
     (fn [{:keys [blob input-stream]}]
       (let [handle (or blob input-stream)]
         (cond
           (nil? handle)
           (if sync? nil (doto (promise-chan) (close!)))

           (already-bytes? handle)
           (if sync? handle (doto (promise-chan) (put! handle)))

           :else
           #?(:clj
              (let [bs (drain-input-stream handle)]
                (if sync? bs (doto (promise-chan) (put! bs))))
              :cljs
              (if sync?
                (throw (ex-info (str "konserve.binary/to-bytes was called with {:sync? true} "
                                     "but the backend handed back a stream, which cannot be "
                                     "drained synchronously in ClojureScript. Use {:sync? false}.")
                                {:type :konserve/sync-stream-unsupported
                                 :handle-type (str (type handle))}))
                (cond
                  (some? (.-getReader handle)) (drain-web-stream handle)
                  (some? (.-on handle)) (drain-node-stream handle)
                  :else (throw (ex-info (str "konserve.binary/to-bytes does not recognise this "
                                             "binary handle. Expected bytes, a WHATWG "
                                             "ReadableStream, or a Node Readable.")
                                        {:type :konserve/unknown-binary-handle
                                         :handle-type (str (type handle))})))))))))))
