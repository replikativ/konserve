(ns konserve.node-filestore-binary-test
  "`konserve.binary/to-bytes` against the node filestore — the backend that
   motivated it.

   It is the one that hands back a DIFFERENT map key depending on `:sync?`:
   `{:blob <js/Buffer>}` synchronously and `{:input-stream <fs.ReadStream>}`
   asynchronously. Both shapes are exercised here, because a helper that only
   handled one of them would still leave every caller writing the branch.

   Named `konserve.node-filestore-…` deliberately: the browser and karma builds
   select namespaces with a negative lookahead on `konserve.node-filestore`, so
   this name is excluded from them along with the backend itself."
  (:require [clojure.core.async :refer [go <!]]
            [cljs.test :refer-macros [deftest is testing async]]
            [cljs-node-io.fs :as fs]
            [konserve.core :as k]
            [konserve.binary :as kb]
            [konserve.node-filestore :refer [connect-fs-store]]))

(def ^:private payload
  "0-255, so the top half is negative as JVM bytes and positive as JS ones —
   where a sign-extension mistake in the conversion would show."
  (vec (range 256)))

(defn- ->binary [xs] (js/Uint8Array.from (into-array xs)))

(defn- bytes->vec [bs] (mapv #(bit-and % 0xff) (array-seq bs)))

(deftest to-bytes-drains-an-fs-read-stream
  (testing "the async path hands back an fs.ReadStream. This is the ONE
            difference among the backends that is genuine JavaScript
            fragmentation — a Node Readable and a WHATWG ReadableStream really
            are different APIs — rather than konserve inconsistency.

            Note the stream is drained, not destroyed: node-filestore opens it on
            the fd konserve is managing, and destroying it raises."
    (async done
           (go
             (let [dir "/tmp/konserve-binary-async-test"
                   opts {:sync? false}]
               (fs/rm-rf dir)
               (let [store (<! (connect-fs-store dir :opts opts))]
                 (<! (k/bassoc store :bin (->binary payload) opts))
                 (is (= payload
                        (bytes->vec (<! (k/bget store :bin (kb/to-bytes opts) opts)))))
                 (done)))))))

(deftest to-bytes-takes-the-blob-in-sync-mode
  (testing "the SAME backend passes `:blob` — a js/Buffer, already materialised —
            when synchronous. A Buffer is a Uint8Array subclass, so it needs no
            draining at all, and `to-bytes` returns it directly rather than
            through a channel."
    (let [dir "/tmp/konserve-binary-sync-test"
          opts {:sync? true}]
      (fs/rm-rf dir)
      (let [store (connect-fs-store dir :opts opts)]
        (k/bassoc store :bin (->binary payload) opts)
        (is (= payload
               (bytes->vec (k/bget store :bin (kb/to-bytes opts) opts))))))))

(deftest a-stream-in-sync-mode-is-refused-loudly
  (testing "there is no way to drain a stream synchronously in ClojureScript, so
            a backend handing one back under `{:sync? true}` gets a named error
            rather than the raw handle. Returning the handle would look like it
            worked and fail later, somewhere else."
    (let [cb (kb/to-bytes {:sync? true})
          fake-stream #js {:getReader (fn [] nil)}]
      (is (thrown-with-msg?
           js/Error #"cannot be drained synchronously"
           (cb {:input-stream fake-stream}))))))
