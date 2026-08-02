(ns konserve.binary-test
  "`to-bytes` exists because `bget`'s handle has four shapes across backends and
   platforms, so a test that covers one backend on one platform proves nothing.

   This runs against the memory store on BOTH platforms, the JVM filestore (a
   real `InputStream`, so the draining path rather than the already-bytes
   shortcut). The node filestore — the backend that changes its map KEY depending
   on `:sync?`, and therefore the one that motivated the helper — is covered in
   `konserve.node-filestore-binary-test`, for the build reason noted below."
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer [deftest testing is async]])
            [clojure.core.async :refer [go #?(:clj <!!) <!]]
            [konserve.core :as k]
            [konserve.binary :as kb]
            [konserve.memory :refer [new-mem-store]]
            #?(:clj [clojure.java.io :as io])))

(defn- bytes->vec
  "Payloads as vectors of unsigned ints, so a JVM `byte[]` (signed) and a
   `js/Uint8Array` (unsigned) compare against the same literal."
  [bs]
  (mapv #(bit-and % 0xff)
        #?(:clj (seq bs) :cljs (array-seq bs))))

(def ^:private payload
  "0-255, so the top half is negative as JVM bytes and positive as JS ones —
   which is where a sign-extension mistake in the conversion would show."
  (vec (range 256)))

(defn- ->binary [xs]
  #?(:clj (byte-array (map unchecked-byte xs))
     :cljs (js/Uint8Array.from (into-array xs))))

;; ---------------------------------------------------------------------------
;; JVM

#?(:clj
   (deftest to-bytes-round-trips-on-the-memory-store-both-modes
     (testing "sync yields bytes directly, async yields a channel of them — the
               distinction the factory arity exists to carry, since a locked-cb
               cannot discover its own mode."
       (doseq [opts [{:sync? true} {:sync? false}]]
         (let [take* (if (:sync? opts) identity <!!)
               store (take* (new-mem-store (atom {}) opts))]
           (take* (k/bassoc store :bin (->binary payload) opts))
           (is (= payload (bytes->vec (take* (k/bget store :bin (kb/to-bytes opts) opts))))
               (str "mode " opts)))))))

#?(:clj
   (deftest missing-binary-yields-nil-not-an-error
     (testing "a key holding no binary comes back nil rather than throwing — the
               behaviour bget always had, and a caller distinguishes absent from
               empty with its own bookkeeping."
       (let [opts {:sync? true}
             store (new-mem-store (atom {}) opts)]
         (is (nil? (k/bget store :nope (kb/to-bytes opts) opts)))))))

#?(:clj
   (deftest empty-binary-is-not-confused-with-a-missing-one
     (testing "zero bytes stored is zero bytes read, and is distinct from nil."
       (let [opts {:sync? true}
             store (new-mem-store (atom {}) opts)]
         (k/bassoc store :empty (->binary []) opts)
         (let [r (k/bget store :empty (kb/to-bytes opts) opts)]
           (is (some? r) "an empty binary is present")
           (is (= [] (bytes->vec r))))))))

#?(:clj
   (deftest drains-a-real-input-stream-from-the-filestore
     (testing "the filestore passes an actual InputStream, so this exercises the
               draining branch. The memory store cannot: it hands back bytes and
               takes the shortcut."
       (doseq [opts [{:sync? true} {:sync? false}]]
         (let [take* (if (:sync? opts) identity <!!)
               dir (str (System/getProperty "java.io.tmpdir") "/konserve-bin-test-"
                        (System/nanoTime))
               connect (requiring-resolve 'konserve.filestore/connect-fs-store)
               store (take* (connect dir :opts opts))]
           (try
             (take* (k/bassoc store :bin (->binary payload) opts))
             (is (= payload (bytes->vec (take* (k/bget store :bin (kb/to-bytes opts) opts))))
                 (str "mode " opts))
             (finally
               (doseq [f (reverse (file-seq (io/file dir)))]
                 (io/delete-file f true)))))))))

;; ---------------------------------------------------------------------------
;; ClojureScript
;;
;; The node-filestore cases live in `konserve.node-filestore-binary-test`, not
;; here: the browser and karma builds select namespaces with a negative lookahead
;; on `konserve.node-filestore`, so a require of it from THIS namespace would drag
;; node-only code into a browser build that deliberately excludes it.

#?(:cljs
   (deftest to-bytes-round-trips-on-the-memory-store-cljs
     (async done
            (go
              (let [opts {:sync? false}
                    store (<! (new-mem-store (atom {}) opts))]
                (<! (k/bassoc store :bin (->binary payload) opts))
                (is (= payload
                       (bytes->vec (<! (k/bget store :bin (kb/to-bytes opts) opts)))))
                (done))))))
