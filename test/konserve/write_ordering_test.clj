(ns konserve.write-ordering-test
  "The target blob must be CLOSED before `.new` is moved over it.

   Not observable on Linux by outcome — POSIX renames over an open file — which
   is exactly why it needs pinning here: the ordering can regress and every
   Linux suite stays green, and the first place it shows is a Windows build
   refusing the rename. So the test records the backing's protocol calls and
   asserts the order directly.

   Recorded through a DELEGATING BACKING, not `with-redefs`. A protocol call on
   a record that implements the protocol inline compiles to a direct interface
   call (`InvokeExpr.emitProto`) and never consults the var, so a redef of
   `-create-blob` or `-atomic-move` is simply not seen. `-close` on a
   `FileChannel` is an `extend-type`, goes through the var, and IS seen — which
   is how an earlier version of this test recorded three closes and no move and
   reported a failure that told you nothing."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.impl.storage-layout :as sl]
            [konserve.protocols :as kp])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- fresh-path []
  (str (Files/createTempDirectory "konserve-ordering-" (make-array FileAttribute 0)) "/store"))

(defn- recording-backing
  "The real backing, with `-create-blob` and `-atomic-move` logged to `events`.
   Implements exactly what `BackingFilestore` does — PBackingStore and
   PConditionalWrite — so `io-operation` takes the same path it would unwrapped."
  [real events created]
  (reify
    kp/PConditionalWrite
    (-conditional-write-domain [_] (kp/-conditional-write-domain real))
    sl/PBackingStore
    (-create-blob [_ store-key env]
      (let [blob (sl/-create-blob real store-key env)]
        (swap! created assoc blob store-key)
        (swap! events conj [:create store-key])
        blob))
    (-atomic-move [_ from to env]
      (swap! events conj [:move to])
      (sl/-atomic-move real from to env))
    (-delete-blob [_ store-key env] (sl/-delete-blob real store-key env))
    (-migratable [_ key store-key env] (sl/-migratable real key store-key env))
    (-migrate [_ mk kv s rh wh env] (sl/-migrate real mk kv s rh wh env))
    (-copy [_ from to env] (sl/-copy real from to env))
    (-create-store [_ env] (sl/-create-store real env))
    (-delete-store [_ env] (sl/-delete-store real env))
    (-sync-store [_ env] (sl/-sync-store real env))
    (-keys [_ env] (sl/-keys real env))
    (-blob-exists? [_ store-key env] (sl/-blob-exists? real store-key env))
    (-store-exists? [_ env] (sl/-store-exists? real env))
    (-handle-foreign-key [_ mk s rh wh env] (sl/-handle-foreign-key real mk s rh wh env))))

(deftest target-closed-before-atomic-move
  (testing "rename-mode write: -close on the target precedes -atomic-move onto it"
    (let [path       (fresh-path)
          plain      (connect-fs-store path :opts {:sync? true})
          events     (atom [])
          created    (atom {})
          store      (assoc plain :backing (recording-backing (:backing plain) events created))
          orig-close sl/-close]
      ;; A first value, so the second write has an existing target to hold open.
      (k/assoc-in store [:k] 1 {:sync? true})
      (reset! events [])
      ;; `-close` is on FileChannel via extend-type, so a redef does reach it.
      (with-redefs [sl/-close (fn [blob env]
                                (swap! events conj [:close (get @created blob)])
                                (orig-close blob env))]
        (k/assoc-in store [:k] 2 {:sync? true}))
      (let [ev         @events
            move       (first (filter #(= :move (first %)) ev))
            target-key (second move)
            close-idx  (.indexOf ^java.util.List ev [:close target-key])
            move-idx   (.indexOf ^java.util.List ev move)]
        (is (some? move) (str "a rename-mode write moves .new over the target; events: " (pr-str ev)))
        (is (<= 0 close-idx) (str "the target " target-key " is closed; events: " (pr-str ev)))
        (is (< close-idx move-idx)
            (str "target closed at " close-idx " but moved over at " move-idx "; events: " (pr-str ev))))
      (is (= 2 (k/get-in store [:k] nil {:sync? true})) "and the value still landed")
      (delete-store path))))
