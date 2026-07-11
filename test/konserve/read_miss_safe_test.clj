(ns konserve.read-miss-safe-test
  "Locks down the round-trip counts of io-operation on a PReadMissSafe backing.

   A miss-safe backing reports an absent key cleanly from the read itself, so the
   -blob-exists? probe (an S3 HEAD) is redundant whenever we touch the blob anyway:
   reads, non-overwrite writes (update-in / update / nested assoc-in / bassoc), and
   deletes. This test uses a minimal in-memory backing that COUNTS -blob-exists?
   and -delete-blob and asserts the probe count stays 0 on a miss-safe backing,
   while a plain (non-miss-safe) backing still probes on the same ops."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.impl.defaults :refer [connect-default-store]]
            [konserve.impl.storage-layout :as sl
             :refer [PBackingStore PBackingBlob PBackingLock PReadMissSafe store-key-not-found-ex]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [superv.async :refer [go-try-]])
  (:import [java.io ByteArrayInputStream]))

;; ---------------------------------------------------------------------------
;; Minimal in-memory backing. state: {store-key -> {:header :meta :value}}.
;; -create-blob is side-effect-free (no materialization) — the miss-safe property
;; the default filestore lacks. -read-header throws store-key-not-found-ex on an
;; absent key (the PReadMissSafe contract).
;; ---------------------------------------------------------------------------

;; Blob/backing methods must be sync/async-agnostic. A bare `go-try-` in a
;; defrecord method returns a raw channel that the SYNC caller's `<?-` does not
;; unwrap (it isn't lexically sync-translated), so wrap every method body in
;; async+sync — exactly like the real backends do.
(defn- async [env body-fn]
  (async+sync (:sync? env) *default-sync-translation* (go-try- (body-fn))))

(defrecord MemLock []
  PBackingLock
  (-release [_ env] (async env (constantly nil))))

(defrecord MemBlob [state counters store-key pending]
  PBackingBlob
  (-read-header [_ env]
    (async env #(if-let [e (get @state store-key)]
                  (:header e)
                  (throw (store-key-not-found-ex store-key)))))
  (-read-meta   [_ _ env]           (async env #(:meta  (get @state store-key))))
  (-read-value  [_ _ env]           (async env #(:value (get @state store-key))))
  (-read-binary [_ _ locked-cb env]
    (async env #(let [^bytes v (:value (get @state store-key))]
                  (locked-cb {:input-stream (ByteArrayInputStream. v) :size (alength v)}))))
  (-write-header [_ arr env]        (async env #(do (swap! pending assoc :header arr) nil)))
  (-write-meta   [_ arr env]        (async env #(do (swap! pending assoc :meta arr) nil)))
  (-write-value  [_ arr _ env]      (async env #(do (swap! pending assoc :value arr) nil)))
  (-write-binary [_ _ input env]    (async env #(do (swap! pending assoc :value input) nil)))
  (-sync  [_ env] (async env (constantly nil)))
  ;; commit-once: after committing, clear pending so the second -close in
  ;; update-blob's finally (post atomic-move) does not resurrect the .new key.
  (-close [_ env] (async env #(let [p @pending]
                                (when (seq p) (reset! pending {}) (swap! state assoc store-key p))
                                nil)))
  (-get-lock [_ env] (async env #(->MemLock))))

(def ^:private backing-impl
  {:-create-blob   (fn [{:keys [state counters]} sk env]
                     (async env #(->MemBlob state counters sk (atom {}))))
   :-delete-blob   (fn [{:keys [state counters]} sk env]
                     (async env #(do (swap! counters update :delete inc)
                                     (swap! state dissoc sk) true)))
   :-blob-exists?  (fn [{:keys [state counters]} sk env]
                     (async env #(do (swap! counters update :exists inc)
                                     (contains? @state sk))))
   :-migratable    (fn [_ _ _ env] (async env (constantly nil)))
   :-migrate       (fn [_ _ _ _ _ _ env] (async env (constantly nil)))
   :-copy          (fn [{:keys [state]} from to env]
                     (async env #(do (swap! state assoc to (get @state from)) nil)))
   :-atomic-move   (fn [{:keys [state]} from to env]
                     (async env #(do (swap! state (fn [s] (-> s (assoc to (get s from)) (dissoc from)))) nil)))
   :-create-store  (fn [_ env] (async env (constantly nil)))
   :-delete-store  (fn [{:keys [state]} env] (async env #(do (reset! state {}) nil)))
   :-store-exists? (fn [_ env] (async env (constantly true)))
   :-sync-store    (fn [_ env] (async env (constantly nil)))
   :-keys          (fn [{:keys [state]} env] (async env #(vec (keys @state))))
   :-handle-foreign-key (fn [_ _ _ _ _ env] (async env (constantly nil)))})

(defrecord MissSafeBacking [state counters])
(defrecord PlainBacking [state counters])
(extend MissSafeBacking PBackingStore backing-impl)
(extend PlainBacking     PBackingStore backing-impl)
(extend-type MissSafeBacking PReadMissSafe)      ;; marker only

(defn- mk-store [ctor]
  (let [counters (atom {:exists 0 :delete 0})
        ;; :lock-blob? false — an in-memory single-process store needs no blob lock
        ;; (orthogonal to the -blob-exists? probe we're counting).
        store    (connect-default-store (ctor (atom {}) counters)
                                        {:config {:lock-blob? false}
                                         :opts   {:sync? true}})]
    [store counters]))

;; ---------------------------------------------------------------------------

(deftest miss-safe-no-probes
  (testing "PReadMissSafe backing does NO -blob-exists? probe on any op"
    (let [[store counters] (mk-store ->MissSafeBacking)
          probes #(:exists @counters)]

      (testing "full assoc (overwrite): no probe"
        (k/assoc store :a 1 {:sync? true})
        (is (= 0 (probes))))

      (testing "get hit / get miss: no probe (read-first)"
        (is (= 1 (k/get store :a nil {:sync? true})))
        (is (nil? (k/get store :missing nil {:sync? true})))
        (is (= 0 (probes))))

      (testing "update-in (read-modify-write): no probe"
        (k/update-in store [:a] inc {:sync? true})
        (is (= 2 (k/get store :a nil {:sync? true})))
        (is (= 0 (probes))))

      (testing "update-in on absent key (read-first => fresh): no probe"
        (k/update-in store [:fresh] (fnil inc 0) {:sync? true})
        (is (= 1 (k/get store :fresh nil {:sync? true})))
        (is (= 0 (probes))))

      (testing "nested assoc-in (read-modify-write): no probe"
        (k/assoc store :m {:x 1} {:sync? true})
        (k/assoc-in store [:m :y] 2 {:sync? true})
        (is (= {:x 1 :y 2} (k/get store :m nil {:sync? true})))
        (is (= 0 (probes))))

      (testing "bassoc / bget: no probe"
        (k/bassoc store :b (.getBytes "hello") {:sync? true})
        (let [out (atom nil)]
          (k/bget store :b (fn [{:keys [^java.io.InputStream input-stream]}]
                             (reset! out (slurp input-stream)) true)
                  {:sync? true})
          (is (= "hello" @out)))
        (is (= 0 (probes))))

      (testing "no HEAD across read / update-in / nested assoc-in / bassoc"
        (is (= 0 (probes)))))))

(deftest dissoc-keeps-its-probe
  (testing "dissoc probes even on a miss-safe backing — konserve's contract requires
            it to report existed?/false-for-missing, and S3 DELETE cannot."
    (let [[store counters] (mk-store ->MissSafeBacking)]
      (k/assoc store :a 1 {:sync? true})
      (let [before (:exists @counters)]
        (is (true?  (k/dissoc store :a {:sync? true}))    "present key -> true")
        (is (false? (k/dissoc store :gone {:sync? true})) "absent key -> false")
        (is (> (:exists @counters) before) "dissoc probed for the boolean")))))

(deftest dissoc-ignore-existence-opt-in
  (testing ":ignore-existence? true skips the probe on a miss-safe backing (GC fast path)"
    (let [[store counters] (mk-store ->MissSafeBacking)]
      (k/assoc store :a 1 {:sync? true})
      (let [before (:exists @counters)]
        (is (true? (k/dissoc store :a {:sync? true :ignore-existence? true})))
        (is (nil?  (k/get store :a nil {:sync? true})) "key is gone")
        (is (= before (:exists @counters)) "no probe with :ignore-existence?"))
      ;; idempotent: deleting an absent key still returns true, still no probe
      (let [before (:exists @counters)]
        (is (true? (k/dissoc store :already-gone {:sync? true :ignore-existence? true})))
        (is (= before (:exists @counters)) "still no probe on an absent key"))))
  (testing "the hint is IGNORED on a non-miss-safe backing (probe stays for safety)"
    (let [[store counters] (mk-store ->PlainBacking)]
      (k/assoc store :a 1 {:sync? true})
      (let [before (:exists @counters)]
        (k/dissoc store :a {:sync? true :ignore-existence? true})
        (is (> (:exists @counters) before) "plain backing still probes")))))

(deftest gate-plain-backing-still-probes
  (testing "a non-miss-safe backing keeps probing on the same ops (the gate works)"
    (let [[store counters] (mk-store ->PlainBacking)]
      (k/assoc store :a 1 {:sync? true})            ;; overwrite skips probe regardless
      (let [after-assoc (:exists @counters)]
        (k/update-in store [:a] inc {:sync? true})  ;; non-overwrite => must probe here
        (is (> (:exists @counters) after-assoc)
            "plain backing probes on update-in"))
      (k/dissoc store :a {:sync? true})             ;; dissoc probes on plain backing
      (is (pos? (:exists @counters))))))
