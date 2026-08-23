(ns konserve.contract-teeth-test
  "Does the conditional-write contract actually CATCH a backend that lies?

   A store's domain is a claim, and `conditional-write-compliance-test` is the
   only thing standing between that claim and a caller who trusts it. Nothing
   proved the suite could tell an honest store from a dishonest one — the domain
   tests elsewhere assert the reported keyword, which is the claim, not the
   behaviour. So: run the suite against a store that declares the STRONGEST
   domain and quietly drops `:expected-revision`, and require that it fails.

   What this cannot do, stated so nobody reads more into it: verify a real
   `:global` claim. That is an assertion about atomicity across machines, and no
   single-process test can witness it. What it does verify is the failure a
   backend author actually makes — declaring a domain and not implementing the
   option — which is the one this suite is here to catch."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.impl.defaults :as defaults]
            [konserve.compliance-test :refer [conditional-write-compliance-test]]
            [konserve.protocols :as p]))

(defn- honest-opts [opts] (dissoc opts :expected-revision))

;; `deftype` with an ILookup that DELEGATES, not `defrecord`: `konserve.core`
;; reads fields off the store it is handed (`:state`, `:lock-registry`, ...), and
;; a record wrapper answers nil for every one of them — which shows up as an NPE
;; deep inside the store rather than as the contract failure under test.
(deftype LyingStore [inner]
  clojure.lang.ILookup
  (valAt [_ k] (get inner k))
  (valAt [_ k nf] (get inner k nf))

  p/PConditionalWrite
  ;; The strongest domain there is, backed by nothing at all.
  (-conditional-write-domain [_] :global)
  (-revision [_ key opts] (p/-revision inner key opts))

  p/PEDNKeyValueStore
  (-exists?   [_ key opts] (p/-exists? inner key opts))
  (-get-meta  [_ key opts] (p/-get-meta inner key opts))
  (-get-in    [_ key-vec not-found opts] (p/-get-in inner key-vec not-found opts))
  ;; the lie: the option is accepted and ignored
  (-update-in [_ key-vec meta-up-fn up-fn opts]
    (p/-update-in inner key-vec meta-up-fn up-fn (honest-opts opts)))
  (-assoc-in  [_ key-vec meta-up-fn val opts]
    (p/-assoc-in inner key-vec meta-up-fn val (honest-opts opts)))
  (-dissoc    [_ key opts] (p/-dissoc inner key opts))

  p/PKeyIterable
  (-keys [_ opts] (p/-keys inner opts)))

(deftest the-contract-catches-a-store-that-only-claims-a-domain
  (testing "a store that declares :global and ignores :expected-revision must
            FAIL the conditional-write contract"
    (let [lying   (LyingStore. (<!! (new-mem-store)))
          reports (atom [])]
      (is (= :global (k/conditional-write-domain lying)) "it claims the strongest domain")
      ;; Capture rather than propagate: these failures are the expected result.
      (binding [clojure.test/report (fn [m] (when (#{:fail :error} (:type m))
                                              (swap! reports conj (:type m))))]
        (conditional-write-compliance-test lying))
      (is (pos? (count @reports))
          (str "the contract must reject a store that only claims to fence; "
               "it reported " (count @reports) " failures"))))

  (testing "and the same suite passes against a store that honours it, so the
            check above is about the lie and not about the suite being noisy"
    (let [honest  (<!! (new-mem-store))
          reports (atom [])]
      (binding [clojure.test/report (fn [m] (when (#{:fail :error} (:type m))
                                              (swap! reports conj m)))]
        (conditional-write-compliance-test honest))
      (is (zero? (count @reports))
          (str "an honest store must pass cleanly: " (pr-str (map :type @reports)))))))

;; ---------------------------------------------------------------------------
;; A claim the layout cannot support
;; ---------------------------------------------------------------------------

;; Only the two methods `-conditional-write-domain` reads are needed, so these
;; are stubs rather than working backings: the question here is what a store
;; PROMISES given a backing and a config, which is answered before any IO.
(deftype SelfFencingBacking []
  p/PSelfConditionalWrite
  p/PConditionalWrite
  (-conditional-write-domain [_] :global))

(deftype LockFencingBacking []
  p/PConditionalWrite
  (-conditional-write-domain [_] :machine))

(defn- domain-of [backing config]
  (k/conditional-write-domain (defaults/map->DefaultStore {:backing backing :config config})))

(deftest a-self-fenced-claim-does-not-survive-a-copy-and-rename-layout
  (testing "`:in-place? false` revokes a SELF-fenced domain"
    ;; Under that layout `update-blob` syncs to `<store-key>.new` and renames it
    ;; into place. The storage layer's precondition is therefore evaluated
    ;; against a key that cannot exist — so it always passes — and the rename
    ;; compares nothing. The write would be reported as fenced with no condition
    ;; ever evaluated, which is the one outcome the capability exists to prevent.
    (is (= :global (domain-of (SelfFencingBacking.) {:in-place? true :lock-blob? true})))
    (is (nil? (domain-of (SelfFencingBacking.) {:in-place? false :lock-blob? true}))
        "a backing that fences itself cannot fence a key it is not writing to"))

  (testing "but a LOCK-based claim survives it, which is the point of the branch"
    ;; konserve holds the lock and evaluates `check-revision!` itself, across the
    ;; write AND the rename, so the layout is irrelevant to that fence. The
    ;; filestore ships `:in-place? false`; hoisting the test above the branch
    ;; would silently disarm the one backend most likely to be run locally.
    (is (= :machine (domain-of (LockFencingBacking.) {:in-place? false :lock-blob? true})))
    (is (nil? (domain-of (LockFencingBacking.) {:in-place? false :lock-blob? false}))
        "without the lock there is no mechanism left, whatever the backing claims")))

(deftest the-filestore-still-declares-a-domain
  ;; The end-to-end guard for the test above: a real filestore, with its real
  ;; default config, must keep fencing.
  (let [dir (str "/tmp/konserve-teeth-" (System/currentTimeMillis))
        store (<!! (connect-fs-store dir))]
    (try
      (is (= false (:in-place? (:config store))) "the filestore is not in-place")
      (is (some? (k/conditional-write-domain store))
          "and is nevertheless fenced, by konserve's lock rather than by itself")
      (conditional-write-compliance-test store)
      (finally (delete-store dir)))))
