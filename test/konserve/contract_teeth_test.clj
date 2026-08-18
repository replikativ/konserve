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
