(ns konserve.tests.tiered
  (:require [clojure.core.async :refer [go <! timeout alts! promise-chan put! close!]]
            [clojure.test :refer [is testing]]
            [konserve.core :as k]
            [konserve.protocols :as protocols]
            [konserve.tiered :as tiered]
            [konserve.compliance-test :refer [async-compliance-test] :as ct]
            [superv.async :refer [<?-]]))

#?(:clj
   (defn test-tiered-compliance-sync [frontend-store backend-store]
     (let [store (clojure.core.async/<!! (tiered/connect-tiered-store frontend-store backend-store))]
       (ct/compliance-test store))))

#?(:clj
   (defn test-tiered-fenced-write-through-sync
     "A fenced write through a `:write-through` tiered store must leave the tiers
      AGREEING.

      Revisions belong to the store that minted them, and the two tiers mint their
      own — so forwarding the caller's `:expected-revision` to the frontend made it
      reject a write the backend had just accepted. The rejection was caught and
      logged, the caller was told the fenced write succeeded, and every later
      `:frontend-first` read returned the PRE-WRITE value indefinitely. Using the
      fence created the incoherence it exists to prevent, in the one configuration
      `-conditional-write-domain` explicitly advertises as supported."
     [frontend-store backend-store]
     (let [store (clojure.core.async/<!! (tiered/connect-tiered-store
                                          frontend-store backend-store
                                          {:write-policy :write-through
                                           :read-policy  :frontend-first
                                           :sync? false}))
           opts  {:sync? true}]
       (k/assoc store :fenced {:v 1} opts)
       (is (= {:v 1} (k/get store :fenced nil opts)))
       (let [r (k/revision store :fenced opts)]
         (k/assoc store :fenced {:v 2} (assoc opts :expected-revision r))
         (is (= {:v 2} (k/get store :fenced nil opts))
             "the tiered read must not serve the pre-write value")
         (is (= {:v 2} (k/get backend-store :fenced nil opts)) "backend")
         ;; The revision-sensitive write invalidates rather than pretending the
         ;; frontend participates in the backend's revision stream. The read
         ;; above returns the backend value and warms asynchronously.
         (is (contains? #{nil {:v 2}} (k/get frontend-store :fenced nil opts))
             "frontend is invalidated or has already been warmed"))
       (let [r (k/revision store :fenced opts)]
         (k/update-in store [:fenced] (fn [v] (assoc v :v 3)) (assoc opts :expected-revision r))
         (is (= {:v 3} (k/get store :fenced nil opts)) "same for update-in"))

       (testing "a stale frontend cannot independently recompute a fenced update"
         (k/assoc store :drift 0 opts)
         (k/assoc backend-store :drift 10 opts) ; another process advances truth
         (let [r (k/revision store :drift opts)]
           (k/update store :drift inc (assoc opts :expected-revision r)))
         (is (= 11 (k/get backend-store :drift nil opts)) "backend applied inc to 10")
         (is (= 11 (k/get store :drift nil opts))
             "tiered read cannot return the frontend's stale 0 incremented to 1"))

       (testing "a nested fenced assoc does not retain stale outer fields"
         (k/assoc store :nested {:v 0 :backend-generation 0} opts)
         (k/assoc backend-store :nested {:v 10 :backend-generation 1} opts)
         (let [r (k/revision store :nested opts)]
           (k/assoc-in store [:nested :v] 11 (assoc opts :expected-revision r)))
         (is (= {:v 11 :backend-generation 1}
                (k/get store :nested nil opts))))

       (testing "an older asynchronous cache fill cannot outlive a fenced write"
         (k/assoc backend-store :fill-race {:v 1} opts)
         (let [populate-started (promise)
               allow-populate (promise)
               delayed-frontend
               (reify protocols/PEDNKeyValueStore
                 (-exists? [_ key call-opts]
                   (protocols/-exists? frontend-store key call-opts))
                 (-get-meta [_ key call-opts]
                   (protocols/-get-meta frontend-store key call-opts))
                 (-get-in [_ key-vec not-found call-opts]
                   (protocols/-get-in frontend-store key-vec not-found call-opts))
                 (-update-in [_ key-vec meta-up-fn up-fn call-opts]
                   (protocols/-update-in frontend-store key-vec meta-up-fn up-fn call-opts))
                 (-assoc-in [_ key-vec meta-up-fn val call-opts]
                   (when (and (= key-vec [:fill-race]) (= val {:v 1}))
                     (deliver populate-started true)
                     (when (= ::timeout (deref allow-populate 5000 ::timeout))
                       (throw (ex-info "timed out waiting to release cache fill" {}))))
                   (protocols/-assoc-in frontend-store key-vec meta-up-fn val call-opts))
                 (-dissoc [_ key call-opts]
                   (protocols/-dissoc frontend-store key call-opts)))
               race-store (clojure.core.async/<!!
                           (tiered/connect-tiered-store
                            delayed-frontend backend-store
                            {:write-policy :write-through
                             :read-policy :frontend-first
                             :sync? false}))]
           (try
             (is (= {:v 1} (k/get race-store :fill-race nil opts)))
             (is (= true (deref populate-started 5000 false))
                 "the old value is waiting to enter the frontend")
             (let [revision (k/revision race-store :fill-race opts)
                   write (future
                           (k/assoc race-store :fill-race {:v 2}
                                    (assoc opts :expected-revision revision)))]
               ;; The backend commits before the tiered store repairs its cache.
               ;; Release the deliberately delayed OLD fill only after that point.
               (is (loop [attempts 500]
                     (cond
                       (= {:v 2} (k/get backend-store :fill-race nil opts)) true
                       (zero? attempts) false
                       :else (do (Thread/sleep 2) (recur (dec attempts)))))
                   "the fenced backend write committed")
               (deliver allow-populate true)
               (is (not= ::timeout (deref write 5000 ::timeout))
                   "the tiered write finishes repairing its cache")
               (is (= 1 @(:cache-generation race-store)) "the write advances the cache generation")
               (is (nil? (k/get frontend-store :fill-race nil opts)) "the old frontend value was invalidated")
               (is (= {:v 2} (k/get race-store :fill-race nil opts))
                   "the completed write cannot be followed by a stale cache fill"))
             (finally
               (deliver allow-populate true)))))

       ;; The shared contract covers stale tokens, create-if-absent, update-in,
       ;; with-revision result shapes, and refusal of conditional multi-assoc.
       (ct/conditional-write-compliance-test store))))

(defn test-tiered-deep-async
  "Tests the deep read miss scenario in a tiered store, ensuring that a value
   read from the deepest backend propagates to the frontend and intermediate tiers,
   triggering write hooks where appropriate."
  [frontend-store middle-store deepest-store]
  (let [hook-promise (promise-chan)
        hook (fn [e] (put! hook-promise e))]
    (k/add-write-hook! middle-store :debug-hook hook)
    (go
      (let [tier-2-3  (<! (tiered/connect-tiered-store middle-store deepest-store))
            top-store (<! (tiered/connect-tiered-store frontend-store tier-2-3))]
        (and
         (is (= [nil "found-it"] (<! (k/assoc-in deepest-store [:deep-secret] "found-it"))))
         (is (false? (<! (k/exists? middle-store :deep-secret))) "Should not be in middle tier yet")
         (is (false? (<! (k/exists? frontend-store :deep-secret))) "Should not be in frontend yet")
         (is (= "found-it" (<! (k/get-in top-store [:deep-secret]))) "Should retrieve value from deepest store")
         (is (true? (<! (k/exists? frontend-store :deep-secret))) "Value should now be in frontend store")
         ;; Verify write hook on middle tier
         (let [[val port] (alts! [hook-promise (timeout 2000)])]
           (and
            (is (identical? port hook-promise) "Write hook should have fired on middle tier")
            (is (= {:api-op :assoc-in, :key :deep-secret, :key-vec [:deep-secret], :value "found-it"} val))))
         (is (true? (<! (k/exists? middle-store :deep-secret))) "Value should now be in middle tier"))))))

(defn test-tiered-compliance-async
  [frontend-store backend-store]
  (go
    (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store))]
      (<! (async-compliance-test store)))))

(defn test-write-policies-async [frontend-store backend-store]
  (go
    (testing "Write-through policy"
      (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store
                                                    :write-policy :write-through))]
        (<?- (k/assoc-in store [:test-key] {:value 42}))
        (is (= {:value 42} (<?- (k/get-in frontend-store [:test-key]))))
        (is (= {:value 42} (<?- (k/get-in backend-store [:test-key]))))))

    (testing "Write-around policy"
      ;; Clean up test key from previous test if stores are reused, though usually new stores are passed
      (<?- (k/dissoc frontend-store :test-key))
      (<?- (k/dissoc backend-store :test-key))

      (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store
                                                    :write-policy :write-around))]
        (<?- (k/assoc-in store [:test-key] {:value 44}))
        (is (nil? (<?- (k/get-in frontend-store [:test-key]))))
        (is (= {:value 44} (<?- (k/get-in backend-store [:test-key]))))
        (is (= {:value 44} (<?- (k/get-in store [:test-key]))))))))

(defn test-frontend-only-async
  "Cache mode: :frontend-only writes + :frontend-first reads. Writes/deletes land in
   the frontend only (backend is read-only truth), reads fall through on a miss, and
   -keys reports the frontend."
  [frontend-store backend-store]
  (go
    (testing "Frontend-only (cache) write policy"
      (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store
                                                    :write-policy :frontend-only
                                                    :read-policy :frontend-first))]
        ;; writes land in the frontend ONLY — the backend is never touched
        (<?- (k/assoc-in store [:cached] {:v 1}))
        (is (= {:v 1} (<?- (k/get-in frontend-store [:cached]))) "write went to frontend")
        (is (nil? (<?- (k/get-in backend-store [:cached]))) "backend NOT written")
        (is (= {:v 1} (<?- (k/get-in store [:cached]))) "reads from frontend")
        ;; a key that exists only in the backend (and hasn't been read/cached yet)
        (<?- (k/assoc-in backend-store [:only-backend] {:v 2}))
        ;; -keys reports the FRONTEND (the local cache), not the read-only backend
        (let [ks (set (map :key (<?- (k/keys store))))]
          (is (contains? ks :cached) "keys includes the cached key")
          (is (not (contains? ks :only-backend)) "keys excludes the not-yet-cached backend key"))
        ;; reading it falls through (frontend-first) and warms the frontend cache
        (is (= {:v 2} (<?- (k/get-in store [:only-backend]))) "cold read falls through to backend")
        ;; The warm is deliberately FIRE-AND-FORGET — warming must not block the read — so
        ;; poll for it instead of assuming it has already landed. Asserting immediately is a
        ;; race: it wins on a fast machine and loses under CI load.
        (let [warmed (loop [tries 0]
                       (let [v (<?- (k/get-in frontend-store [:only-backend]))]
                         (cond
                           (some? v)   v
                           (>= tries 100) nil
                           :else (do (<! (timeout 20))
                                     (recur (inc tries))))))]
          (is (= {:v 2} warmed) "read-through warmed the frontend"))
        ;; dissoc removes from the frontend ONLY — a backend copy is untouched
        (<?- (k/assoc-in backend-store [:cached] {:v 99}))
        (<?- (k/dissoc store :cached))
        (is (nil? (<?- (k/get-in frontend-store [:cached]))) "dissoc removed from frontend")
        (is (= {:v 99} (<?- (k/get-in backend-store [:cached]))) "backend copy untouched by dissoc")))))

(defn test-read-policies-async [frontend-store backend-store]
  (go
    (testing "Frontend-first policy"
      (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store
                                                    :read-policy :frontend-first))]
        (<?- (k/assoc-in frontend-store [:test-key] {:source "frontend"}))
        (<?- (k/assoc-in backend-store [:test-key] {:source "backend"}))
        (is (= {:source "frontend"} (<?- (k/get-in store [:test-key]))))))

    (testing "Frontend-only policy"
      (<?- (k/dissoc frontend-store :test-key))
      (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store
                                                    :read-policy :frontend-only))]
        (<?- (k/assoc-in backend-store [:test-key] {:source "backend"}))
        (is (nil? (<?- (k/get-in store [:test-key]))))))))

(defn test-key-operations-async [frontend-store backend-store]
  (go
    (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store))]
      (<?- (k/assoc-in frontend-store [:frontend-key] {:value "frontend"}))
      (<?- (k/assoc-in backend-store [:backend-key] {:value "backend"}))
      (<?- (k/assoc-in store [:tiered-key] {:value "tiered"}))

      (let [all-keys (set (map :key (<?- (k/keys store))))]
        (is (not (contains? all-keys :frontend-key)))
        (is (contains? all-keys :backend-key))
        (is (contains? all-keys :tiered-key)))

      (is (true? (<?- (k/exists? store :frontend-key))))
      (is (true? (<?- (k/exists? store :backend-key))))
      (is (true? (<?- (k/exists? store :tiered-key))))
      (is (false? (<?- (k/exists? store :nonexistent-key))))

      (<?- (k/dissoc store :tiered-key))
      (is (false? (<?- (k/exists? store :tiered-key)))))))

(defn test-binary-operations-async [frontend-store backend-store]
  (go
    (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store
                                                  :write-policy :write-through))
          test-bytes #?(:clj (byte-array (range 10))
                        :cljs #js [0 1 2 3 4 5 6 7 8 9])]

      (is (true? (<?- (k/bassoc store :binary-key test-bytes))))

      (let [read-val (<?- (k/bget store :binary-key
                                  (fn [{:keys [input-stream]}]
                                    (go input-stream))))]
        (is (= (vec (seq test-bytes)) (vec (seq read-val))))))))

(defn test-sync-on-connect-async [frontend-store backend-store]
  (go
    (<?- (k/assoc-in backend-store [:existing-key-1] {:value "existing1"}))
    (<?- (k/assoc-in backend-store [:existing-key-2] {:value "existing2"}))

    (let [tiered-store (<?- (tiered/connect-tiered-store frontend-store backend-store))
          sync-result (<?- (tiered/sync-on-connect tiered-store
                                                   tiered/populate-missing-strategy
                                                   {:sync? false}))]

      (is (= 2 (:synced-keys sync-result)))
      (is (= 0 (:frontend-keys sync-result)))
      (is (= 2 (:backend-keys sync-result)))

      (is (= {:value "existing1"} (<?- (k/get-in frontend-store [:existing-key-1]))))
      (is (= {:value "existing2"} (<?- (k/get-in frontend-store [:existing-key-2]))))

      (is (= {:value "existing1"} (<?- (k/get-in tiered-store [:existing-key-1]))))
      (is (= {:value "existing2"} (<?- (k/get-in tiered-store [:existing-key-2])))))))

(defn test-error-handling [frontend-store backend-store]
  (testing "Error handling"
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (tiered/connect-tiered-store frontend-store backend-store :write-policy :invalid-policy)))
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (tiered/connect-tiered-store frontend-store backend-store :read-policy :invalid-policy)))))
