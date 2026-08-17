(ns konserve.compliance-test
  (:require [clojure.core.async :refer [#?(:clj <!!) <! go]]
            [konserve.core :as k]
            [konserve.impl.defaults]
            [konserve.utils :as utils]
            #?(:cljs [cljs.test :refer [is testing]])
            #?(:clj [clojure.test :refer [are is testing]])))

#_(deftype UnknownType [])

#_(:clj (defn exception? [thing]
          (instance? Throwable thing)))

(defn conditional-write-compliance-test
  "The `:expected-revision` contract, for any backend that claims to support it.

      A backend that does NOT implement `PConditionalWrite` passes trivially — that
      is the point of the capability: not supporting fencing is a legitimate state,
      SILENTLY IGNORING a request for it is not. So the one thing checked for such a
      store is that it refuses rather than writes.

      Call this from your backend's test suite with a connected, empty store."
  [store]
  ;; The ASYNC arm is JVM-only: resolving a channel to a value needs `<!!`, which
  ;; ClojureScript has no equivalent of. The sync arm covers cljs, where konserve
  ;; does have synchronous IO (node-filestore, memory) — which is the arm that
  ;; matters there anyway. Written as one function rather than two so the contract
  ;; cannot drift between platforms; it was previously wrapped whole in
  ;; `#?(:clj ...)` while `memory_test` referred it unconditionally, so the cljs
  ;; build simply did not compile.
  (doseq [opts #?(:clj [{:sync? false} {:sync? true}] :cljs [{:sync? true}])
             :let [<!! (if (:sync? opts) identity #?(:clj <!! :cljs identity))
                   k*  (keyword (str "cas-" (if (:sync? opts) "sync" "async")))
                   ;; A rejection arrives DIFFERENTLY in the two arms: synchronously
                   ;; it is thrown, asynchronously konserve delivers the exception
                   ;; as a VALUE on the channel. `(is (thrown? ...))` around `<!!`
                   ;; therefore cannot pass in the async arm — the first version of
                   ;; this test asserted exactly that and failed 3/18 the first time
                   ;; anyone ran it. Normalise, then assert on the type.
                   rejected? (fn [f]
                               (let [r (try (<!! (f)) (catch #?(:clj Exception :cljs js/Error) e e))]
                                 (= :konserve/revision-mismatch
                                    (:type (ex-data r)))))]]
       (if-not (k/conditional-write? store)
         (testing "a store without the capability REFUSES, it does not ignore"
           (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                        (<!! (k/assoc store k* {:v 1} (assoc opts :expected-revision :anything))))))
         (testing "conditional writes"
           (testing "create-if-absent succeeds on a missing key"
             (<!! (k/assoc store k* {:v 1} (assoc opts :expected-revision konserve.impl.defaults/absent)))
             (is (= {:v 1} (<!! (k/get store k* nil opts)))))

           (testing "create-if-absent is rejected once the key exists"
             (is (rejected? #(k/assoc store k* {:v :no} (assoc opts :expected-revision konserve.impl.defaults/absent)))))

           (let [r0 (<!! (k/revision store k* opts))]
             (testing "a write on the revision we read succeeds and MOVES the revision"
               (<!! (k/assoc store k* {:v 2} (assoc opts :expected-revision r0)))
               (is (= {:v 2} (<!! (k/get store k* nil opts))))
               (is (not= r0 (<!! (k/revision store k* opts)))))

             (testing "the same revision a second time is rejected, and writes nothing"
               (is (rejected? #(k/assoc store k* {:v :lost} (assoc opts :expected-revision r0))))
               (is (= {:v 2} (<!! (k/get store k* nil opts)))
                   "the loser must not have overwritten the winner"))

             (testing "update-in is fenced too, and up-fn does not run when rejected"
               (let [ran (atom 0)]
                 (is (rejected? #(k/update-in store [k*] (fn [v] (swap! ran inc) (assoc v :v :lost))
                                              (assoc opts :expected-revision r0))))
                 (is (zero? @ran) "a rejected update must not run the caller's function"))))

           ;; The rejection must leave NO trace. A backing that creates its blob
           ;; before it can compare revisions leaves an empty one behind when the
           ;; comparison fails, and an empty blob is worse than a missing key: it
           ;; is not absent (so `create-if-absent` can never succeed again) and it
           ;; is not readable (a header-size error, not not-found). One ordinary
           ;; conflict on a key that never existed would brick that key forever.
           (testing "a REJECTED write on a missing key leaves the key missing"
             (let [ghost (keyword (str "cas-ghost-" (if (:sync? opts) "sync" "async")))]
               (is (rejected? #(k/assoc store ghost {:v :no}
                                        (assoc opts :expected-revision :a-revision-that-never-existed))))
               (is (false? (<!! (k/exists? store ghost opts)))
                   "the key must not have come into existence")
               (is (= :missing (<!! (k/get store ghost :missing opts)))
                   "and reading it reports not-found rather than raising")
               (testing "so create-if-absent still succeeds afterwards"
                 (<!! (k/assoc store ghost {:v 1} (assoc opts :expected-revision konserve.impl.defaults/absent)))
                 (is (= {:v 1} (<!! (k/get store ghost nil opts)))))))

           (testing "an unconditional write still works"
             (<!! (k/assoc store k* {:v 3} opts))
             (is (= {:v 3} (<!! (k/get store k* nil opts)))))

           (testing "multi-assoc refuses to be made conditional"
             (when (utils/multi-key-capable? store)
               (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                            (<!! (k/multi-assoc store {:cas-m1 1} (assoc opts :expected-revision :x)))))))))))

#?(:clj
   (defn compliance-test [store]
     (doseq [opts [{:sync? false} {:sync? true}]
             :let [<!! (if (:sync? opts) identity <!!)]]

       (testing "Testing the append store functionality."
         (<!! (k/append store :foolog {:bar 42} opts))
         (<!! (k/append store :foolog {:bar 43} opts))
         (is (= (<!! (k/log store :foolog opts))
                '({:bar 42}
                  {:bar 43})))
         (is (= (<!! (k/reduce-log store
                                   :foolog
                                   (fn [acc elem]
                                     (conj acc elem))
                                   []
                                   opts))
                [{:bar 42} {:bar 43}]))
         (let [{:keys [key type last-write]} (<!! (k/get-meta store :foolog nil opts))]
           (are [x y] (= x y)
             :foolog        key
             :append-log    type
             java.util.Date (clojure.core/type last-write))))

       (testing "Test the core API."
         (is (= nil (<!! (k/get store :foo nil opts))))
         (is (false? (<!! (k/exists? store :foo opts))))
         (<!! (k/assoc store :foo :bar opts))
         (is (<!! (k/exists? store :foo opts)))
         (is (= :bar (<!! (k/get store :foo nil opts))))
         (<!! (k/assoc-in store [:foo] :bar2 opts))
         (is (= :bar2 (<!! (k/get store :foo nil opts))))
         (is (= :default (<!! (k/get-in store [:fuu] :default opts))))
         (is (= :bar2 (<!! (k/get store :foo nil opts))))
         (is (= :default (<!! (k/get-in store [:fuu] :default opts))))
         (<!! (k/update-in store [:foo] name opts))
         (is (= "bar2" (<!! (k/get store :foo nil opts))))
         (<!! (k/assoc-in store [:baz] {:bar 42} opts))
         (<!! (k/assoc-in store [:baz :barf] 43 opts))
         (is (= 42 (<!! (k/get-in store [:baz :bar] nil opts))))
         (<!! (k/update-in store [:baz :bar] inc opts))
         (is (= 43 (<!! (k/get-in store [:baz :bar] nil opts))))
         (<!! (k/update-in store [:baz :bar] (fn [x] (+ x 2 3)) opts))
         (is (= 48 (<!! (k/get-in store [:baz :bar] nil opts))))
         (is (= true (<!! (k/dissoc store :foo opts))))
         (is (= false (<!! (k/dissoc store :not-there opts))))
         (is (= nil (<!! (k/get-in store [:foo] nil opts))))
         (<!! (k/bassoc store :binbar (byte-array (range 10)) opts))
         (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                                      (go
                                        (is (= (map byte (slurp input-stream))
                                               (range 10)))
                                        true))
                      opts))
         ;; EVERY DOCUMENTED INPUT SHAPE, not just the byte array. `bassoc`
         ;; promises an InputStream, a File, a String and a byte array, and
         ;; before this only the filestore handled any but the last — every
         ;; other backing mishandled four of them, undetected, because this
         ;; suite only ever passed bytes. konserve-s3 stashes the blob and
         ;; later calls `(.write baos value)`, which needs an array.
         #?(:clj
            (let [expected (map byte (range 10))]
              (<!! (k/bassoc store :bin-stream
                             (java.io.ByteArrayInputStream. (byte-array (range 10))) opts))
              (<!! (k/bget store :bin-stream
                           (fn [{:keys [input-stream]}]
                             (go (is (= expected (map byte (slurp input-stream)))
                                     "bassoc must accept an InputStream")
                                 true))
                           opts))
              (let [f (java.io.File/createTempFile "konserve-compliance" ".bin")]
                (try
                  (with-open [o (java.io.FileOutputStream. f)]
                    (.write o (byte-array (range 10))))
                  (<!! (k/bassoc store :bin-file f opts))
                  (<!! (k/bget store :bin-file
                               (fn [{:keys [input-stream]}]
                                 (go (is (= expected (map byte (slurp input-stream)))
                                         "bassoc must accept a File")
                                     true))
                               opts))
                  (finally (.delete f))))
              (<!! (k/bassoc store :bin-string "hello konserve" opts))
              (<!! (k/bget store :bin-string
                           (fn [{:keys [input-stream]}]
                             (go (is (= "hello konserve" (slurp input-stream))
                                     "bassoc must accept a String")
                                 true))
                           opts))
              ;; cleaned up so the key-listing assertion below still describes
              ;; the same store it always did
              (doseq [kk [:bin-stream :bin-file :bin-string]]
                (<!! (k/dissoc store kk opts)))))
         (let  [list-keys (<!! (k/keys store opts))]
           (are [x y] (= x y)
             #{{:key :baz
                :type :edn}
               {:key :binbar
                :type :binary}
               {:key :foolog
                :type :append-log}}
             ;; `:revision` is dropped for the same reason as `:last-write`: both
             ;; are bookkeeping the store maintains, not part of what the caller
             ;; stored, and neither is stable across the writes this test makes.
             (->> list-keys (map #(clojure.core/dissoc % :last-write :revision)) set)
             true
             (every?
              (fn [{:keys [:last-write]}]
                (= (type (java.util.Date.)) (type last-write)))
              list-keys)))

         (doseq [to-delete [:baz :binbar :foolog]]
           (<!! (k/dissoc store to-delete opts)))

        ;; TODO fix by adding spec to core and cache namespace
         #_(let [params (clojure.core/keys store)
                 corruptor (fn [s k]
                             (if (= (type (k s)) clojure.lang.Atom)
                               (clojure.core/assoc-in s [k] (atom {}))
                               (clojure.core/assoc-in s [k] (UnknownType.))))
                 corrupt (reduce corruptor store params)]
             (is (exception? (<!! (get corrupt :bad))))
             (is (exception? (<!! (get-meta corrupt :bad))))
             (is (exception? (<!! (assoc corrupt :bad 10))))
             (is (exception? (<!! (dissoc corrupt :bad))))
             (is (exception? (<!! (assoc-in corrupt [:bad :robot] 10))))
             (is (exception? (<!! (update-in corrupt [:bad :robot] inc))))
             (is (exception? (<!! (exists? corrupt :bad))))
             (is (exception? (<!! (keys corrupt))))
             (is (exception? (<!! (bget corrupt :bad (fn [_] nil)))))
             (is (exception? (<!! (bassoc corrupt :binbar (byte-array (range 10))))))))

       ;; Optional test for multi-key operations - runs if store supports it
       (when (utils/multi-key-capable? store)
         (testing "Testing multi-key operations"
             ;; Test multi-assoc with flat keys
           (let [result (<!! (k/multi-assoc store {:multi1 42 :multi2 "value"} opts))]
             (is (= result {:multi1 true :multi2 true}))
             (is (= 42 (<!! (k/get store :multi1 nil opts))))
             (is (= "value" (<!! (k/get store :multi2 nil opts)))))

             ;; multi-assoc also takes an ORDERED seq of [k v] pairs — the form used when a
             ;; batch writes immutable values plus a mutable pointer that makes them
             ;; reachable, with the pointer LAST (see konserve.core/multi-assoc).
           (let [batch  [[:ord-a 1] [:ord-b 2] [:ord-root {:refs [:ord-a :ord-b]}]]
                 result (<!! (k/multi-assoc store batch
                                            (k/uniform-meta [[:ord-a 1] [:ord-b 2]]
                                                            {:immutable? true})
                                            opts))]
             (is (= result {:ord-a true :ord-b true :ord-root true}))
             (is (= 1 (<!! (k/get store :ord-a nil opts))))
             (is (= 2 (<!! (k/get store :ord-b nil opts))))
             (is (= {:refs [:ord-a :ord-b]} (<!! (k/get store :ord-root nil opts))))
             ;; per-key meta applied only to the pair-seq keys we named
             (is (true? (:immutable? (<!! (k/get-meta store :ord-a nil opts)))))
             (is (nil? (:immutable? (<!! (k/get-meta store :ord-root nil opts))))
                 "the mutable pointer is not marked immutable"))
           (<!! (k/multi-dissoc store [:ord-a :ord-b :ord-root] opts))

             ;; Test multi-dissoc with existing keys
           (let [result (<!! (k/multi-dissoc store [:multi1 :multi2] opts))]
             (is (= result {:multi1 true :multi2 true}))
             (is (= nil (<!! (k/get store :multi1 nil opts))))
             (is (= nil (<!! (k/get store :multi2 nil opts)))))

             ;; Test multi-dissoc with non-existing keys
           (let [result (<!! (k/multi-dissoc store [:nonexistent1 :nonexistent2] opts))]
             (is (= result {:nonexistent1 false :nonexistent2 false})))

             ;; Test multi-dissoc with mix of existing and non-existing keys
           (<!! (k/multi-assoc store {:multi3 "test3" :multi4 "test4"} opts))
           (let [result (<!! (k/multi-dissoc store [:multi3 :multi4 :nonexistent3] opts))]
             (is (= result {:multi3 true :multi4 true :nonexistent3 false}))
             (is (= nil (<!! (k/get store :multi3 nil opts))))
             (is (= nil (<!! (k/get store :multi4 nil opts)))))

             ;; Test multi-get with existing keys
           (<!! (k/multi-assoc store {:multi5 "value5" :multi6 {:nested "data"} :multi7 123} opts))
           (let [result (<!! (k/multi-get store [:multi5 :multi6 :multi7] opts))]
             (is (= result {:multi5 "value5" :multi6 {:nested "data"} :multi7 123})))

             ;; Test multi-get with some missing keys (sparse map behavior)
           (let [result (<!! (k/multi-get store [:multi5 :nonexistent :multi7] opts))]
             ;; Should only contain found keys
             (is (= result {:multi5 "value5" :multi7 123}))
             ;; Verify missing key is not in result
             (is (not (contains? result :nonexistent))))

             ;; Test multi-get with all missing keys
           (let [result (<!! (k/multi-get store [:missing1 :missing2 :missing3] opts))]
             ;; Should return empty map
             (is (= result {})))

             ;; Test multi-get with empty key list
           (let [result (<!! (k/multi-get store [] opts))]
             ;; Should return empty map
             (is (= result {})))

             ;; Clean up multi-get test keys
           (<!! (k/multi-dissoc store [:multi5 :multi6 :multi7] opts))))

      ;; Optional test for write hooks - runs if store supports it
       (when (utils/write-hooks-capable? store)
         (testing "Testing write hooks"
           (let [hook-events (atom [])]
            ;; Register a hook that captures events
             (k/add-write-hook! store ::test-hook
                                (fn [event]
                                  (swap! hook-events conj event)))

            ;; Test assoc-in triggers hook
             (<!! (k/assoc-in store [:hook-test] {:value 42} opts))
             (is (= 1 (count @hook-events)))
             (is (= :assoc-in (:api-op (first @hook-events))))
             (is (= :hook-test (:key (first @hook-events))))
             (is (= {:value 42} (:value (first @hook-events))))
             (is (= [:hook-test] (:key-vec (first @hook-events))))

            ;; Test update-in triggers hook
             (<!! (k/update-in store [:hook-test :value] inc opts))
             (is (= 2 (count @hook-events)))
             (is (= :update-in (:api-op (second @hook-events))))
             (is (= :hook-test (:key (second @hook-events))))
             (is (= [:hook-test :value] (:key-vec (second @hook-events))))

            ;; Test dissoc triggers hook
             (<!! (k/dissoc store :hook-test opts))
             (is (= 3 (count @hook-events)))
             (is (= :dissoc (:api-op (nth @hook-events 2))))
             (is (= :hook-test (:key (nth @hook-events 2))))

            ;; Test bassoc triggers hook
             (<!! (k/bassoc store :hook-bin (byte-array [1 2 3]) opts))
             (is (= 4 (count @hook-events)))
             (is (= :bassoc (:api-op (nth @hook-events 3))))
             (is (= :hook-bin (:key (nth @hook-events 3))))

            ;; Test multi-assoc triggers hook (if supported)
             (when (utils/multi-key-capable? store)
               (<!! (k/multi-assoc store {:hook-m1 1 :hook-m2 2} opts))
               (is (= 5 (count @hook-events)))
               (is (= :multi-assoc (:api-op (nth @hook-events 4))))
               (is (= {:hook-m1 1 :hook-m2 2} (:kvs (nth @hook-events 4))))
              ;; Clean up multi-assoc keys
               (<!! (k/dissoc store :hook-m1 opts))
               (<!! (k/dissoc store :hook-m2 opts))

              ;; An ORDERED batch must reach the hook VERBATIM, order intact — a sync layer
              ;; relays it in this order so a subscriber applies the mutable pointer last.
               (let [batch [[:hook-o1 1] [:hook-o2 2] [:hook-root 3]]
                     before (count @hook-events)]
                 (<!! (k/multi-assoc store batch opts))
                 (let [ev (nth @hook-events before)]
                   (is (= :multi-assoc (:api-op ev)))
                   (is (= batch (:kvs ev)) "ordered kvs forwarded verbatim")
                   (is (= [:hook-o1 :hook-o2 :hook-root] (mapv first (:kvs ev)))
                       "batch order preserved onto the write hook")))
               (<!! (k/multi-dissoc store [:hook-o1 :hook-o2 :hook-root] opts)))

            ;; Test removing hook - should stop receiving events
             (k/remove-write-hook! store ::test-hook)
             (let [count-before (count @hook-events)]
               (<!! (k/assoc-in store [:hook-after-remove] "test" opts))
               (is (= count-before (count @hook-events))
                   "No new events after hook removal"))

            ;; Clean up
             (<!! (k/dissoc store :hook-bin opts))
             (<!! (k/dissoc store :hook-after-remove opts))))))))

(defn async-compliance-test [store]
  (go
    (and
     (is (= nil (<! (k/get store :foo))))
     (is (= [nil :bar] (<! (k/assoc store :foo :bar))))
     (is (= :bar (<! (k/get store :foo))))
     (is (= [nil :bar2] (<! (k/assoc-in store [:foo] :bar2))))
     (is (= :bar2 (<! (k/get store :foo))))
     (is (= :default (<! (k/get-in store [:fuu] :default))))
     (<! (k/update-in store [:foo] name))
     (is (= "bar2" (<! (k/get store :foo))))
     (<! (k/assoc-in store [:baz] {:bar 42}))
     (is (= (<! (k/get-in store [:baz :bar])) 42))
     (<! (k/update-in store [:baz :bar] inc))
     (is (= (<! (k/get-in store [:baz :bar])) 43))
     (<! (k/update-in store [:baz :bar] #(+ % 2 3)))
     (is (= (<! (k/get-in store [:baz :bar])) 48))
     (<! (k/dissoc store :foo))
     (is (= (<! (k/get-in store [:foo])) nil))

     ;; Optional test for multi-key operations - runs if store supports it
     (if (utils/multi-key-capable? store)
       (do
         ;; Test multi-assoc
         (is (= {:multi1 true :multi2 true}
                (<! (k/multi-assoc store {:multi1 42 :multi2 "value"}))))
         (is (= 42 (<! (k/get store :multi1))))
         (is (= "value" (<! (k/get store :multi2))))

         ;; Test multi-get with existing keys
         (<! (k/multi-assoc store {:multi5 "value5" :multi6 {:nested "data"} :multi7 123}))
         (is (= {:multi5 "value5" :multi6 {:nested "data"} :multi7 123}
                (<! (k/multi-get store [:multi5 :multi6 :multi7]))))

         ;; Test multi-get with some missing keys (sparse map)
         (is (= {:multi5 "value5" :multi7 123}
                (<! (k/multi-get store [:multi5 :nonexistent :multi7]))))

         ;; Test multi-get with all missing keys
         (is (= {} (<! (k/multi-get store [:missing1 :missing2]))))

         ;; Test multi-dissoc
         (is (= {:multi1 true :multi2 true}
                (<! (k/multi-dissoc store [:multi1 :multi2]))))
         (is (= nil (<! (k/get store :multi1))))
         (is (= nil (<! (k/get store :multi2))))

         ;; Clean up
         (<! (k/multi-dissoc store [:multi5 :multi6 :multi7]))
         true)
       true))))
