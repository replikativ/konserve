(ns konserve.value-metadata-test
  "The per-write metadata channel: a static `meta` map on `assoc`/`multi-assoc`
   (merged into the stored metadata + forwarded on the write-hook), and a general
   `meta-up-fn` transform on `assoc-in`/`update-in`. Backward-compatible (3/4 arities
   unchanged); `opts` stays last."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]))

(defn- mem []
  (let [cfg {:backend :memory :id (random-uuid)}]
    (k/create-store cfg {:sync? true})
    (k/connect-store cfg {:sync? true})))

(deftest assoc-meta-map
  (testing "assoc 5-arity merges meta into stored metadata + forwards it on the hook"
    (let [store  (mem)
          events (atom [])]
      (k/add-write-hook! store :t (fn [e] (swap! events conj e)))
      ;; mutable (4-arity) — no meta, no :immutable?, hook has no :meta
      (k/assoc store :k1 1 {:sync? true})
      (is (nil? (:immutable? (k/get-meta store :k1 nil {:sync? true}))))
      (is (nil? (:meta (last @events))) "mutable write's hook event has no :meta")
      ;; immutable (5-arity, meta before opts)
      (k/assoc store :k2 2 {:immutable? true} {:sync? true})
      (let [m (k/get-meta store :k2 nil {:sync? true})]
        (is (true? (:immutable? m)) "meta recorded durably")
        (is (= :edn (:type m)) "built fields (:type) preserved — they win on conflict")
        (is (some? (:last-write m)) "built :last-write preserved"))
      (is (= {:immutable? true} (:meta (last @events))) "hook forwards :meta")
      (is (= 2 (k/get store :k2 nil {:sync? true})) "value stored correctly"))))

(deftest multi-assoc-meta-map
  (testing "multi-assoc 4-arity applies meta to EVERY value in the batch"
    (let [store  (mem)
          events (atom [])]
      (k/add-write-hook! store :t (fn [e] (swap! events conj e)))
      (k/multi-assoc store {:a 1 :b 2} {:immutable? true} {:sync? true})
      (is (true? (:immutable? (k/get-meta store :a nil {:sync? true}))))
      (is (true? (:immutable? (k/get-meta store :b nil {:sync? true}))))
      (is (= {:immutable? true} (:meta (last @events))) "batch hook forwards :meta"))))

(deftest assoc-in-meta-up-fn
  (testing "assoc-in 5-arity: meta-up-fn TRANSFORMS the built metadata (the general form)"
    (let [store (mem)]
      (k/assoc-in store [:x] 1
                  (fn [m] (clojure.core/assoc m :immutable? true :tag :node))
                  {:sync? true})
      (let [m (k/get-meta store :x nil {:sync? true})]
        (is (true? (:immutable? m)))
        (is (= :node (:tag m)) "arbitrary metadata via the transform")
        (is (= :edn (:type m)) "default metadata still built (transform composes on top)")))))

(deftest update-in-meta-up-fn
  (testing "update-in 5-arity meta-up-fn transforms metadata while updating the value"
    (let [store (mem)]
      (k/assoc store :c 1 {:sync? true})
      (k/update-in store [:c] inc (fn [m] (clojure.core/assoc m :immutable? true)) {:sync? true})
      (is (= 2 (k/get store :c nil {:sync? true})))
      (is (true? (:immutable? (k/get-meta store :c nil {:sync? true})))))))

(deftest backward-compat
  (testing "existing 3/4 arities are byte-for-byte unchanged (no meta, no :meta on hook)"
    (let [store  (mem)
          events (atom [])]
      (k/add-write-hook! store :t (fn [e] (swap! events conj e)))
      (k/assoc store :a 1 {:sync? true})         ; 4-arity opts
      (k/assoc-in store [:b] 2 {:sync? true})     ; 4-arity opts
      (k/update store :a inc {:sync? true})       ; 4-arity opts
      (is (= 2 (k/get store :a nil {:sync? true})))
      (is (= 2 (k/get store :b nil {:sync? true})))
      (is (every? #(nil? (:meta %)) @events) "no :meta on any hook event"))))
