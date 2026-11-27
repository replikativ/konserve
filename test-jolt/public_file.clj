(ns public-file (:require [clojure.core.async :as a] [konserve.core :as k]))

(defrecord Datum [a v])

(defn- result [ch]
  (let [v (a/<!! ch)]
    (if (instance? Throwable v) (throw v) v)))

(defn- check-async [config]
  (let [s (result (k/create-store config))]
    (try
      (assert (result (k/store-exists? config)))
      (assert (instance? Throwable (a/<!! (k/create-store config))))
      (result (k/assoc s :item false {:expected-revision k/absent}))
      (assert (false? (result (k/get (result (k/connect-store config)) :item))))
      (assert (= :konserve/revision-mismatch
                 (:type (ex-data (a/<!! (k/assoc s :item true {:expected-revision k/absent}))))))
      (result (k/bassoc s :binary (byte-array [42])))
      (assert (= 42 (result (k/bget s :binary
                                   (fn [{:keys [input-stream]}]
                                     (a/go (a/<! (a/timeout 5)) (.read input-stream)))))))
      (assert (= #{:item :binary} (set (map :key (result (k/keys s))))))
      (result (k/dissoc s :item))
      (assert (false? (result (k/exists? s :item))))
      (finally (result (k/delete-store config))))
    (assert (false? (result (k/store-exists? config))))))

(defn -main [dir]
  (let [c {:backend :file :id (random-uuid) :path dir
           :config {:encoding {:serializer :BoringSerializer
                               :read-handlers (atom {'public-file/Datum map->Datum})}}}
        o {:sync? true}
        rejected? (fn [op] (try (op) false (catch Exception _ true)))
        _ (assert (rejected? #(k/connect-store c o)))
        s (k/create-store c o)]
    (assert (rejected? #(k/create-store c o)))
    (assert (rejected? #(k/delete-store (assoc c :filesystem :unsupported) o)))
    (assert (k/store-exists? c o))
    (assert (rejected? #(k/bassoc s :invalid-binary 7 o)))
    (assert (not (k/exists? s :invalid-binary o)))
    (k/assoc s :counter 1 (assoc o :expected-revision k/absent))
    (let [[v rev] (k/get s :counter nil (assoc o :with-revision? true))]
      (assert (= 1 v))
      (k/assoc s :counter 2 (assoc o :expected-revision rev))
      (assert (= :konserve/revision-mismatch
                 (try (k/assoc s :counter 3 (assoc o :expected-revision rev)) nil
                      (catch Exception e (:type (ex-data e)))))))
    (assert (= 2 (k/get (k/connect-store c o) :counter nil o)))
    (let [datum (->Datum :name "Ada")]
      (k/assoc s :row datum o)
      (let [restored (k/get (k/connect-store c o) :row nil o)]
        (assert (instance? Datum restored))
        (assert (= datum restored))))
    (assert (= #{:row :counter} (set (map :key (k/keys s o)))))
    (assert (k/dissoc s :counter o))
    (assert (not (k/exists? s :counter o)))
    (k/delete-store c o)
    (assert (not (k/store-exists? c o)))
    (check-async c)
    (println "Jolt file lifecycle and revision checks passed.")))
