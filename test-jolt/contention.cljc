(ns contention (:require [konserve.core :as k]))

(def opts {:sync? true})
(def revision-opts (assoc opts :with-revision? true))

(defn- wait-for [path]
  (loop [left 3000]
    (when-not (.exists (java.io.File. path))
      (when (zero? left) (throw (ex-info "Peer timed out" {:marker path})))
      (Thread/sleep 50)
      (recur (dec left)))))

(defn -main [dir markers]
  (let [config {:backend :file :path dir :id #uuid "c3a8119c-e3b2-4773-a462-0d6bb93e2234"
                :config {:encoding {:serializer :BoringSerializer}}}
        store (k/connect-store config opts)
        side #?(:jolt "jolt" :clj "jvm")
        peer #?(:jolt "jvm" :clj "jolt")
        marker #(str markers "/" %)]
    #?(:jolt (do (k/assoc store :counter {} (assoc opts :expected-revision k/absent))
                 (spit (marker "initialized") "ready")))
    (wait-for (marker "initialized"))
    (let [[old token] (k/get store :counter nil revision-opts)]
      (assert (= {} old))
      (spit (marker side) "ready")
      (wait-for (marker "go"))
      (let [result (try (k/assoc store :counter {:first side} (assoc opts :expected-revision token))
                        "won"
                        (catch Exception e
                          (if (= :konserve/revision-mismatch (:type (ex-data e))) "stale" (throw e))))]
        (spit (marker (str side "-cas")) result)
        (wait-for (marker (str peer "-cas")))
        (assert (= #{"won" "stale"} #{result (slurp (marker (str peer "-cas")))}))))
    (dotimes [_ 30]
      (k/update-in store [:counter (keyword side)] (fnil inc 0) opts))
    (spit (marker (str side "-done")) "done")
    (wait-for (marker (str peer "-done")))
    (let [v (k/get (k/connect-store config opts) :counter nil opts)]
      (assert (= {:jolt 30 :jvm 30} (dissoc v :first)) (pr-str v))
      (assert (= "won" (slurp (marker (str (:first v) "-cas")))))
      (println side v))))
