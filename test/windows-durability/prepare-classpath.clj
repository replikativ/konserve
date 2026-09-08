(require '[clojure.tools.deps :as deps] '[clojure.edn :as edn])
(let [basis (deps/create-basis {:project "deps.edn" :aliases [:test]})
      project (edn/read-string (slurp "deps.edn"))
      build-basis (deps/create-basis
                   {:project "deps.edn"
                    :extra {:deps (get-in project [:aliases :build :deps])}})]
  (spit "probe-results/konserve.cp" (deps/join-classpath (:classpath-roots basis)))
  (spit "probe-results/build.cp"
        (deps/join-classpath (cons "." (:classpath-roots build-basis)))))
