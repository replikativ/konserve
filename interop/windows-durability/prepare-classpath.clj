(require '[clojure.tools.deps :as deps])
(let [basis (deps/create-basis {:project "deps.edn" :aliases [:test]})]
  (spit "probe-results/konserve.cp" (deps/join-classpath (:classpath-roots basis))))
