(require '[build :as build] '[clojure.string :as str])
;; Exercise the release builder, not an independently reconstructed test jar.
(build/clean nil)
(build/jar nil)
(let [artifact (.getCanonicalPath (java.io.File. build/jar-file))
      canonical #(.getCanonicalPath (java.io.File. %))
      source-roots (set (map canonical ["src" "resources" "target/classes"]))
      separator (System/getProperty "path.separator")
      roots (str/split (slurp "probe-results/konserve.cp")
                       (re-pattern (java.util.regex.Pattern/quote separator)))]
  (spit "probe-results/artifact.path" artifact)
  ;; Neither source nor loose classes/resources may mask a broken artifact.
  (spit "probe-results/artifact.cp"
        (str/join separator (cons artifact (remove #(source-roots (canonical %)) roots)))))
(shutdown-agents)
