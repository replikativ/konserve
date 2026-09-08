(ns unix-directory-smoke
  "Native-image entry point exercising the packaged Unix directory barrier."
  (:require [konserve.directory-sync :as directory-sync])
  (:gen-class))

(defn -main [& _]
  (let [path (java.nio.file.Files/createTempDirectory
              "konserve-unix-barrier-"
              (make-array java.nio.file.attribute.FileAttribute 0))]
    (try
      (directory-sync/sync-directory! path)
      (println "PASS packaged Unix directory barrier")
      (finally (java.nio.file.Files/delete path)))))
