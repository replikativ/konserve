;; Real process termination, NOT an OS/power-loss simulation. Parent records the
;; acknowledged boundary outside the child and checks the recovered root closure.
(require '[konserve.core :as k] '[konserve.filestore :as fs])
(import '[java.nio.file Files] '[java.util.concurrent TimeUnit]
        '[java.io BufferedReader InputStreamReader])
(let [[mode path phase] *command-line-args*]
  (if (= mode "child")
    (let [store (fs/connect-fs-store path :opts {:sync? true})]
      (k/assoc store :new-value {:generation 2} {:sync? true})
      (when (= phase "root") (k/assoc store :root :new-value {:sync? true}))
      (println "READY")
      (flush)
      (read-line))
    (doseq [boundary ["value" "root"]]
      (let [base (Files/createTempDirectory "konserve-process-crash-"
                                            (make-array java.nio.file.attribute.FileAttribute 0))
            path (str base)
            store (fs/connect-fs-store path :opts {:sync? true})
            _ (k/assoc store :old-value {:generation 1} {:sync? true})
            _ (k/assoc store :root :old-value {:sync? true})
            executable (str (System/getProperty "java.home") "/bin/java"
                            (when (.startsWith (System/getProperty "os.name") "Windows") ".exe"))
            process (-> (ProcessBuilder.
                         ^java.util.List [executable "--enable-native-access=ALL-UNNAMED"
                                          "-cp" (System/getProperty "java.class.path")
                                          "clojure.main" "interop/windows-durability/process-crash.clj"
                                          "child" path boundary])
                        (.redirectError java.lang.ProcessBuilder$Redirect/INHERIT)
                        (.start))
            ready (future
                    (with-open [reader (BufferedReader. (InputStreamReader. (.getInputStream process)))]
                      (loop []
                        (let [line (.readLine reader)]
                          (cond (= "READY" line) true
                                (nil? line) false
                                :else (recur))))))]
        (try
          (assert (= true (deref ready 90000 :timeout)) "Child failed to acknowledge the boundary")
          (.destroyForcibly process)
          (assert (.waitFor process 15 TimeUnit/SECONDS) "Child did not terminate")
          (let [reopened (fs/connect-fs-store path :opts {:sync? true})
                root (k/get reopened :root nil {:sync? true})
                expected (if (= boundary "root") :new-value :old-value)]
            (assert (= expected root) "Wrong recovered root")
            (assert (= {:generation (if (= expected :new-value) 2 1)}
                       (k/get reopened root nil {:sync? true})) "Root references missing or incorrect data")
            (assert (= {:generation 2} (k/get reopened :new-value nil {:sync? true}))
                    "Acknowledged payload missing")
            (println "PASS process-kill boundary" boundary))
          (finally
            (.destroyForcibly process)
            (.waitFor process 15 TimeUnit/SECONDS)
            (future-cancel ready)
            (fs/delete-store path)))))))
(shutdown-agents)
