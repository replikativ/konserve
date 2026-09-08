(import '[java.lang ProcessHandle]
        '[java.util.concurrent TimeUnit])

(defn descendants []
  (with-open [s (.descendants (ProcessHandle/current))]
    (vec (iterator-seq (.iterator s)))))

(defn facts [process]
  (let [h (.toHandle process)]
    {:pid (.pid process) :process-alive (.isAlive process)
     :handle-alive (.isAlive h)
     :start (str (.startInstant (.info h)))
     :descendants (mapv #(.pid %) (descendants))}))

(let [[bb config] *command-line-args*]
  (prn {:runtime (System/getProperty "java.runtime.version")
        :bb (System/getProperty "babashka.version")})
  (doseq [ready? [false true] attempt (range 10)]
    (let [process (.start (ProcessBuilder.
                          [bb "--config" config "-e"
                           "(println :ready) (flush) (Thread/sleep 60000)"]))]
      (try
        (when ready?
          (let [line (future (.readLine (java.io.BufferedReader.
                                       (java.io.InputStreamReader. (.getInputStream process)))))]
            (assert (= ":ready" (deref line 10000 :timeout)))))
        (let [before (facts process)
              children (descendants)
              destroyed (mapv (fn [h] [(.pid h) (.destroy h)]) (reverse children))]
          ;; Exactly the production cleanup's handle-based wait strategy.
          (doseq [h children]
            (when (.isAlive h)
              (try (.get (.onExit h) 5 TimeUnit/SECONDS)
                   (catch java.util.concurrent.TimeoutException _ (.destroyForcibly h)))))
          (doseq [h children]
            (when (.isAlive h)
              (try (.get (.onExit h) 5 TimeUnit/SECONDS)
                   (catch java.util.concurrent.TimeoutException _ nil))))
          (let [after (facts process)
                waited (.waitFor process 1 TimeUnit/SECONDS)]
            (prn {:ready ready? :attempt attempt :before before
                  :destroyed destroyed :after after
                  :direct-wait-completed waited :after-wait (facts process)})))
        (finally
          (.destroyForcibly process)
          (.waitFor process 5 TimeUnit/SECONDS)))))
(shutdown-agents)
