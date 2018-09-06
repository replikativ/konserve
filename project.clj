(defproject io.replikativ/konserve "0.5.0-beta4"
  :description "Durable cross-platform key-value store protocol with core.async."
  :url "http://github.com/replikativ/konserve"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src"]
  :test-paths ["test"]
  :dependencies [[org.clojure/clojure "1.9.0" :scope "provided"]
                 [org.clojure/clojurescript "1.9.946" :scope "provided"]
                 [io.replikativ/incognito "0.2.5-SNAPSHOT"]
                 [fress "0.2.0"]
                 [org.clojure/core.async "0.4.474"]

                 [org.clojure/data.fressian "0.2.1"] ;; for filestore

                 [io.replikativ/hasch "0.3.5"]
                 [org.clojars.mmb90/cljs-cache "0.1.4"]]

  :plugins [[lein-cljsbuild "1.1.7"]]

  :profiles {:dev {:dependencies [[com.cemerick/piggieback "0.2.1"]]
                   :figwheel {:nrepl-port 7888
                              :nrepl-middleware ["cider.nrepl/cider-middleware"
                                                 "cemerick.piggieback/wrap-cljs-repl"]}
                   :plugins [[lein-figwheel "0.5.8"]]}
             :test {:dependencies [[clj-time "0.13.0"]]}}

  :clean-targets ^{:protect false} ["target" "out" "resources/public/js"]

  :hooks [leiningen.cljsbuild]

  :cljsbuild
  {:test-commands {"unit-tests" ["node" "target/unit-tests.js"]}
   :builds
   {:tests
    {:source-paths ["src" "test"]
     :notify-command ["node" "target/unit-tests.js"]
     :compiler {:output-to "target/unit-tests.js"
                :optimizations :none
                :target :nodejs
                :main konserve.konserve-test}}}})

