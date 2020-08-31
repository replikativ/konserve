(defproject io.replikativ/konserve "0.6.0-SNAPSHOT"
  :description "Durable cross-platform key-value store protocol with core.async."
  :url "http://github.com/replikativ/konserve"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src"]
  :test-paths ["test"]
  :dependencies [[org.clojure/clojure "1.10.1" :scope "provided"]
                 [io.replikativ/incognito "0.2.5"]
                 [io.replikativ/superv.async "0.2.9-SNAPSHOT"]
                 [fress "0.3.1"]
                 [org.clojure/data.fressian "0.2.1"] ;; for filestore
                 [io.replikativ/hasch "0.3.7"]
                 [org.clojars.mmb90/cljs-cache "0.1.4"]
                 [thheller/shadow-cljs "2.8.109"]
                 [org.clojure/clojurescript "1.10.520"]
                 [com.google.javascript/closure-compiler-unshaded "v20190325"]
                 [org.clojure/google-closure-library "0.0-20190213-2033d5d9"]
                 [org.lz4/lz4-java "1.6.0"]
                 [tick "0.4.26-alpha"]]
  :profiles
  {:cljs
   {:source-paths ["src/cljs"]
    :dependencies [[thheller/shadow-cljs "2.8.109"]]}}

  #_(:plugins [[lein-cljsbuild "1.1.7"]]

              :profiles {:dev {:dependencies [[com.cemerick/piggieback "0.2.1"]]
                               :figwheel {:nrepl-port 7888
                                          :nrepl-middleware ["cider.nrepl/cider-middleware"
                                                             "cemerick.piggieback/wrap-cljs-repl"]}
                               :plugins [[lein-figwheel "0.5.8"]]}}

              :clean-targets ^{:protect false} ["target" "out" "resources/public/js"])

; :hooks [leiningen.cljsbuild]

 ; :cljsbuild {}
  #_{;:test-commands {"unit-tests" ["node" "target/unit-tests.js"]}
     :builds
     {:tests
      {:source-paths ["src" "test"]
       :notify-command ["node" "target/unit-tests.js"]
       :compiler {:output-to "target/unit-tests.js"
                  :optimizations :none
                  :target :nodejs
                  :main konserve.konserve-test}}}})

