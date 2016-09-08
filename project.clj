(defproject io.replikativ/konserve "0.4.3"
  :description "Durable cross-platform key-value store protocol with core.async."
  :url "http://github.com/replikativ/konserve"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/clojurescript "1.8.34"]
                 [org.clojure/core.async "0.2.385"]
                 [io.replikativ/hasch "0.3.1"]

                 [io.replikativ/incognito "0.2.0"]]

  :plugins [[lein-cljsbuild "1.1.2"]]

  :profiles {:dev {:dependencies [[com.cemerick/piggieback "0.2.1"]]
                   :figwheel {:nrepl-port 7888
                              :nrepl-middleware ["cider.nrepl/cider-middleware"
                                                 "cemerick.piggieback/wrap-cljs-repl"]}
                   :plugins [[lein-figwheel "0.5.0-2"]]}}

  :clean-targets ^{:protect false} ["target" "out" "resources/public/js"]



  :cljsbuild
  {:builds
   [{:id "cljs_repl"
     :source-paths ["src"]
     :figwheel true
     :compiler
     {:main konserve.indexeddb
      :asset-path "js/out"
      :output-to "resources/public/js/main.js"
      :output-dir "resources/public/js/out"
      :optimizations :none
      :pretty-print true}}]})
