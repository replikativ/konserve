(defproject io.replikativ/konserve "0.3.2"
  :description "Durable cross-platform key-value store protocol with core.async."
  :url "http://github.com/replikativ/konserve"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src"]
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/clojurescript "1.7.107"]
                 [es.topiq/full.async "0.2.8-beta1"]
                 [io.replikativ/incognito "0.1.2"]]

  :plugins [[lein-cljsbuild "1.1.1"]]

  :profiles {:dev {:plugins [[com.cemerick/austin "0.1.6"]]}}

  :cljsbuild
  {:builds
   [{:source-paths ["src"]
     :compiler
     {:output-to "resources/public/js/main.js"
      :optimizations :simple
      :pretty-print true}}]})
