(defproject io.replikativ/konserve "0.3.0"
  :description "Durable cross-platform key-value store protocol with core.async."
  :url "http://github.com/replikativ/konserve"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src"]
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/clojurescript "1.7.107"]
                 [es.topiq/full.async "0.2.8-beta1"]
                 #_[org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [io.replikativ/incognito "0.1.2"]]

  :plugins [[lein-cljsbuild "1.1.0"]]

  :profiles {:dev {:plugins [[com.cemerick/austin "0.1.6"]]}}

  :cljsbuild
  {:builds
   [{:source-paths ["src"]
     :compiler
     {:output-to "resources/public/js/main.js"
      :optimizations :simple
      :pretty-print true}}]})
