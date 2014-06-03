(defproject viebel/konserve "0.1.2"
  :description "Durable key-value store protocol with core.async."
  :url "http://github.com/ghubber/konserve"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj" "src/cljs"]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/clojurescript "0.0-2202"]
                 [org.clojure/core.async "0.1.278.0-76b25b-alpha"]
                 [com.ashafa/clutch "0.4.0-RC1"]]

  :plugins [[lein-cljsbuild "1.0.3"]
            [com.keminglabs/cljx "0.3.2"]
            [com.cemerick/austin "0.1.4"]]

  :cljx {:builds [{:source-paths ["src/cljx"]
                   :output-path "target/classes"
                   :rules :clj}

                  {:source-paths ["src/cljx"]
                   :output-path "target/classes"
                   :rules :cljs}]}

  :hooks [cljx.hooks]

  :cljsbuild
  {:builds
   [{:source-paths ["src/cljs"
                    "target/classes"]
     :compiler
     {:output-to "resources/public/js/main.js"
      :optimizations :simple
      :pretty-print true}}]})
