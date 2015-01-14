(defproject net.polyc0l0r/konserve "0.2.3"
  :description "Durable cross-platform key-value store protocol with core.async."
  :url "http://github.com/ghubber/konserve"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj" "src/cljs"]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/clojurescript "0.0-2511"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/data.fressian "0.2.0"]
                 #_[com.taoensso/nippy "2.7.0"]
                 [weasel "0.4.2"]]

  :plugins [[lein-cljsbuild "1.0.3"]
            [com.keminglabs/cljx "0.5.0"]]

  :cljx {:builds [{:source-paths ["src/cljx"]
                   :output-path "target/classes"
                   :rules :clj}

                  {:source-paths ["src/cljx"]
                   :output-path "target/classes"
                   :rules :cljs}]}

  :prep-tasks [["cljx" "once"] "javac" "compile"]

  :cljsbuild
  {:builds
   [{:source-paths ["src/cljs"
                    "target/classes"]
     :compiler
     {:output-to "resources/public/js/main.js"
      :optimizations :simple
      :pretty-print true}}]})
