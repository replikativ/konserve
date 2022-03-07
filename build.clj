(ns build
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.build.api :as b]
            [borkdude.gh-release-artifact :as gh]
            [org.corfield.build :as bb]))

(def lib 'io.replikativ/konserve)
(def version (format "0.7.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean
  [_]
  (b/delete {:path "target"}))

(defn jar
  [opts]
  (-> opts
      (assoc :class-dir class-dir
             :src-pom "./template/pom.xml"
             :lib lib
             :version version
             :basis basis
             :jar-file jar-file
             :src-dirs ["src"])
      bb/jar))

(defn ci "Run the CI pipeline of tests (and build the JAR)." [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/clean)
      (bb/jar)))

(defn install "Install the JAR locally." [opts]
  (-> opts
      jar
      bb/install))

(defn deploy "Deploy the JAR to Clojars." [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/deploy)))

(defn release
  [_]
  (-> (gh/overwrite-asset {:org "replikativ"
                           :repo (name lib)
                           :tag version
                           :commit (gh/current-commit)
                           :file jar-file
                           :content-type "application/java-archive"})
      :url
      println))

