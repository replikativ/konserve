(ns konserve.directory-provisioning-test
  (:require [clojure.test :refer [deftest is]]
            [konserve.directory-sync :as ds])
  (:import [java.nio.file Files Path FileVisitOption]
           [java.io IOException]))

(defn- with-tree [f]
  (let [root (Files/createTempDirectory "konserve-provision-"
                                        (make-array java.nio.file.attribute.FileAttribute 0))]
    (try (f root)
         (finally
           (with-open [paths (Files/walk root (make-array FileVisitOption 0))]
             (doseq [p (reverse (sort-by #(.getNameCount ^Path %)
                                         (iterator-seq (.iterator paths))))]
               (Files/delete ^Path p)))))))

(defn- failure [f]
  (try (f) nil (catch Exception e e)))

(deftest retries-repeat-every-required-barrier
  (doseq [fail-at (range 1 6)]
    (with-tree
      (fn [^Path root]
        (let [a (.resolve root "a") b (.resolve a "b")
              expected [root root a a b]
              calls (atom []) error (IOException. "injected persistence failure")]
          (with-redefs [ds/sync-directory! (fn [p]
                                             (swap! calls conj p)
                                             (when (= fail-at (count @calls)) (throw error)))]
            (is (identical? error (failure #(ds/provision-directory! root b)))))
          (is (= (take fail-at expected) @calls))
          (reset! calls [])
          (with-redefs [ds/sync-directory! #(swap! calls conj %)]
            (is (= b (ds/provision-directory! root b)))
            (is (= expected @calls))
            (reset! calls [])
            (ds/provision-directory! root b)
            (is (= expected @calls) "Existing names are not evidence of completed barriers")))))))

(deftest invalid-targets-do-not-trigger-io
  (with-tree
    (fn [^Path root]
      (let [calls (atom [])]
        (with-redefs [ds/sync-directory! #(swap! calls conj %)]
          (doseq [path [nil "" :bad (.resolve root "../outside")]]
            (is (= :konserve/invalid-provisioning-path
                   (:type (ex-data (failure #(ds/provision-directory! root path))))))))
        (is (empty? @calls))))))

(deftest missing-ancestor-is-not-created
  (with-tree
    (fn [^Path root]
      (let [missing (.resolve root "missing")]
        (is (= :konserve/invalid-provisioning-directory
               (:type (ex-data (failure #(ds/provision-directory! missing (.resolve missing "child")))))))))))

(deftest actual-directory-barriers
  (with-tree
    (fn [^Path root]
      (is (= root (ds/provision-directory! root root)))
      (let [path (.resolve root "a/b")]
        (is (= path (ds/provision-directory! root path)))
        (is (= path (ds/provision-directory! root path)))))))

(deftest regular-files-are-not-directory-components
  (with-tree
    (fn [^Path root]
      (let [file (.resolve root "file")]
        (Files/createFile file (make-array java.nio.file.attribute.FileAttribute 0))
        (is (= :konserve/invalid-provisioning-directory
               (:type (ex-data (failure #(ds/provision-directory! root (.resolve file "child")))))))))))

(deftest symlink-components-are-rejected
  ;; Windows symlink creation requires privileges not guaranteed on CI.
  (when-not (ds/windows?)
    (with-tree
      (fn [^Path root]
        (let [target (.resolve root "target") link (.resolve root "link")]
          (Files/createDirectory target (make-array java.nio.file.attribute.FileAttribute 0))
          (Files/createSymbolicLink link target (make-array java.nio.file.attribute.FileAttribute 0))
          (is (= :konserve/invalid-provisioning-directory
                 (:type (ex-data (failure #(ds/provision-directory! root (.resolve link "child"))))))))))))
