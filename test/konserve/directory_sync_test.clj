(ns konserve.directory-sync-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is]]
            [konserve.core :as k]
            [konserve.filestore :as fs])
  (:import [java.nio.file Files AccessDeniedException]
           [java.io IOException]))

(defn- private-var [name] (ns-resolve 'konserve.filestore name))

(defn- outcome [f]
  (try (f) (catch Throwable e e)))

(defn- caused-by? [result expected]
  (and (instance? Throwable result)
       (or (identical? result expected)
           (when-let [cause (ex-cause result)]
             (caused-by? cause expected)))))

(deftest directory-open-policy
  (let [denied (AccessDeniedException. "injected")]
    (doseq [windows? [false true] allow-unsafe? [false true]]
      (with-redefs-fn {(private-var 'windows?) (constantly windows?)
                       (private-var 'open-directory-channel)
                       (fn [& _] (throw denied))}
        #(is (= (when (or (not allow-unsafe?) (not windows?)) denied)
                (outcome (fn [] ((private-var 'sync-base) nil "unused" allow-unsafe?)))))))
    (is (instance? Exception
                   (outcome #((private-var 'sync-base) :custom "unused" false))))
    (is (nil? ((private-var 'sync-base) :custom "unused" true)))))

(deftest directory-configuration-validation
  (doseq [config [{:allow-unsafe-directory-sync? :yes}
                  {:allow-unsafe-directory-sync? nil}
                  {:strict-directory-sync? false}]]
    (is (= :konserve/invalid-directory-sync-config
           (:type (ex-data (outcome #(fs/connect-fs-store "unused" :config config)))))))
  (is (= :konserve/invalid-directory-sync-config
         (:type (ex-data
                 (outcome #(fs/connect-fs-store "unused" :filesystem :custom
                                                :config {})))))))

(deftest write-completion-requires-directory-sync
  (doseq [sync? [true false]]
    (let [path (str (Files/createTempDirectory "konserve-directory-sync-"
                                               (make-array java.nio.file.attribute.FileAttribute 0)))
          config {:sync-blob? true :in-place? false}
          store (fs/connect-fs-store path :config config :opts {:sync? true})
          call! (fn [f] (outcome #(let [result (f)] (if sync? result (<!! result)))))
          opts {:sync? sync?}]
      (try
        (doseq [operation ['open-directory-channel 'force-directory-channel!]]
          (let [failure (if (= operation 'open-directory-channel)
                          (AccessDeniedException. "injected")
                          (IOException. "injected force failure"))]
            (with-redefs-fn {(private-var operation) (fn [& _] (throw failure))}
              #(is (caused-by? (call! (fn [] (k/assoc store :key operation opts))) failure)))))
        ;; Failed completion can still leave the new value visible: retry, do not
        ;; infer rollback. Success after removing the fault survives a reopen.
        (let [result (call! #(k/assoc store :key :complete opts))]
          (if (and ((private-var 'windows?)) (instance? Throwable result))
            ;; A real Windows filesystem may refuse directory opens. Strict
            ;; mode must report that, not pretend the successful-retry path ran.
            (is (some #(instance? AccessDeniedException %)
                      (take-while some? (iterate ex-cause result))))
            (do
              (is (not (instance? Throwable result)))
              (let [reopened (fs/connect-fs-store path :config config :opts {:sync? true})]
                (is (= :complete (k/get reopened :key nil {:sync? true})))))))
        (finally (fs/delete-store path))))))
