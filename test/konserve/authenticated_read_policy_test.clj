(ns konserve.authenticated-read-policy-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.encryptor :as enc]
            [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :as layout]
            [konserve.simulation.crash :as crash]
            [konserve.filestore :as fs])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn with-store [f]
  (let [path (str (Files/createTempDirectory "konserve-auth-policy-" (make-array FileAttribute 0)))]
    (try (f path) (finally (fs/delete-store path)))))

(defn open-store [path encryptor]
  (fs/connect-fs-store path :config {:encoding {:encryptor encryptor}}
                       :opts {:sync? true}))

(deftest strict-reader-rejects-valid-plaintext-and-legacy-records
  (doseq [legacy [nil {:type :aes :key (apply str (repeat 64 "1"))}]]
    (with-store
      (fn [path]
        (let [key (apply str (repeat 64 "1"))
              old (open-store path legacy)
              _ (k/assoc old :checkpoint {:revision 7} {:sync? true})
              strict (open-store path {:type :aes-gcm :key key :require-authenticated? true})]
          (is (thrown? clojure.lang.ExceptionInfo
                       (k/get strict :checkpoint nil {:sync? true})))
          (is (instance? Throwable (<!! (k/get strict :checkpoint nil {:sync? false}))))
          ;; Rejected reads must not rewrite or remove the existing record.
          (is (= {:revision 7} (k/get old :checkpoint nil {:sync? true}))))))))

(deftest strict-gcm-roundtrip-and-compatibility-opt-in
  (with-store
    (fn [path]
      (let [key (enc/generate-key)
            strict (open-store path {:type :aes-gcm :key key :require-authenticated? true})]
        (k/assoc strict :checkpoint {:revision 8} {:sync? true})
        (is (= {:revision 8} (k/get strict :checkpoint nil {:sync? true})))
        (k/assoc (open-store path nil) :legacy :plain {:sync? true})
        (is (= :plain (k/get (open-store path {:type :aes-gcm :key key})
                             :legacy nil {:sync? true})))))))

(deftest encryption-config-fails-closed
  (is (thrown? clojure.lang.ExceptionInfo (enc/get-encryptor :aes-gmc)))
  (with-store
    (fn [path]
      (is (thrown? clojure.lang.ExceptionInfo
                   (open-store path {:type :none :require-authenticated? true})))
      (is (thrown? clojure.lang.ExceptionInfo
                   (open-store path {:type :aes-gcm :key (enc/generate-key)
                                     :require-authenticated? "true"}))))))

(deftest authenticated-cbor-checkpoints-survive-crash-boundaries
  (doseq [point [:after-write-header :after-write-meta :after-write-value
                 :after-sync :after-atomic-move :after-sync-store]]
    (let [{:keys [backing state-atom crash-point-atom]} (crash/create-crash-aware-store)
          _ (layout/-create-store backing {:sync? true})
          config {:opts {:sync? true}
                  :config {:encoding {:serializer :BoringSerializer
                                      :encryptor {:type :aes-gcm :key (enc/generate-key)
                                                  :require-authenticated? true}}}}
          store (defaults/connect-default-store backing config)]
      (k/assoc store :checkpoint {:revision 0 :receipts {}} {:sync? true})
      (crash/set-crash-point! crash-point-atom point)
      (is (thrown? Exception (k/assoc store :checkpoint {:revision 1 :receipts {}}
                                      {:sync? true})))
      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)
      (let [reopened (defaults/connect-default-store backing config)
            recovered (k/get reopened :checkpoint nil {:sync? true})]
        (is (contains? #{{:revision 0 :receipts {}} {:revision 1 :receipts {}}}
                       recovered))
        (k/assoc reopened :checkpoint {:revision 2 :receipts {}} {:sync? true})
        (crash/simulate-crash! state-atom)
        (is (= {:revision 2 :receipts {}} (k/get reopened :checkpoint nil {:sync? true})))))))
