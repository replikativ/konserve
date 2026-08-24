(ns konserve.connect-readonly-test
  "Connecting to an EXISTING store must not write.

   `connect-default-store` used to call `-create-store` unconditionally on
   every connect — 'auto-create on connect' implemented as 'create on
   connect'. On an object-store backing that is a PUT of the store marker, so
   every connect performed a write it had no reason to perform: a reader
   holding read-only credentials could not connect at all, and a cold open
   against S3 cost 2 PUTs (datahike probes and then connects) where the
   correct number is zero. Measured in replikativ/datahike-serverless#6.

   The oracle here is a REFUSING backing: `-create-store` on it throws once
   the store exists, exactly as a read-only credential would. A counting
   assertion alone could go stale; a backing that fails the way production
   fails cannot."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.filestore :refer [delete-store]]
            [konserve.impl.defaults :refer [connect-default-store]]
            [konserve.impl.storage-layout :as sl])
  (:import [java.util UUID]))

(defn- counting-refusing-backing
  "Wrap `inner` so that `-create-store` counts its calls and THROWS when the
   store already exists — the behaviour of a backing reached through
   read-only credentials. Everything else delegates."
  [inner counter]
  (reify
    sl/PBackingStore
    (-create-blob [_ store-key env] (sl/-create-blob inner store-key env))
    (-delete-blob [_ store-key env] (sl/-delete-blob inner store-key env))
    (-blob-exists? [_ store-key env] (sl/-blob-exists? inner store-key env))
    (-copy [_ from to env] (sl/-copy inner from to env))
    (-atomic-move [_ from to env] (sl/-atomic-move inner from to env))
    (-migratable [_ key store-key env] (sl/-migratable inner key store-key env))
    (-migrate [_ mk kv ser rh wh env] (sl/-migrate inner mk kv ser rh wh env))
    (-create-store [this env]
      (swap! counter inc)
      (if (sl/-store-exists? inner env)
        (throw (ex-info "write refused: store exists and credentials are read-only"
                        {:type ::readonly-violation}))
        (sl/-create-store inner env)))
    (-sync-store [_ env] (sl/-sync-store inner env))
    (-store-exists? [_ env] (sl/-store-exists? inner env))
    (-delete-store [_ env] (sl/-delete-store inner env))
    (-keys [_ env] (sl/-keys inner env))))

(defn- fs-backing
  "A filestore backing constructed DIRECTLY — `connect-fs-store` would run
   `connect-default-store` itself and thereby create the store before the
   wrapped backing under test ever saw a call, making the first-connect
   assertion vacuously zero."
  [path]
  (konserve.filestore/map->BackingFilestore
   ;; nil filesystem = the default one (see filestore's get-path)
   {:base path :detected-old-blobs nil :ephemeral? false :filesystem nil}))

(deftest connect-to-an-existing-store-does-not-write
  (let [path (str "/tmp/konserve-readonly-test-" (UUID/randomUUID))]
    (try
      (testing "first connect creates — auto-create is untouched"
        (let [counter (atom 0)
              backing (counting-refusing-backing (fs-backing path) counter)
              store   (connect-default-store backing {:opts {:sync? true}})]
          (is (= 1 @counter) "a missing store is created, exactly once")
          (k/assoc store :seed {:v 1} {:sync? true})))
      (testing "a second connect performs NO create — and therefore works on
                credentials that cannot write"
        (let [counter (atom 0)
              backing (counting-refusing-backing (fs-backing path) counter)
              ;; With the old behaviour this THROWS ::readonly-violation before
              ;; returning a store — which is precisely what an S3 reader on a
              ;; read-only IAM policy saw.
              store   (connect-default-store backing {:opts {:sync? true}})]
          (is (zero? @counter) "an existing store is probed, never created")
          (is (= {:v 1} (k/get store :seed nil {:sync? true}))
              "and the connect is fully functional for reads")))
      (finally (delete-store path)))))
