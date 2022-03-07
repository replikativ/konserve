(ns konserve.indexeddb
  (:require [incognito.edn :refer [read-string-safe]]
            [konserve.serializers :as ser]
            [konserve.protocols :refer [PEDNKeyValueStore -exists? -get -update-in -assoc-in -get-meta
                                        PBinaryKeyValueStore -bget -bassoc
                                        PStoreSerializer -serialize -deserialize]]
            [cljs.core.async :refer (take! <! >! put! close! chan poll!)])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))

(defrecord IndexedDBKeyValueStore [db store-name serializer read-handlers write-handlers locks version]
  PEDNKeyValueStore
  (-exists? [_this key]
    (let [res       (chan)
          tx        (.transaction db #js [store-name])
          obj-store (.objectStore tx store-name)
          req       (.openCursor obj-store (pr-str key))]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot check for existence."
                                 {:type  :access-error
                                  :key   key
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn [e]
              (put! res (if (.-result (.-target e))
                          true false))
              (close! res)))
      res))

  (-get-meta [_this key]
    (let [res       (chan)
          tx        (.transaction db #js [store-name])
          obj-store (.objectStore tx store-name)
          req       (.get obj-store (pr-str key))]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot read edn value."
                                 {:type  :read-error
                                  :key   key
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn [e] (when-let [r (.-result req)]
                      (put! res (-deserialize serializer read-handlers (aget r "meta"))))
              (close! res)))
      res))

  (-get [this key]
    (let [res       (chan)
          tx        (.transaction db #js [store-name])
          obj-store (.objectStore tx store-name)
          req       (.get obj-store (pr-str key))]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot read edn value."
                                 {:type  :read-error
                                  :key   key
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn [e] (when-let [r (.-result req)]
                      (put! res (-deserialize serializer read-handlers (aget r "edn_value"))))
              (close! res)))
      res))

  (-assoc-in [this key-vec meta-up val] (-update-in this key-vec meta-up (fn [_] val) []))
  (-update-in [_this key-vec meta-up up-fn args]
    (let [[fkey & rkey] key-vec
          res           (chan)
          tx            (.transaction db #js [store-name] "readwrite")
          obj-store     (.objectStore tx store-name)
          req           (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot read edn value."
                                 {:type  :read-error
                                  :key   key-vec
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn read-old [e]
              (try
                (let [[old-meta old] (when-let [r (.-result req)]
                                       [(-deserialize serializer read-handlers (aget r "meta")) (-deserialize serializer read-handlers (aget r "edn_value"))])
                      edn-meta       (meta-up old-meta)
                      edn-value      (if-not (empty? rkey)
                                       (apply update-in old rkey up-fn args)
                                       (apply up-fn old args))]
                  (let [up-req (.put obj-store
                                     (clj->js {:key (pr-str fkey)
                                               :version version
                                               :meta
                                               (-serialize serializer nil write-handlers edn-meta)
                                               :edn_value
                                               (-serialize serializer nil write-handlers edn-value)}))]
                    (set! (.-onerror up-req)
                          (fn [e]
                            (put! res (ex-info "Cannot write edn value."
                                               {:type  :write-error
                                                :key   key-vec
                                                :error (.-target e)}))
                            (close! res)))
                    (set! (.-onsuccess up-req)
                          (fn [e]
                            (put! res [(get-in old rkey) edn-value])
                            (close! res)))))
                (catch :default e
                  (put! res (ex-info "Cannot parse edn value."
                                     {:type  :read-error
                                      :key   key-vec
                                      :error e}))
                  (close! res)))))
      res))

  (-dissoc [_this key]
    (let [res       (chan)
          tx        (.transaction db #js [store-name] "readwrite")
          obj-store (.objectStore tx store-name)
          up-req    (.delete obj-store (pr-str key))]
      (set! (.-onerror up-req)
            (fn [e]
              (put! res (ex-info "Cannot write edn value."
                                 {:type  :write-error
                                  :key   key
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess up-req)
            (fn [e]
              (close! res)))
      res))

  PBinaryKeyValueStore
  (-bget [_this key lock-cb]
    (let [res       (chan)
          tx        (.transaction db #js [store-name])
          obj-store (.objectStore tx store-name)
          req       (.get obj-store (pr-str key))]
      (set! (.-onerror req)
            (fn [e]
              (go
                (>! res (<!
                         (lock-cb (ex-info "Cannot read binary value."
                                           {:type  :read-error
                                            :key   key
                                            :error (.-target e)}))))
                (close! res))))
      (set! (.-onsuccess req)
            (fn [e] (when-let [r (.-result req)]
                      (put! res (lock-cb (aget r "value"))))
                ;; returns nil
              (close! res)))
      res))
  (-bassoc [this key meta-up blob]
    (let [res       (chan)
          tx        (.transaction db #js [store-name] "readwrite")
          obj-store (.objectStore tx store-name)
          req       (.get obj-store (pr-str key))]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot read edn value."
                                 {:type  :read-error
                                  :key   key
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn read-old [e]
              (try
                (let [old-meta (when-let [r (.-result req)]
                                 (-deserialize serializer read-handlers (aget r "meta")))
                      edn-meta (meta-up old-meta)]
                  (let [up-req (.put obj-store
                                     (clj->js {:key   (pr-str key)
                                               :meta
                                               (-serialize serializer nil write-handlers edn-meta)
                                               :version version
                                               :value blob}))]
                    (set! (.-onerror up-req)
                          (fn [e]
                            (put! res (ex-info "Cannot write binary value."
                                               {:type  :write-error
                                                :key   key
                                                :error (.-target e)}))
                            (close! res)))
                    (set! (.-onsuccess up-req)
                          (fn [e] (close! res)))))
                (catch :default e
                  (put! res (ex-info "Cannot parse edn value."
                                     {:type  :read-error
                                      :key   key
                                      :error e}))
                  (close! res)))))
      res)))

(defn new-indexeddb-store
  "Create an IndexedDB backed edn store with read-handlers according to
  incognito.

  Be careful not to mix up edn and JSON values."
  [name & {:keys [read-handlers write-handlers serializer version]
           :or {read-handlers (atom {})
                write-handlers (atom {})
                serializer (ser/string-serializer)
                version 1}}]
  (let [res (chan)
        req (.open js/window.indexedDB name 1)]
    (set! (.-onerror req)
          (fn [e]
            (put! res (ex-info "Cannot open IndexedDB store."
                               {:type :db-error
                                :error (.-target e)}))
            (close! res)))
    (set! (.-onsuccess req)
          (fn success-handler [e]
            (put! res (map->IndexedDBKeyValueStore {:db (.-result req)
                                                    :serializer serializer
                                                    :store-name name
                                                    :read-handlers read-handlers
                                                    :write-handlers write-handlers
                                                    :locks (atom {})
                                                    :version version}))))
    (set! (.-onupgradeneeded req)
          (fn upgrade-handler [e]
            (let [db (-> e .-target .-result)]
              (.createObjectStore db name #js {:keyPath "key"}))))
    res))

(comment
    ;;new-gc
    ;; jack in figwheel cljs REPL
  (require 'figwheel-sidecar.repl-api)
  (figwheel-sidecar.repl-api/cljs-repl)

  (defrecord Test [a])
  (Test. 5)

  (go (def my-store (<! (new-indexeddb-store "konserve"
                                             :read-handlers
                                             (atom {'konserve.indexeddb.Test
                                                    map->Test})))))

    ;; or
  (-jassoc-in my-store ["test" "bar"] #js {:a 3})
  (go (println (<! (-jget-in my-store ["test"]))))
  (go (println (<! (-exists? my-store 1))))

  (go (doseq [i (range 10)]
        (println (<! (-get-in my-store [i])))))

  (go (time
       (doseq [i (range 10)]
         (<! (-update-in my-store [i] (fn [_] (inc i)))))
       #_(doseq [i (range 10)]
           (println (<! (-get-in my-store [i]))))))
  (go (println (<! (-get my-store 999))))

  (go (prn (<! (-update-in my-store ["foo"] (fn [_] {:meta "META"}) (fn [_] 0) []))))

  (go (prn (<! (-update-in my-store ["foo"] (fn [_] {:meta "META"}) inc []))))

  (go (println (<! (-get-meta my-store "foo"))))

  (go (println (<! (-get my-store "foo"))))

  (go (println (<! (-assoc-in my-store ["rec-test"] (Test. 5)))))
  (go (println (<! (-get my-store "rec-test"))))

  (go (println (<! (-assoc-in my-store ["test2"] {:a 1 :b 4.2}))))

  (go (println (<! (-assoc-in my-store ["test"] {:a 43}))))

  (go (println (<! (-update-in my-store ["test" :a] inc))))
  (go (println (<! (-get my-store "test2"))))

  (go (println (<! (-bassoc my-store
                            "blob-fun"
                            (fn [_] "my meta")
                            (new js/Blob #js ["hello worlds"], #js {"type" "text/plain"})))))

  (go (println (<! (-get-meta my-store "blob-fun"))))

  (go (.log js/console (<! (-bget my-store "blob-fun" identity)))))
