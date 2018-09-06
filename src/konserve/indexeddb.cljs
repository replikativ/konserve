(ns konserve.indexeddb
  (:require [incognito.edn :refer [read-string-safe]]
            [konserve.core :as k]
            [konserve.serializers :as ser]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore -exists? -get-in -update-in
                                        PJSONAsyncKeyValueStore -jget-in -jassoc-in -jupdate-in
                                        PBinaryAsyncKeyValueStore -bget -bassoc
                                        -serialize -deserialize]]
            [cljs.core.async :as async :refer (take! <! >! put! close! chan poll!)])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))


(defrecord IndexedDBKeyValueStore [db store-name serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    (let [res (chan)
          tx (.transaction db #js [store-name])
          obj-store (.objectStore tx store-name)
          req (.openCursor obj-store (pr-str key))]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot check for existence."
                                 {:type :access-error
                                  :key key
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn [e]
              (put! res (if (.-result (.-target e))
                          true false))
              (close! res)))
      res))

  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          res (chan)
          tx (.transaction db #js [store-name])
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot read edn value."
                                 {:type :read-error
                                  :key key-vec
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn [e] (when-let [r (.-result req)]
                     (put! res (get-in (-deserialize serializer read-handlers (aget r "edn_value"))
                                       rkey)))
              ;; returns nil
              (close! res)))
      res))

  (-update-in [this key-vec up-fn]
    (let [[fkey & rkey] key-vec
          res (chan)
          tx (.transaction db #js [store-name] "readwrite")
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot read edn value."
                                 {:type :read-error
                                  :key key-vec
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn read-old [e]
              (try
                (let [old (when-let [r (.-result req)]
                            (-deserialize serializer read-handlers (aget r "edn_value")))
                      up (if-not (empty? rkey)
                           (update-in old rkey up-fn)
                           (up-fn old))]
                  (let [up-req (.put obj-store
                                     (clj->js {:key (pr-str fkey)
                                               :edn_value
                                               (-serialize serializer nil write-handlers up)}))]
                    (set! (.-onerror up-req)
                          (fn [e]
                            (put! res (ex-info "Cannot write edn value."
                                               {:type :write-error
                                                :key key-vec
                                                :error (.-target e)}))
                            (close! res)))
                    (set! (.-onsuccess up-req)
                          (fn [e]
                            (put! res [(get-in old rkey) up])
                            (close! res)))))
                (catch :default e
                  (put! res (ex-info "Cannot parse edn value."
                                     {:type :read-error
                                      :key key-vec
                                      :error e}))
                  (close! res)))))
      res))

  (-assoc-in [this key-vec val] (-update-in this key-vec (fn [_] val)))

  (-dissoc [this key]
    (let [res (chan)
          tx (.transaction db #js [store-name] "readwrite")
          obj-store (.objectStore tx store-name)
          up-req (.delete obj-store (pr-str key))]
      (set! (.-onerror up-req)
            (fn [e]
              (put! res (ex-info "Cannot write edn value."
                                 {:type :write-error
                                  :key key
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess up-req)
            (fn [e]
              (close! res)))
      res))

  PBinaryAsyncKeyValueStore
  (-bget [this key lock-cb]
    (let [res (chan)
          tx (.transaction db #js [store-name])
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str key))]
      (set! (.-onerror req)
            (fn [e]
              (go
                (>! res (<!
                         (lock-cb (ex-info "Cannot read binary value."
                                           {:type :read-error
                                            :key key
                                            :error (.-target e)}))))
                (close! res))))
      (set! (.-onsuccess req)
            (fn [e] (when-let [r (.-result req)]
                     (put! res (lock-cb (aget r "value"))))
              ;; returns nil
              (close! res)))
      res))
  (-bassoc [this key blob]
    (let [res (chan)
          tx (.transaction db #js [store-name] "readwrite")
          obj-store (.objectStore tx store-name)
          req (.put obj-store #js {:key (pr-str key)
                                   :value blob})]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot write binary value."
                                 {:type :write-error
                                  :key key
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn [e] (close! res)))
      res))

  PJSONAsyncKeyValueStore
  (-jget-in [this key-vec]
    (let [[fkey & rkey] key-vec
          res (chan)
          tx (.transaction db #js [store-name])
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot write json value."
                                 {:type :write-error
                                  :key key
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn [e] (when-let [r (.-result req)]
                     (put! res (-> r
                                   (aget "json_value")
                                   js->clj
                                   (get-in rkey))))
              ;; returns nil
              (close! res)))
      res))
  (-jassoc-in [this key-vec value]
    (let [[fkey & rkey] key-vec
          res (chan)
          tx (.transaction db #js [store-name] "readwrite")
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot write json value."
                                 {:type :write-error
                                  :key key
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn read-old [e]
              (let [old (when-let [r (.-result req)]
                          (js->clj (-> r (aget "json_value"))))
                    up-req (if (or value (not (empty? rkey)))
                             (.put obj-store
                                   #js {:key (pr-str fkey)
                                        :json_value (if-not (empty? rkey)
                                                      (clj->js (assoc-in old rkey value))
                                                      value)})
                             (.delete obj-store (pr-str fkey)))]
                (set! (.-onerror up-req)
                      (fn [e]
                        (put! res (ex-info "Cannot write json value."
                                           {:type :write-error
                                            :key key
                                            :error (.-target e)}))
                        (close! res)))
                (set! (.-onsuccess up-req)
                      (fn [e] (close! res))))))
      res))
  (-jupdate-in [this key-vec up-fn]
    (let [[fkey & rkey] key-vec
          res (chan)
          tx (.transaction db #js [store-name] "readwrite")
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (fn [e]
              (put! res (ex-info "Cannot write json value."
                                 {:type :write-error
                                  :key key
                                  :error (.-target e)}))
              (close! res)))
      (set! (.-onsuccess req)
            (fn read-old [e]
              (let [old (when-let [r (.-result req)]
                          (-> r (aget "json_value")))
                    new (if-not (empty? rkey)
                          (update-in old rkey up-fn)
                          (up-fn old))
                    up-req (if new
                             (.put obj-store
                                   (clj->js {:key (pr-str fkey)
                                             :json_value
                                             (pr-str new)}))
                             (.delete obj-store (pr-str fkey)))]
                (set! (.-onerror up-req)
                      (fn [e]
                        (put! res (ex-info "Cannot write json value."
                                           {:type :write-error
                                            :key key
                                            :error (.-target e)}))
                        (close! res)))
                (set! (.-onsuccess up-req)
                      (fn [e]
                        (put! res [(get-in old rkey) (get-in new rkey)])
                        (close! res))))))
      res)))


(defn new-indexeddb-store
  "Create an IndexedDB backed edn store with read-handlers according to
  incognito.

  Be careful not to mix up edn and JSON values."
  [name & {:keys [read-handlers write-handlers serializer]
           :or {read-handlers (atom {})
                write-handlers (atom {})
                serializer (ser/string-serializer)}}]
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
                                                    :locks (atom {})}))))
    (set! (.-onupgradeneeded req)
          (fn upgrade-handler [e]
            (let [db (-> e .-target .-result)]
              (.createObjectStore db name #js {:keyPath "key"}))))
    res))



(comment
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
  (go (println (<! (-get-in my-store [999]))))

  (go (println (<! (-assoc-in my-store ["rec-test"] (Test. 5)))))
  (go (println (<! (-get-in my-store ["rec-test"]))))

  (go (println (<! (-assoc-in my-store ["test2"] {:a 1 :b 4.2}))))

  (go (println (<! (-assoc-in my-store ["test"] {:a 43}))))

  (go (println (<! (-update-in my-store ["test" :a] inc))))
  (go (println (<! (-get-in my-store ["test2"]))))


  (go (println (<! (-bassoc my-store
                            "blob-fun"
                            (new js/Blob #js ["hello worlds"], #js {"type" "text/plain"})))))
  (go (.log js/console (<! (-bget my-store "blob-fun" identity))))
  )
