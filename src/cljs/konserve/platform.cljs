(ns konserve.platform
  "Platform specific io operations cljs."
  (:require [konserve.protocols :refer [IAsyncKeyValueStore -get-in -assoc-in -update-in]]
            [cljs.reader :refer [read-string]]
            [cljs.core.async :as async :refer (take! <! >! put! close! chan)])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))


(defn log [& s]
  (.log js/console (apply str s)))


(defn error-handler [topic res e]
  (.log js/console topic e) (close! res) (throw e))

(defrecord IndexedDBKeyValueStore [db store-name]
  IAsyncKeyValueStore
  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          res (chan)
          tx (.transaction db #js [store-name])
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (partial error-handler (str "cannot get-in" fkey) res))
      (set! (.-onsuccess req)
            (fn [e] (when-let [r (.-result req)]
                      (put! res (-> r
                                      (aget "edn_value")
                                      read-string
                                      (get-in rkey))))
              ;; returns nil
              (close! res)))
      res))
  (-assoc-in [this key-vec value]
    (let [[fkey & rkey] key-vec
          res (chan)
          tx (.transaction db #js [store-name] "readwrite")
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (partial error-handler (str "cannot get for assoc-in" fkey) res))
      (set! (.-onsuccess req)
            (fn read-old [e]
              (let [old (when-let [r (.-result req)]
                          (-> r (aget "edn_value") read-string))
                    up-req (if (or value (not (empty? rkey)))
                             (.put obj-store
                                   (clj->js {:key (pr-str fkey)
                                             :edn_value
                                             (pr-str (if-not (empty? rkey)
                                                       (assoc-in old rkey value)
                                                       value))}))
                             (.delete obj-store (pr-str fkey)))]
                (set! (.-onerror up-req)
                      (partial error-handler (str "cannot put for assoc-in" fkey) res))
                (set! (.-onsuccess up-req)
                      (fn [e] (close! res))))))
      res))
  (-update-in [this key-vec up-fn]
    (let [[fkey & rkey] key-vec
          res (chan)
          tx (.transaction db #js [store-name] "readwrite")
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (partial error-handler (str "cannot get for update-in" fkey) res))
      (set! (.-onsuccess req)
            (fn read-old [e]
              (let [old (when-let [r (.-result req)]
                          (-> r (aget "edn_value") read-string))
                    new (if-not (empty? rkey)
                          (update-in old rkey up-fn)
                          (up-fn old))
                    up-req (if new
                             (.put obj-store
                                   (clj->js {:key (pr-str fkey)
                                             :edn_value
                                             (pr-str new)}))
                             (.delete obj-store (pr-str fkey)))]
                (set! (.-onerror up-req)
                      (partial error-handler (str "cannot put for update-in" fkey) res))
                (set! (.-onsuccess up-req)
                      (fn [e]
                        (put! res [(get-in old rkey) (get-in new rkey)])
                        (close! res))))))
      res)))

(defrecord IndexedDBJSONKeyValueStore [db store-name]
  IAsyncKeyValueStore
  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          res (chan)
          tx (.transaction db #js [store-name])
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (partial error-handler (str "cannot get-in" fkey) res))
      (set! (.-onsuccess req)
            (fn [e] (when-let [r (.-result req)]
                      (put! res (-> r
                                    (aget "json_value")
                                    js->clj
                                    (get-in rkey))))
              ;; returns nil
              (close! res)))
      res))
  (-assoc-in [this key-vec value]
    (let [[fkey & rkey] key-vec
          res (chan)
          tx (.transaction db #js [store-name] "readwrite")
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (partial error-handler (str "cannot get for assoc-in" fkey) res))
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
                      (partial error-handler (str "cannot put for assoc-in" fkey) res))
                (set! (.-onsuccess up-req)
                      (fn [e] (close! res))))))
      res))
  (-update-in [this key-vec up-fn]
    (let [[fkey & rkey] key-vec
          res (chan)
          tx (.transaction db #js [store-name] "readwrite")
          obj-store (.objectStore tx store-name)
          req (.get obj-store (pr-str fkey))]
      (set! (.-onerror req)
            (partial error-handler (str "cannot get for update-in" fkey) res))
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
                      (partial error-handler (str "cannot put for update-in" fkey) res))
                (set! (.-onsuccess up-req)
                      (fn [e]
                        (put! res [(get-in old rkey) (get-in new rkey)])
                        (close! res))))))
      res)))


(defn new-indexeddb-store [name]
  (let [res (chan)
        req (.open js/window.indexedDB name 1)]
    (set! (.-onerror req)
          (partial error-handler "ERROR opening DB:" res))
    (set! (.-onsuccess req)
          (fn success-handler [e]
            (log "db-opened:" (.-result req))
            (put! res (IndexedDBKeyValueStore. (.-result req) name))))
    (set! (.-onupgradeneeded req)
          (fn upgrade-handler [e]
            (let [db (-> e .-target .-result)]
              (.createObjectStore db name #js {:keyPath "key"}))
            (log "db upgraded from version: " (.-oldVersion e))))
    res))

(defn create-store [type db store-name]
  (log "create-store" type)
  (case type
    :edn (IndexedDBKeyValueStore. db store-name)
    :json (IndexedDBJSONKeyValueStore. db store-name)))

(defn new-indexeddb-store 
  ([name type]
  (let [res (chan)
        req (.open js/window.indexedDB name 1)]
    (set! (.-onerror req)
          (partial error-handler "ERROR opening DB:" res))
    (set! (.-onsuccess req)
          (fn success-handler [e]
            (log "db-opened:" (.-result req))
            (put! res (create-store type (.-result req) name))))
    (set! (.-onupgradeneeded req)
          (fn upgrade-handler [e]
            (let [db (-> e .-target .-result)]
              (.createObjectStore db name #js {:keyPath "key"}))
            (log "db upgraded from version: " (.-oldVersion e))))
    res))
  ([name] 
   (new-indexeddb-store name :edn)))


(comment
  ;; fire up repl
  (do
    (ns dev)
    (def repl-env (reset! cemerick.austin.repls/browser-repl-env
                          (cemerick.austin/repl-env)))
    (cemerick.austin.repls/cljs-repl repl-env))


  (go (def my-db (<! (new-indexeddb-store "konserve"))))

  (go (println "get:" (<! (-get-in my-db ["test" :a]))))

  (go (doseq [i (range 10)]
        (<! (-assoc-in my-db [i] i))))

  (go (doseq [i (range 10)]
        (println (<! (-get-in my-db [i])))))

  (go (println (<! (-assoc-in my-db ["test2"] {:a 1 :b 4.2}))))

  (go (println (<! (-assoc-in my-db ["test" :a] 43))))

  (go (println (<! (-update-in my-db ["test" :a] inc)))))
