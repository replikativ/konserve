(ns konserve.platform
  "Platform specific io operations cljs."
  (:require [konserve.protocols :refer [IEDNAsyncKeyValueStore -get-in -assoc-in -update-in
                                        IJSONAsyncKeyValueStore -jget-in -jassoc-in -jupdate-in]]
            [konserve.literals :refer [TaggedLiteral]]
            [cljs.reader :refer [read-string]]
            [cljs.core.async :as async :refer (take! <! >! put! close! chan)])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))


(defn log [& s]
  (.log js/console (apply str s)))

(defn error-handler [topic res e]
  (.log js/console topic e) (close! res) (throw e))


(extend-protocol IPrintWithWriter
  konserve.literals.TaggedLiteral
  (-pr-writer [coll writer opts] (-write writer (str "#" (:tag coll) " " (:value coll)))))

(defn read-string-safe [tag-table s]
  (binding [cljs.reader/*tag-table* (atom (merge tag-table
                                                 (select-keys @cljs.reader/*tag-table*
                                                              #{"inst" "uuid" "queue"})))
            cljs.reader/*default-data-reader-fn*
            (atom (fn [tag val]
                    (konserve.literals.TaggedLiteral. tag val)))]
    (read-string s)))


(defrecord IndexedDBKeyValueStore [db store-name tag-table]
  IEDNAsyncKeyValueStore
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
                     (put! res (get-in (->> (aget r "edn_value")
                                            (read-string-safe @tag-table))
                                       rkey)))
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
                          (->> (aget r "edn_value") (read-string-safe @tag-table)))
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
                          (->> (aget r "edn_value")
                               (read-string-safe @tag-table)))
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



;; TODO find smart ways to either make edn as fast or share parts of the implementation
(defrecord IndexedDBJSONKeyValueStore [db store-name]
  IJSONAsyncKeyValueStore
  (-jget-in [this key-vec]
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
  (-jassoc-in [this key-vec value]
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
  (-jupdate-in [this key-vec up-fn]
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


(defn new-indexeddb-edn-store
  "Create an IndexedDB backed edn store with tag-table if you need custom types/records,
e.g. {'namespace.Symbol (fn [val] ...)}"
  ([name] (new-indexeddb-edn-store name (atom {})))
  ([name tag-table]
     (let [res (chan)
           req (.open js/window.indexedDB name 1)]
       (set! (.-onerror req)
             (partial error-handler "ERROR opening DB:" res))
       (set! (.-onsuccess req)
             (fn success-handler [e]
               (log "db-opened:" (.-result req))
               (put! res (IndexedDBKeyValueStore. (.-result req) name tag-table))))
       (set! (.-onupgradeneeded req)
             (fn upgrade-handler [e]
               (let [db (-> e .-target .-result)]
                 (.createObjectStore db name #js {:keyPath "key"}))
               (log "db upgraded from version: " (.-oldVersion e))))
       res)))

(defn new-indexeddb-json-store
  [name]
  (let [res (chan)
        req (.open js/window.indexedDB name 1)]
    (set! (.-onerror req)
          (partial error-handler "ERROR opening DB:" res))
    (set! (.-onsuccess req)
          (fn success-handler [e]
            (log "db-opened:" (.-result req))
            (put! res (IndexedDBJSONKeyValueStore. (.-result req) name))))
    (set! (.-onupgradeneeded req)
          (fn upgrade-handler [e]
            (let [db (-> e .-target .-result)]
              (.createObjectStore db name #js {:keyPath "key"}))
            (log "db upgraded from version: " (.-oldVersion e))))
    res))


(comment
  ;; fire up repl
  (do
    (ns dev)
    (def repl-env (reset! cemerick.austin.repls/browser-repl-env
                          (cemerick.austin/repl-env)))
    (cemerick.austin.repls/cljs-repl repl-env))


  (go (def my-store (<! (new-indexeddb-edn-store "konserve"))))
  ;; or
  (go (def game-store (<! (new-indexeddb-json-store "game-database"))))

  (go (println "get:" (<! (-get-in my-store ["test" :a]))))

  (let [store my-store]
    (go (doseq [i (range 10)]
          (<! (-assoc-in store [i] i)))
        (doseq [i (range 10)]
          (println (<! (-get-in store [i]))))))

  (defrecord Test [a])
  (go (println (<! (-assoc-in my-store ["rec-test"] (Test. 5)))))
  (go (println (<! (-get-in my-store ["rec-test"]))))

  (go (println (<! (-assoc-in my-store ["test2"] {:a 1 :b 4.2}))))

  (go (println (<! (-assoc-in my-store ["test" :a] 43))))

  (go (println (<! (-update-in my-store ["test" :a] inc)))))
