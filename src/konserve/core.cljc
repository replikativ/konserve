(ns konserve.core
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [konserve.protocols :refer [-exists? -get-meta -get -assoc-in
                                        -update-in -dissoc -bget -bassoc
                                        -keys]]
            [hasch.core :refer [uuid]]
            [taoensso.timbre :as timbre :refer [trace]]
            [konserve.utils :refer [meta-update async+sync]]
            [clojure.core.async :refer [go chan poll! put! <!]]))


;; ACID

;; atomic
;; consistent
;; isolated
;; durable


(defn get-lock [{:keys [locks] :as store} key]
  (or (clojure.core/get @locks key)
      (let [c (chan)]
        (put! c :unlocked)
        (clojure.core/get (swap! locks (fn [old]
                                         (trace "creating lock for: " key)
                                         (if (old key) old
                                             (clojure.core/assoc old key c))))
                          key))))

(defmacro locked [store key & code]
  `(let [l# (get-lock ~store ~key)]
    (try
         ;; spin lock
      (while (not (poll! l#))
        (Thread/sleep (rand-int 20)))
      ~@code
      (finally
        (put! l# :unlocked)))))

(defmacro go-locked [store key & code]
  `(go
     (let [l# (get-lock ~store ~key)]
       (try
         (<! l#)
         (trace "acquired go-lock for: " ~key)
         ~@code
         (finally
           (trace "releasing go-lock for: " ~key)
           (put! l# :unlocked))))))


(defn exists?
  "Checks whether value is in the store."
  ([store key]
   (exists? store key false))
  ([store key sync?]
   (trace "exists? on key " key)
   (async+sync sync?
               {go-locked locked}
               (go-locked
                store key
                (-exists? store key sync?)))))

(defn get-in
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key-vec]
   (get-in store key-vec nil))
  ([store key-vec not-found]
   (get-in store key-vec not-found false))
  ([store key-vec not-found sync?]
   (trace "get-in on key " key-vec)
   (async+sync sync?
               {go-locked locked
                <! do}
               (go-locked
                store (first key-vec)
                (let [a (<! (-get store (first key-vec) sync?))]
                  (if (some? a)
                    (clojure.core/get-in a (rest key-vec))
                    not-found))))))

(defn get
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get store key nil))
  ([store key not-found]
   (get store key not-found false))
  ([store key not-found sync?]
   (get-in store [key] not-found sync?)))

(defn get-meta
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get-meta store key nil))
  ([store key not-found]
   (get-meta store key not-found false))
  ([store key not-found sync?]
   (trace "get-meta on key " key)
   (async+sync sync?
               {go-locked locked
                <! do}
               (go-locked
                store key
                (let [a (<! (-get-meta store key sync?))]
                  (if (some? a)
                    a
                    not-found))))))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  ([store key-vec up-fn]
   (update-in store key-vec up-fn false))
  ([store key-vec up-fn sync?]
   (trace "update-in on key " key)
   (async+sync sync?
               {go-locked locked
                <! do}
               (go-locked
                store (first key-vec)
                (<! (-update-in store key-vec (partial meta-update (first key-vec) :edn) up-fn sync?))))))

(defn update
  "Updates a position described by key by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  ([store key fn]
   (update store key fn false))
  ([store key fn sync?]
   (trace "update on key " key)
   (update-in store [key] fn sync?)))

(defn assoc-in
  "Associates the key-vec to the value, any missing collections for
  the key-vec (nested maps and vectors) are newly created."
  ([store key-vec val]
   (assoc-in store key-vec val false))
  ([store key-vec val sync?]
   (trace "assoc-in on key " key)
   (async+sync sync?
               {<! do}
               (go-locked
                store (first key-vec)
                (<! (-assoc-in store key-vec (partial meta-update (first key-vec) :edn) val sync?))))))

(defn assoc
  "Associates the key-vec to the value, any missing collections for
 the key-vec (nested maps and vectors) are newly created."
  ([store key val]
   (assoc store key val false))
  ([store key val sync?]
   (trace "assoc on key " key)
   (assoc-in store [key] val sync?)))

(defn dissoc
  "Removes an entry from the store. "
  ([store key]
   (dissoc store key false))
  ([store key sync?]
   (trace "dissoc on key " key)
   (async+sync sync?
               {go-locked locked
                <! do}
               (go-locked
                store key
                (<! (-dissoc store key sync?))))))

(defn append
  "Append the Element to the log at the given key or create a new append log there.
  This operation only needs to write the element and pointer to disk and hence is useful in write-heavy situations."
  ([store key elem]
   (append store key elem false))
  ([store key elem sync?]
   (trace "append on key " key)
   (async+sync sync?
               {go-locked locked
                <! do}
               (go-locked
                store key
                (let [head (<! (-get store key sync?))
                      [append-log? last-id first-id] head
                      new-elem {:next nil
                                :elem elem}
                      id (uuid)]
                  (when (and head (not= append-log? :append-log))
                    (throw (ex-info "This is not an append-log." {:key key})))
                  (<! (-update-in store [id] (partial meta-update key :append-log) (fn [_] new-elem) sync?))
                  (when first-id
                    (<! (-update-in store [last-id :next] (partial meta-update key :append-log) (fn [_] id) sync?)))
                  (<! (-update-in store [key] (partial meta-update key :append-log) (fn [_] [:append-log id (or first-id id)]) sync?))
                  [first-id id])))))

(defn log
  "Loads the whole append log stored at key."
  ([store key]
   (log store key false))
  ([store key sync?]
   (trace "log on key " key)
   (async+sync sync?
               {go do
                <! do}
               (go
                 (println "loading head")
                 (let [head (<! (get store key sync?))
                       _ (println "loaded head")
                       [append-log? last-id first-id] head]
                   (when (and head (not= append-log? :append-log))
                     (throw (ex-info "This is not an append-log." {:key key})))
                   (when first-id
                     (loop [{:keys [next elem]} (<! (get store first-id sync?))
                            hist []]
                       (println "next: " next)
                       (if next
                         (recur (<! (get store next))
                                (conj hist elem))
                         (conj hist elem)))))))))

(defn reduce-log
  "Loads the append log and applies reduce-fn over it."
  ([store key reduce-fn acc]
   (reduce-log store key reduce-fn acc false))
  ([store key reduce-fn acc sync?]
   (trace "reduce-log on key " key)
   (async+sync sync?
               {go do
                <! do}
               (go
                 (let [head (<! (get store key sync?))
                       [append-log? last-id first-id] head]
                   (when (and head (not= append-log? :append-log))
                     (throw (ex-info "This is not an append-log." {:key key})))
                   (if first-id
                     (loop [id first-id
                            acc acc]
                       (let [{:keys [next elem]} (<! (get store id sync?))]
                         (if (and next (not= id last-id))
                           (recur next (reduce-fn acc elem))
                           (reduce-fn acc elem))))
                     acc))))))

(defn bget
  "Calls locked-cb with a platform specific binary representation inside
  the lock, e.g. wrapped InputStream on the JVM and Blob in
  JavaScript. You need to properly close/dispose the object when you
  are done!

  You have to do all work in locked-cb, e.g.

  (fn [{is :input-stream}]
    (let [tmp-file (io/file \"/tmp/my-private-copy\")]
      (io/copy is tmp-file)))
  "
  ([store key locked-cb]
   (bget store key locked-cb false))
  ([store key locked-cb sync?]
   (trace "bget on key " key)
   (async+sync sync?
               {go-locked locked
                <! do}
               (go-locked
                store key
                (<! (-bget store key locked-cb sync?))))))

(defn bassoc
  "Copies given value (InputStream, Reader, File, byte[] or String on
  JVM, Blob in JavaScript) under key in the store."
  ([store key val]
   (bassoc store key val false))
  ([store key val sync?]
   (trace "bassoc on key " key)
   (async+sync sync?
               {go-locked locked
                <! do}
               (go-locked
                store key
                (<! (-bassoc store key (partial meta-update key :binary) val sync?))))))

(defn keys
  "Return a channel that will yield all top-level keys currently in the store."
  ([store]
   (keys store false))
  ([store sync?]
   (trace "fetching keys")
   (-keys store sync?)))
