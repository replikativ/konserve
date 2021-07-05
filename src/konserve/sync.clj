(ns konserve.sync
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [konserve.protocols :refer [-sync-exists? -sync-get-meta -sync-get -sync-assoc-in
                                        -sync-update-in -sync-dissoc -sync-bget -sync-bassoc
                                        -sync-keys]]
            [hasch.core :refer [uuid]]
            [taoensso.timbre :as timbre :refer [trace]]
            [konserve.utils :refer [meta-update]]
            [clojure.core.async :refer [chan poll! put!]]))


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


(defn exists?
  "Checks whether value is in the store."
  [store key]
  (trace "exists? on key " key)
  (locked
   store key
   (-sync-exists? store key)))

(defn get-in
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key-vec]
   (get-in store key-vec nil))
  ([store key-vec not-found]
   (trace "get-in on key " key)
   (locked
    store key-vec
    (let [a (-sync-get store (first key-vec))]
      (if (some? a)
        (clojure.core/get-in a (rest key-vec))
        not-found)))))

(defn get
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get store key nil))
  ([store key not-found]
   (trace "get-in on key " key)
   (get-in store [key] not-found)))

(defn get-meta
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get-meta store key nil))
  ([store key not-found]
   (trace "get-meta on key " key)
   (locked
    store key
    (let [a (-sync-get-meta store key)]
      (if (some? a)
        a
        not-found)))))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  [store key-vec up-fn & args]
  (trace "update-in on key " key)
  (locked
   store (first key-vec)
   (-sync-update-in store key-vec (partial meta-update (first key-vec) :edn) up-fn args)))

(defn update
  "Updates a position described by key by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  [store key meta-up-fn fn & args]
  (trace "update on key " key)
  (apply update-in store [key] meta-up-fn fn args))

(defn assoc-in
  "Associates the key-vec to the value, any missing collections for
  the key-vec (nested maps and vectors) are newly created."
  [store key-vec val & args]
  (trace "assoc-in on key " key)
  (locked
   store (first key-vec)
   (-sync-assoc-in store key-vec (partial meta-update (first key-vec) :edn) val)))

(defn assoc
  "Associates the key-vec to the value, any missing collections for
 the key-vec (nested maps and vectors) are newly created."
  [store key val]
  (trace "assoc on key " key)
  (assoc-in store [key] val))

(defn dissoc
  "Removes an entry from the store. "
  [store key]
  (trace "dissoc on key " key)
  (locked
   store key
   (-sync-dissoc store key)))

(defn append
  "Append the Element to the log at the given key or create a new append log there.
  This operation only needs to write the element and pointer to disk and hence is useful in write-heavy situations."
  [store key elem]
  (trace "append on key " key)
  (locked
   store key
   (let [head (-sync-get store key)
         [append-log? last-id first-id] head
         new-elem {:next nil
                   :elem elem}
         id (uuid)]
     (when (and head (not= append-log? :append-log))
       (throw (ex-info "This is not an append-log." {:key key})))
     (-sync-update-in store [id] (partial meta-update key :append-log) (fn [_] new-elem) [])
     (when first-id
       (-sync-update-in store [last-id :next] (partial meta-update key :append-log) (fn [_] id) []))
     (-sync-update-in store [key] (partial meta-update key :append-log) (fn [_] [:append-log id (or first-id id)]) [])
     [first-id id])))

(defn log
  "Loads the whole append log stored at "
  [store key]
  (trace "log on key " key)
  (let [head (get store key)
        [append-log? last-id first-id] head]
    (when (and head (not= append-log? :append-log))
      (throw (ex-info "This is not an append-log." {:key key})))
    (when first-id
      (loop [{:keys [next elem]} (get store first-id)
             hist []]
        (if next
          (recur (get store next)
                 (conj hist elem))
          (conj hist elem))))))

(defn reduce-log
  "Loads the whole append log stored at "
  [store key reduce-fn acc]
  (trace "reduce-log on key " key)
  (let [head (get store key)
        [append-log? last-id first-id] head]
    (when (and head (not= append-log? :append-log))
      (throw (ex-info "This is not an append-log." {:key key})))
    (if first-id
      (loop [id first-id
             acc acc]
        (let [{:keys [next elem]} (get store id)]
          (if (and next (not= id last-id))
            (recur next (reduce-fn acc elem))
            (reduce-fn acc elem))))
      acc)))

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
  [store key locked-cb]
  (trace "bget on key " key)
  (locked
   store key
   (-sync-bget store key locked-cb)))

(defn bassoc
  "Copies given value (InputStream, Reader, File, byte[] or String on
  JVM, Blob in JavaScript) under key in the store."
  [store key val]
  (trace "bassoc on key " key)
  (locked
   store key
   (-sync-bassoc store key (partial meta-update key :binary) val)))

(defn keys
  "Return a channel that will yield all top-level keys currently in the store."
  ([store]
   (trace "fetching keys")
   (-sync-keys store)))
