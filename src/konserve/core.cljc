(ns konserve.core
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [konserve.protocols :refer [-exists? -get-meta -get -assoc-in
                                        -update-in -dissoc -bget -bassoc
                                        -keys]]
            [hasch.core :refer [uuid]]
            [taoensso.timbre :as timbre :refer [trace]]
            [clojure.core.async :refer [chan poll! put! <! go]])
  #?(:cljs (:require-macros [konserve.core :refer [go-locked]])))


;; TODO we keep one chan for each key in memory
;; as async ops seem to interfere with the atom state changes
;; and cause deadlock


(defn get-lock [{:keys [locks] :as store} key]
  (or (clojure.core/get @locks key)
      (let [c (chan)]
        (put! c :unlocked)
        (clojure.core/get (swap! locks (fn [old]
                                         (if (old key) old
                                             (clojure.core/assoc old key c))))
                          key))))

(defmacro go-locked [store key & code]
  `(go
     (let [l# (get-lock ~store ~key)]
       (try
         (<! l#)
         ~@code
         (finally
           (put! l# :unlocked))))))

(defn exists?
  "Checks whether value is in the store."
  [store key]
  (trace "exists? on key " key)
  (go-locked
   store key
   (<! (-exists? store key))))

(defn get-in
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key-vec]
   (get-in store key-vec nil))
  ([store key-vec not-found]
   (trace "get-in on key " key)
   (go-locked
    store key-vec
    (let [a (<! (-get store (first key-vec)))]
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
   (go-locked
    store key
    (let [a (<! (-get-meta store key))]
      (if (some? a)
        a
        not-found)))))

(defn now []
  #?(:clj (java.util.Date.)
     :cljs (js/Date.)))

(defn meta-update
  "Meta Data has following 'edn' format
  {:key 'The stored key'
   :type 'The type of the stored value binary or edn'
   ::timestamp Date object.}
  Returns the meta value of the stored key-value tupel. Returns metadata if the key
  value not exist, if it does it will update the timestamp to date now. "
  [key type old]
  (if (empty? old)
    {:key key :type type ::timestamp (now)}
    (clojure.core/assoc old ::timestamp (now))))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  [store key-vec up-fn & args]
  (trace "update-in on key " key)
  (go-locked
   store (first key-vec)
   (<! (-update-in store key-vec (partial meta-update (first key-vec) :edn) up-fn args))))

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
  (go-locked
   store (first key-vec)
   (<! (-assoc-in store key-vec (partial meta-update (first key-vec) :edn) val))))

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
  (go-locked
   store key
   (<! (-dissoc store key))))

(defn append
  "Append the Element to the log at the given key or create a new append log there.
  This operation only needs to write the element and pointer to disk and hence is useful in write-heavy situations."
  [store key elem]
  (trace "append on key " key)
  (go-locked
   store key
   (let [head (<! (-get store key))
         [append-log? last-id first-id] head
         new-elem {:next nil
                   :elem elem}
         id (uuid)]
     (when (and head (not= append-log? :append-log))
       (throw (ex-info "This is not an append-log." {:key key})))
     (<! (-update-in store [id] (partial meta-update key :append-log) (fn [_] new-elem) []))
     (when first-id
       (<! (-update-in store [last-id :next] (partial meta-update key :append-log) (fn [_] id) [])))
     (<! (-update-in store [key] (partial meta-update key :append-log) (fn [_] [:append-log id (or first-id id)]) []))
     [first-id id])))

(defn log
  "Loads the whole append log stored at "
  [store key]
  (trace "log on key " key)
  (go
    (let [head (<! (get store key))
          [append-log? last-id first-id] head]
      (when (and head (not= append-log? :append-log))
        (throw (ex-info "This is not an append-log." {:key key})))
      (when first-id
        (loop [{:keys [next elem]} (<! (get store first-id))
               hist []]
          (if next
            (recur (<! (get store next))
                   (conj hist elem))
            (conj hist elem)))))))

(defn reduce-log
  "Loads the whole append log stored at "
  [store key reduce-fn acc]
  (trace "reduce-log on key " key)
  (go
    (let [head (<! (get store key))
          [append-log? last-id first-id] head]
      (when (and head (not= append-log? :append-log))
        (throw (ex-info "This is not an append-log." {:key key})))
      (if first-id
        (loop [id first-id
               acc acc]
          (let [{:keys [next elem]} (<! (get store id))]
            (if (and next (not= id last-id))
              (recur next (reduce-fn acc elem))
              (reduce-fn acc elem))))
        acc))))

(defn bget
  "Calls locked-cb with a platform specific binary representation inside
  the lock, e.g. wrapped InputStream on the JVM and Blob in
  JavaScript. You need to properly close/dispose the object when you
  are done!

  You have to do all work in a async thread of locked-cb, e.g.

  (fn [{is :input-stream}]
    (async/thread
      (let [tmp-file (io/file \"/tmp/my-private-copy\")]
        (io/copy is tmp-file))))
  "
  [store key locked-cb]
  (trace "bget on key " key)
  (go-locked
   store key
   (<! (-bget store key locked-cb))))

(defn bassoc
  "Copies given value (InputStream, Reader, File, byte[] or String on
  JVM, Blob in JavaScript) under key in the store."
  [store key val]
  (trace "bassoc on key " key)
  (go-locked
   store key
   (<! (-bassoc store key (partial meta-update key :binary) val))))

;rename list-meta 
(defn keys
  "Return a channel that will yield all top-level keys currently in the store."
  ([store]
   (trace "fetching keys")
   (-keys store)))
