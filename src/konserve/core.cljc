(ns konserve.core
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [konserve.protocols :refer [-exists? -get-meta -get-in -assoc-in
                                        -update-in -dissoc -bget -bassoc
                                        -keys]]
            [hasch.core :as hasch]
            [taoensso.timbre :refer [trace debug]]
            [konserve.utils :refer [meta-update #?(:clj async+sync) *default-sync-translation*]]
            [superv.async :refer [go-try- <?-]]
            [clojure.core.async :refer [chan put! poll!]])
  #?(:cljs (:require-macros [konserve.utils :refer [async+sync]]
                            [konserve.core :refer [go-locked locked]])))

;; ACID

;; atomic
;; consistent
;; isolated
;; durable

(defn get-lock [{:keys [locks] :as _store} key]
  (or (clojure.core/get @locks key)
      (let [c (chan)]
        (put! c :unlocked)
        (clojure.core/get (swap! locks (fn [old]
                                         (trace "creating lock for: " key)
                                         (if (old key) old
                                             (clojure.core/assoc old key c))))
                          key))))

(defn wait [lock]
  #?(:clj (while (not (poll! lock))
            (Thread/sleep (rand-int 20)))
     :cljs (debug "WARNING: konserve lock is not active. Only use the synchronous variant with the memory store in JavaScript.")))

(defmacro locked [store key & code]
  `(let [l# (get-lock ~store ~key)]
     (try
       (wait l#)
       (trace "acquired spin lock for " ~key)
       ~@code
       (finally
         (trace "releasing spin lock for " ~key)
         (put! l# :unlocked)))))

(defmacro go-locked [store key & code]
  `(go-try-
    (let [l# (get-lock ~store ~key)]
      (try
        (<?- l#)
        (trace "acquired go-lock for: " ~key)
        ~@code
        (finally
          (trace "releasing go-lock for: " ~key)
          (put! l# :unlocked))))))

(defn exists?
  "Checks whether value is in the store."
  ([store key]
   (exists? store key {:sync? false}))
  ([store key opts]
   (trace "exists? on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store key
                (<?- (-exists? store key opts))))))

(defn get-in
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key-vec]
   (get-in store key-vec nil))
  ([store key-vec not-found]
   (get-in store key-vec not-found {:sync? false}))
  ([store key-vec not-found opts]
   (trace "get-in on key " key-vec)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store (first key-vec)
                (<?- (-get-in store key-vec not-found opts))))))

(defn get
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get store key nil))
  ([store key not-found]
   (get store key not-found {:sync? false}))
  ([store key not-found opts]
   (get-in store [key] not-found opts)))

(defn get-meta
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get-meta store key nil))
  ([store key not-found]
   (get-meta store key not-found {:sync? false}))
  ([store key not-found opts]
   (trace "get-meta on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store key
                (let [a (<?- (-get-meta store key opts))]
                  (if (some? a)
                    a
                    not-found))))))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  ([store key-vec up-fn]
   (update-in store key-vec up-fn {:sync? false}))
  ([store key-vec up-fn opts]
   (trace "update-in on key " key-vec)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store (first key-vec)
                (<?- (-update-in store key-vec (partial meta-update (first key-vec) :edn) up-fn opts))))))

(defn update
  "Updates a position described by key by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  ([store key fn]
   (update store key fn {:sync? false}))
  ([store key fn opts]
   (trace "update on key " key)
   (update-in store [key] fn opts)))

(defn assoc-in
  "Associates the key-vec to the value, any missing collections for
  the key-vec (nested maps and vectors) are newly created."
  ([store key-vec val]
   (assoc-in store key-vec val {:sync? false}))
  ([store key-vec val opts]
   (trace "assoc-in on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store (first key-vec)
                (<?- (-assoc-in store key-vec (partial meta-update (first key-vec) :edn) val opts))))))

(defn assoc
  "Associates the key-vec to the value, any missing collections for
 the key-vec (nested maps and vectors) are newly created."
  ([store key val]
   (assoc store key val {:sync? false}))
  ([store key val opts]
   (trace "assoc on key " key)
   (assoc-in store [key] val opts)))

(defn dissoc
  "Removes an entry from the store. "
  ([store key]
   (dissoc store key {:sync? false}))
  ([store key opts]
   (trace "dissoc on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store key
                (<?- (-dissoc store key opts))))))

(defn append
  "Append the Element to the log at the given key or create a new append log there.
  This operation only needs to write the element and pointer to disk and hence is useful in write-heavy situations."
  ([store key elem]
   (append store key elem {:sync? false}))
  ([store key elem opts]
   (trace "append on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store key
                (let [head (<?- (-get-in store [key] nil opts))
                      [append-log? last-id first-id] head
                      new-elem {:next nil
                                :elem elem}
                      id (hasch/uuid)]
                  (when (and head (not= append-log? :append-log))
                    (throw (ex-info "This is not an append-log." {:key key})))
                  (<?- (-update-in store [id] (partial meta-update key :append-log) (fn [_] new-elem) opts))
                  (when first-id
                    (<?- (-update-in store [last-id :next] (partial meta-update key :append-log) (fn [_] id) opts)))
                  (<?- (-update-in store [key] (partial meta-update key :append-log) (fn [_] [:append-log id (or first-id id)]) opts))
                  [first-id id])))))

(defn log
  "Loads the whole append log stored at key."
  ([store key]
   (log store key {:sync? false}))
  ([store key opts]
   (trace "log on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-try-
                (let [head (<?- (get store key nil opts))
                      [append-log? _last-id first-id] head]
                  (when (and head (not= append-log? :append-log))
                    (throw (ex-info "This is not an append-log." {:key key})))
                  (when first-id
                    (loop [{:keys [next elem]} (<?- (get store first-id nil opts))
                           hist []]
                      (if next
                        (recur (<?- (get store next nil opts))
                               (conj hist elem))
                        (conj hist elem)))))))))

(defn reduce-log
  "Loads the append log and applies reduce-fn over it."
  ([store key reduce-fn acc]
   (reduce-log store key reduce-fn acc {:sync? false}))
  ([store key reduce-fn acc opts]
   (trace "reduce-log on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-try-
                (let [head (<?- (get store key nil opts))
                      [append-log? last-id first-id] head]
                  (when (and head (not= append-log? :append-log))
                    (throw (ex-info "This is not an append-log." {:key key})))
                  (if first-id
                    (loop [id first-id
                           acc acc]
                      (let [{:keys [next elem]} (<?- (get store id nil opts))]
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
   (bget store key locked-cb {:sync? false}))
  ([store key locked-cb opts]
   (trace "bget on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store key
                (<?- (-bget store key locked-cb opts))))))

(defn bassoc
  "Copies given value (InputStream, Reader, File, byte[] or String on
  JVM, Blob in JavaScript) under key in the store."
  ([store key val]
   (bassoc store key val {:sync? false}))
  ([store key val opts]
   (trace "bassoc on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store key
                (<?- (-bassoc store key (partial meta-update key :binary) val opts))))))

(defn keys
  "Return a channel that will yield all top-level keys currently in the store."
  ([store]
   (keys store {:sync? false}))
  ([store opts]
   (trace "fetching keys")
   (-keys store opts)))
