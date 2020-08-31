(ns konserve.core
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [konserve.protocols :refer [-exists? -get-meta -get -assoc-in
                                        -update-in -dissoc -bget -bassoc
                                        -keys]]
            [hasch.core :refer [uuid]]
            #?(:clj [clojure.core.async :refer [chan poll! put! <! go]]
               :cljs [cljs.core.async :refer [chan poll! put! <!]]))
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go]]
                            [konserve.core :refer [go-locked]])))

(defn- cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

#?(:clj
   (defmacro if-cljs
     "Return then if we are generating cljs code and else for Clojure code.
     https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
     [then else]
     (if (cljs-env? &env) then else)))

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

#?(:clj
   (defmacro go-locked [store key & code]
     (let [res `(if-cljs
                 (cljs.core.async.macros/go
                   (let [l# (get-lock ~store ~key)]
                     (try
                       (cljs.core.async/<! l#)
                       ~@code
                       (finally
                         (cljs.core.async/put! l# :unlocked)))))
                 (go
                   (let [l# (get-lock ~store ~key)]
                     (try
                       (<! l#)
                       ~@code
                       (finally
                         (put! l# :unlocked))))))]
       res)))

(defn exists?
  "Checks whether value is in the store."
  [store key]
  (go-locked
   store key
   (<! (-exists? store key))))

(defn get-in
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key-vec]
   (get-in store key-vec nil))
  ([store key-vec not-found]
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
   (get-in store [key] not-found)))

(defn get-meta
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get-meta store key nil))
  ([store key not-found]
   (go-locked
    store key
    (let [a (<! (-get-meta store key))]
      (if (some? a)
        a
        not-found)))))

(defn meta-update
  "Meta Data has following 'edn' format
  {:key 'The stored key'
   :type 'The type of the stored value binary or edn'
   ::timestamp 'Java.util.Date.'}
  Returns the meta value of the stored key-value tupel. Returns metadata if the key
  value not exist, if it does it will update the timestamp to date now. "
  [key type old]
  (if (empty? old)
    {:key key :type type ::timestamp (java.util.Date.)}
    (clojure.core/assoc old ::timestamp (java.util.Date.))))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  [store key-vec up-fn & args]
  (go-locked
   store (first key-vec)
   (<! (-update-in store key-vec (partial meta-update (first key-vec) :edn) up-fn args))))

(defn update
  "Updates a position described by key by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  [store key meta-up-fn fn & args]
  (apply update-in store [key] meta-up-fn fn args))

(defn assoc-in
  "Associates the key-vec to the value, any missing collections for
  the key-vec (nested maps and vectors) are newly created."
  [store key-vec val & args]
  (go-locked
   store (first key-vec)
   (<! (-assoc-in store key-vec (partial meta-update (first key-vec) :edn) val))))

(defn assoc
  "Associates the key-vec to the value, any missing collections for
 the key-vec (nested maps and vectors) are newly created."
  [store key val]
  (assoc-in store [key] val))

(defn dissoc
  "Removes an entry from the store. "
  [store key]
  (go-locked
   store key
   (<! (-dissoc store key))))

(defn append
  "Append the Element to the log at the given key or create a new append log there.
  This operation only needs to write the element and pointer to disk and hence is useful in write-heavy situations."
  [store key elem]
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
  (go-locked
   store key
   (<! (-bget store key locked-cb))))

(defn bassoc
  "Copies given value (InputStream, Reader, File, byte[] or String on
  JVM, Blob in JavaScript) under key in the store."
  [store key val]
  (go-locked
   store key
   (<! (-bassoc store key (partial meta-update key :binary) val))))

;rename list-meta 
(defn keys
  "Return a channel that will yield all top-level keys currently in the store."
  ([store] (-keys store)))
