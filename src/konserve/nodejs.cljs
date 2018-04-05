(ns konserve.nodejs
  (:require [cljs.nodejs :as node]
            [konserve.core :as k]
            [hasch.core :refer [uuid]]
            [konserve.serializers :as ser]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore -exists? -get-in -update-in -assoc-in -dissoc
                                        PJSONAsyncKeyValueStore -jget-in -jassoc-in -jupdate-in
                                        PBinaryAsyncKeyValueStore -bget -bassoc
                                        -serialize -deserialize]]
            [cljs.core.async :as async :refer (take! <! >! put! close! chan poll!)])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))

(defonce fs (node/require "fs"))

(defonce stream (node/require "stream"))

(defn delete-store
  "Permanently deletes the folder of the store with all files."
  [folder]
  (if (.existsSync fs folder)
    (try
      (doseq [path [(str folder "/meta") (str folder "/data") folder]]
        (when (.existsSync fs path)
          (doseq [file (.readdirSync fs path)]
            (let [file-path (str path "/" file)]
              (when (.existsSync fs file-path)
                (if (.isDirectory (.statSync fs file-path))
                  (.rmdirSync fs file-path)
                  (.unlinkSync fs file-path)))))))
      (catch js/Object err
        (println err))
      (finally
        (.rmdirSync fs folder)))
    "folder not exists"))

(defn check-and-create-folder
  "Creates Folder with given path."
  [path]
  (let [test-file (str path "/" (random-uuid))]
    (println "check and create folder")
    (try
      (when-not (fs.existsSync path)
        (.mkdirSync fs path))
      (.writeFileSync fs test-file "test-data")
      (.unlinkSync fs test-file)
      (catch js/Object err
        (println err)))))

(defn write-edn-key [folder {:keys [key] :as meta}]
  (let [key       (uuid key)
        temp-file (str folder "/meta/" key ".new")
        new-file  (str folder "/meta/" key)
        fd        (.openSync fs new-file "w+")
        _         (.closeSync fs fd)
        ws        (.createWriteStream fs temp-file)]
    (.on ws "close" #(try (.renameSync fs temp-file new-file) (catch js/Object err (println err))))
    (.on ws "error" (fn [err] (println "error: " err)))
    (.write ws (prn-str meta))
    (.end ws)))

(defn write-edn [folder key up-fn]
  (let [key       (uuid key)
        temp-file (str folder "/data/" key ".new")
        new-file  (str folder "/data/" key)
        res-ch    (chan)]
    (if (.existsSync fs new-file)
      (let [rs (.createReadStream fs new-file)]
        (println "file exist")
        (.on rs "data" (fn [chunk]
                         (let [old (cljs.reader/read-string (.toString chunk))
                               ws  (.createWriteStream fs temp-file)
                               value     (up-fn old)]
                           (.on ws "finish" #(put! res-ch [old value]))
                           (.on ws "error" (fn [err] (close! res-ch) (println "error: " err)))
                           (.write ws (prn-str value))
                           (.close ws))))
        (.on rs "close" (fn [] (.renameSync fs temp-file new-file) (close! res-ch)))
        (.on rs "error" (fn [err] (close! res-ch) (println "error: " err))))
      (let [fd        (.openSync fs new-file "w+")
            _         (.closeSync fs fd)
            ws        (.createWriteStream fs temp-file)
            value     (up-fn)]
        (println "file not exist")
        (.on ws "close" #(try
                           (.renameSync fs temp-file new-file)
                           (catch js/Object err (println err))))
        (.write ws (prn-str value))
        (.end ws)))
    res-ch))

(defn read-edn [path key]
  (let [file-name (str path "/data/" (uuid key))
        res-ch    (chan)]
    (if (.existsSync fs file-name)
      (let [rs      (.createReadStream fs file-name)]
        (.on rs "data" (fn [chunk] (put! res-ch  (cljs.reader/read-string (.toString chunk)))))
        (.on rs "close" #(close! res-ch))
        (.on rs "error" (fn [err] (close! res-ch) (println "error: " err))))
      (do (put! res-ch "File not exist")
          (close! res-ch)))
    res-ch))

(defn delete-entry [folder file-name res-ch]
  (let [key-file (str folder "/meta/" file-name)
        data-file (str folder "/data/" file-name)]
    (.unlink fs key-file
             (fn [err]
               (if err
                 (if (= (aget err "code") "ENOENT")
                   (do (put! res-ch "File not exist")
                       (close! res-ch err))
                   (throw err))
                 (do (.unlink fs data-file
                              (fn [err]
                                (if err
                                  (throw err))))
                     (put! res-ch "true")
                     (close! res-ch)))))))

(defrecord FileSystemNodejsStore
    [folder serializer read-handlers write-handlers locks config]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    (let [fn  (uuid key)
          f   (str folder "/data/" fn)
          res (chan)]
      (put! res (.existsSync fs f))
      (close! res)
      res))
  (-get-in [this key-vec]
    (read-edn folder (first key-vec)))
  (-update-in [this key-vec up-fn]
    (go (write-edn-key folder {:key (first key-vec) :format :edn}))
        (write-edn folder (first key-vec) up-fn))
  (-assoc-in [this key-vec val] (-update-in this key-vec (fn [_] val)))
  (-dissoc [this key]
    (let [fn (uuid key)
          res-ch (chan)]
      (delete-entry folder fn res-ch)
      res-ch)))

(defn new-fs-store
  "Filestore contains a Key and a Data Folder"
  [path & {:keys  [read-handlers write-handlers serializer config]
           :or    {read-handlers  (atom {})
                   write-handlers (atom {})
                   serializer     (ser/string-serializer)
                   config {:fsync true}}}]
  (println "creating store")
  (let [_         (check-and-create-folder path)
        _         (check-and-create-folder (str path "/meta"))
        _         (check-and-create-folder (str path "/data"))]
    (println "creating system")
    (go (map->FileSystemNodejsStore {:folder         path
                                     :serializer     serializer
                                     :read-handlers  read-handlers
                                     :write-handlers write-handlers
                                     :locks          (atom {})
                                     :config         config}))))

(comment
  ;create store
  (go (def store (<! (new-fs-store "/tmp/mystore"))))

  (delete-store "mystore")
  ;exists?
  
  (go (println (<! (-assoc-in store [:new] 43))))
  (go (println (<! (-exists? store :new)))) 
  (go (println (<! (-update-in store [:new] inc))))
  (go (println (<! (-get-in store [:new]))))
  (go (println (<! (-dissoc store :new))))


;;Input, Outputstream

(def my-obj (clj->js {:key "123" :format :edn}))

(defn- read-key
  [filename key]
  (when (.existsSync fs path)
    (let [rs (.createReadStream fs path)]
      (.on rs "data" (fn [chunk] (set! scope (.toString chunk))))
      (.on rs "close" #(println "Reading done"))
      (.on rs "error" (fn [err] (println "error: " err))))))










(write-edn-key "/tmp" {:key "123" :format :edn})






(def rc (chan))

(go (println (<! (read-edn "/tmp" "123"))))

(defn read-my-key []
  (let [value (js-obj)]
    (go (set! (.-var value) (<! (read-edn "/tmp" "123"))))
    value))

(read-my-key)

(write-edn "myfile.txt" "Hello World")

(take! (read-edn "/tmp" "123") #(println %))

(def myatom (atom {}))

(poll! (read-edn "/tmp" "123"))

(async/reduce (<! (read-edn "/tmp" "123")))

;read back use -> cljs.reader/read-string
(cljs.reader/read-string (prn-str {:x "123" :y "123"}))

