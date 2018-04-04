(ns konserve.nodejs
  (:require [cljs.nodejs :as node]
            [konserve.core :as k]
            [hasch.core :refer [uuid]]
            [konserve.serializers :as ser]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore -exists?
-get-in -update-in
                                        PJSONAsyncKeyValueStore
-jget-in -jassoc-in -jupdate-in
                                        PBinaryAsyncKeyValueStore -bget
-bassoc
                                        -serialize -deserialize]]
            [cljs.core.async :as async :refer (take! <! >! put! close!
chan poll!)])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))

(defonce fs (node/require "fs"))

(defonce stream (node/require "stream"))

(defn delete-store
  "Permanently deletes the folder of the store with all files."
  [folder]
  (if (fs.existsSync folder)
    (try
      (doseq [path [(str folder "/meta") (str folder "/data") folder]]
        (when (fs.existsSync path)
          (doseq [file (fs.readdirSync path)]
            (let [file-path (str path "/" file)]
              (when (fs.existsSync file-path)
                (if (.isDirectory (fs.statSync file-path))
                  (fs.rmdirSync file-path)
                  (fs.unlinkSync file-path)))))))
      (catch js/Object err
        (println err))
      (finally
        (fs.rmdirSync folder)))
    "folder not exists"))

(defn check-and-create-folder
  "Creates Folder with given path."
  [path]
  (let [test-file (str path "/" (random-uuid))]
    (try
      (when-not (fs.existsSync path)
        (fs.mkdirSync path))
      (fs.writeFileSync test-file "test-data")
      (fs.unlinkSync test-file)
      (catch js/Object err
        (println err)))))

(defrecord FileSystemNodejsStore
    [folder serializer read-handlers write-handlers locks config]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    (let [fn  (uuid key)
          f   (str folder "/data/" key)
          res (chan)]
      (put! res (fs.existsSync f))
      (close! res)
      res)))

(defn new-fs-store
  "Filestore contains a Key and a Data Folder"
  [path & {:keys  [read-handlers write-handlers serializer config]
           :or    {read-handlers  (atom {})
                   write-handlers (atom {})
                   serializer     (ser/string-serializer)
                   config {:fsync true}}}]
  (let [_         (check-and-create-folder path)
        _         (check-and-create-folder (str path "/meta"))
        _         (check-and-create-folder (str path "/data"))]
    (go (map->FileSystemNodejsStore {:folder         path
                                     :serializer     serializer
                                     :read-handlers  read-handlers
                                     :write-handlers write-handlers
                                     :locks          (atom {})
                                     :config         config}))))

(comment
  ;create store
  (go (def store (<! (new-fs-store "mystore"))))
  ;delete store
  (delete-store "mystore")
  ;exists?
  (go (println (<! (-exists? store "ne1w.txt"))))
;;Input, Outputstream

(def my-obj (clj->js {:key "123" :format :edn}))

(defn- read-key
  [filename key]
  (when (fs.existsSync path)
    (let [rs (fs.createReadStream path)]
      (.on rs "data" (fn [chunk] (set! scope (.toString chunk))))
      (.on rs "close" #(println "Reading done"))
      (.on rs "error" (fn [err] (println "error: " err))))))


(defn write-edn-key [folder {:keys [key] :as meta}]
  (let [key       (uuid key)
        temp-file (str folder "/meta/" key ".new")
        new-file  (str folder "/meta/" key)
        fd        (fs.openSync new-file "w+")
        _         (fs.closeSync fd)
        ws        (fs.createWriteStream temp-file)]
    (.on ws "close" #(try (fs.renameSync temp-file new-file) (catch js/Object err (println err))))
    (.write ws (prn-str meta))
    (.end ws)))

(defn write-edn [folder key value]
  (let [key       (uuid key)
        temp-file (str folder "/data/" key ".new")
        new-file  (str folder "/data/" key)
        old       (read-key new-file key)
        fd        (fs.openSync new-file "w+")
        _         (fs.closeSync fd)
        ws        (fs.createWriteStream temp-file)]
    (.on ws "close" #(try
                       (fs.renameSync temp-file new-file)
                       (catch js/Object err (println err))))
    (.write ws (prn-str value))
    (.end ws)))

(defn read-edn [path key]
  (let [file-name (str path "/meta/" (uuid key))
        res-ch    (chan)]
    (when (fs.existsSync file-name)
      (let [rs      (fs.createReadStream file-name)]
        (.on rs "data" (fn [chunk] (put! res-ch  (cljs.reader/read-string (.toString chunk)))))
        (.on rs "close" #(close! res-ch))
        (.on rs "error" (fn [err] (close! res-ch) (println "error: " err)))
        res-ch))))

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

