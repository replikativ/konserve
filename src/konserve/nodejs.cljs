(ns konserve.nodejs
  (:require [cljs.nodejs :as node]
            [konserve.core :as k]
            [hasch.core :refer [uuid]]
            [fressian-cljs.core :as fress]
            [konserve.serializers :as ser]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore -exists? -get-in -update-in -assoc-in -dissoc
                                        PJSONAsyncKeyValueStore -jget-in -jassoc-in -jupdate-in
                                        PBinaryAsyncKeyValueStore -bget -bassoc
                                        -serialize -deserialize]]
            [cljs-node-io.core :as io :refer [slurp spit]]
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
  (let [key       (uuid (first key))
        temp-file (str folder "/meta/" key ".new")
        new-file  (str folder "/meta/" key)
        fd        (.openSync fs new-file "w+")
        _         (.closeSync fs fd)
        ws        (.createWriteStream fs temp-file)
        res-ch (chan)]
    (.on ws "close" #(try (.renameSync fs temp-file new-file)
                          (catch js/Object err
                            (put! res-ch (ex-info "Could not write edn key."
                                                  {:type      :write-edn-key-error
                                                   :key       key
                                                   :exception err}))
                            (close! res-ch))
                          (finally (close! res-ch))))
    (.on ws "error"
         (fn [err]
           (put! res-ch (ex-info "Could not write edn key."
                                 {:type      :write-edn-key-error
                                  :key       key
                                  :exception err}))
           (close! res-ch)))
    (.write ws (js/Uint8Array. (fress/write meta)))
    (.end ws)
    res-ch))

(defn write-edn [folder key up-fn]
  (let [key       (uuid (first key))
        temp-file (str folder "/data/" key ".new")
        new-file  (str folder "/data/" key)
        res-ch    (chan)]
    (if (.existsSync fs new-file)
      (let [rs (.createReadStream fs new-file)]
        (.on rs "data" (fn [chunk]
                         (let [old (fress/read (. chunk -buffer))
                               ws  (.createWriteStream fs temp-file)
                               value     (up-fn old)]
                           (.on ws "finish" #(put! res-ch [old value]))
                           (.on ws "error"
                                (fn [err]
                                  (put! res-ch (ex-info "Could not write edn."
                                                        {:type      :write-edn-error
                                                         :key       key
                                                         :exception err}))
                                  (close! res-ch)))
                           (.write ws (js/Uint8Array. (fress/write value)))
                           (.close ws))))
        (.on rs "close" (fn [] (.renameSync fs temp-file new-file) (close! res-ch)))
        (.on rs "error" (fn [err]
                          (put! res-ch (ex-info "Could not write edn."
                                                {:type      :write-edn-error
                                                 :key       key
                                                 :exception err}))
                          (close! res-ch))))
      (let [fd        (.openSync fs new-file "w+")
            _         (.closeSync fs fd)
            ws        (.createWriteStream fs temp-file)
            value     (up-fn nil)]
        (.on ws "finish" #(put! res-ch [nil value]))
        (.on ws "close" #(try
                           (.renameSync fs temp-file new-file)
                           (catch js/Object err
                             (put! res-ch (ex-info "Could not write edn."
                                                   {:type      :write-edn-error
                                                    :key       key
                                                    :exception err}))
                             (close! res-ch))))
        (.write ws (js/Uint8Array. (fress/write value)))
        (.end ws)))
    res-ch))

(defn write-binary [folder key input]
  (let [res-ch (chan)]
    (try
      (let [fn        (uuid key)
            temp-file (str folder "/data/" fn ".new")
            new-file  (str folder "/data/" fn)
            ws        (.createWriteStream fs temp-file)
            input     (if (js/Buffer.isBuffer input)
                        (let [stream (stream.PassThrough)
                              _      (.end stream input)]
                          stream)
                        (if (instance? stream input)
                          input
                          (throw (js/Error. "Invalid input type"))))]
        (.on ws "finish" #(println "tempfile writing done"))
        (.on ws "close" #(try
                           (.renameSync fs temp-file new-file)
                           (catch js/Object err
                             (put! res-ch (ex-info "Could not write binary."
                                                   {:type      :write-binary-error
                                                    :key       key
                                                    :exception err})))
                           (finally
                             (.unpipe input ws)
                             (put! res-ch true)
                             (close! res-ch))))
        (.pipe input ws))
      (catch js/Object err
        (put! res-ch (ex-info "Could not write binary."
                              {:type      :write-binary-error
                               :key       key
                               :exception err}))
        (close! res-ch)))
    res-ch))

(defn read-edn [path key]
  (let [file-name (str path "/data/" (uuid key))
        res-ch    (chan)]
    (if (.existsSync fs file-name)
      (let [rs      (.createReadStream fs file-name)]
        (.on rs "data" (fn [chunk] (put! res-ch  (fress/read (. chunk -buffer)))))
        (.on rs "close" #(close! res-ch))
        (.on rs "error" (fn [err]
                          (put! res-ch (ex-info "Could not read edn."
                                                {:type      :read-edn-error
                                                 :key       key
                                                 :exception err}))
                          (close! res-ch))))
      (do (put! res-ch "File not exist")
          (close! res-ch)))
    res-ch))

(defn read-edn-key [path key]
  (let [file-name (str path "/meta/" key)
        res-ch    (chan)]
    (if (.existsSync fs file-name)
      (let [rs      (.createReadStream fs file-name)]
        (.on rs "data" (fn [chunk] (put! res-ch  (fress/read (. chunk -buffer)))))
        (.on rs "close" #(close! res-ch))
        (.on rs "error" (fn [err]
                          (put! res-ch (ex-info "Could not read edn key."
                                                {:type      :read-edn-key-error
                                                 :key       key
                                                 :exception err}))
                          (close! res-ch))))
      (do (put! res-ch "File not exist")
          (close! res-ch)))
    res-ch))

(defn read-binary
  "return read stream"
  [folder key locked-cb]
  (let [res       (chan)
        file-id   (uuid key)
        file-name (str folder "/data/" file-id)]
    (if (.existsSync fs file-name)
      (let [rs (.createReadStream fs file-name)]
        (.on rs "close" #(close! res))
        (.on rs "error" (fn [err]
                          (put! res (ex-info "Could not read binary."
                                                {:type      :read-binary-error
                                                 :key       key
                                                 :exception err}))
                          (close! res)))
        (go
          (<! (locked-cb {:read-stream rs}))
          (put! res "File not exist")
          (close! res))))
    res))

(defn list-keys [{:keys [folder]}]
  (doseq [id (filter #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" %) (js->clj (.readdirSync fs "/tmp/mystore/meta")))]
    (go (println (<! (read-edn-key folder id))))))

(defn delete-entry [folder file-name res-ch]
  (let [key-file (str folder "/meta/" file-name)
        data-file (str folder "/data/" file-name)]
    (.unlink fs key-file
             (fn [err]
               (if err
                 (if (= (aget err "code") "ENOENT")
                   (do (put! res-ch "File not exist")
                       (close! res-ch))
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
    (go (<! (write-edn-key folder {:key key-vec :format :edn}))
        (<! (write-edn folder key-vec up-fn))))
  (-assoc-in [this key-vec val] (-update-in this key-vec (fn [_] val)))
  (-dissoc [this key] (let [fn (uuid key)
                            res-ch (chan)]
                        (delete-entry folder fn res-ch)
                        res-ch))
  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb] (read-binary folder key locked-cb))
  (-bassoc [this key input] (do (write-edn-key folder {:key key :format :binary})
                                (println "start write binary")
                                (write-binary folder (first key) input))))

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

  (delete-store "/tmp/mystore")

  ;;EDN read/write functionality
  (go (println (<! (-assoc-in store [:new] 43))))

  (go (println (<! (-exists? store :new))))
  (go (println (<! (-update-in store [:new] inc))))
  (go (def x (<! (-get-in store [:new]))))
  (go (println (<! (-dissoc store :new))))
  (doseq [i (range 1 10)]
    (go (<! (-assoc-in store [i] (+ i i)))))
  ;;Binary read/write functionality
  (go (println (<! (-bassoc store [:bin1ary] (js/Buffer.from #js [1 2 3 4])))))

  ;,invalid input for write binary

  (go (println (<! (-bassoc store [:binary] (.createReadStream fs "/tmp/mystore/data/2d081fba-8060-59dd-aa19-7b61d4a2d504")))))


  (go (println (<! (-bassoc store [:binary] "123"))))


  (go (<! (-bget store [:binary] #(println %))))


  (prn "123")

  (ex-info "Write binary input is not a stream or buffer."
           {:type      :write-binary-error
            :key       key
            :exception err})



  (instance? #object[ReadStream] "123")

  (type (.createReadStream fs "/tmp/mystore/data/2d081fba-8060-59dd-aa19-7b61d4a2d504"))

  (type (.createReadStream fs "/tmp/mystore/data/2d081fba-8060-59dd-aa19-7b61d4a2d504"))

  (def inMemory (atom nil))

  (go (.pipe (<! (-bget store :binary #())) (.createWriteStream fs "temp1234")))


  ;; TODO no vector for key
  (go (<! (-bget store [:binary] #(.pipe (:read-stream %)
                                         (.createWriteStream fs "temp1234")))))

  (go (<! (-bget store [:binary] #(-bassoc store [:other-binary] (:read-stream %)))))

  ;;Copy binary

(def mybuf (js/Buffer.from #js [1 2 3 4]))

(def ymstream (stream.PassThrough))

(.end ymstream mybuf)

(type ymstream)

(time (spit "xwerqwer" "qwerqwerqwer"))

(time )

(time (write-binary "/tmp/mystore" "12341234" "1234123412341234"))


(slurp "myfile")









(go
  (let [[err] (<! (io/aspit "data.edn" data))]
            (if-not err
              (println "you've successfully written to 'data.edn'")
              (println "there was an error writing: " err))))

(go
  (let [[err datastring] (<! (io/aslurp "data.edn"))]
    (if-not err
      (handle-data (read-string datastring))
      (handle-error err))))




(type ymstream)

(if (js/Buffer.isBuffer mybuf)
  (println "i a m buffer")
  false)

(def new-array (apply vector (range 1 10)))

new-array

(def myarray )

myarray


(go (println (<! (-bassoc store :testy ))))

(uuid :testy)

(go (.pipe (<! (-bget store :testy)) (.createWriteStream fs "/tmp/1234")))


(list-keys store)

(go (<! (write-binary-new "/tmp/mystore" "123" mybuf)))


(write-binary-new "/tmp/mystore" "123" mybuf)




(def mychan (chan))

(put! mychan (ex-info "Could not write binary."
                      {:type      :write-binary-error
                       :key       "123"
                       :exception "123"}))

(take! mychan #(prn %))


)


