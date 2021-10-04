(ns konserve.nio-helpers
  (:import [java.nio.channels Channels]
           [java.io Reader File InputStream
            ByteArrayInputStream FileInputStream]))

(def
  ^{:doc "Type object for a Java primitive byte array."
    :private true}
  byte-array-type (class (make-array Byte/TYPE 0)))

(def
  ^{:doc "Type object for a Java primitive char array."
    :private true}
  char-array-type (class (make-array Character/TYPE 0)))

(defprotocol BlobToChannel
  (blob->channel [input buffer-size]))

(extend-protocol BlobToChannel
  InputStream
  (blob->channel [input buffer-size]
    [(Channels/newChannel input)
     (fn [bis buffer]  (.read bis buffer))])

  File
  (blob->channel [input buffer-size]
    [(Channels/newChannel (FileInputStream. input))
     (fn [bis buffer]  (.read bis buffer))])

  String
  (blob->channel [input buffer-size]
    [(Channels/newChannel (ByteArrayInputStream. (.getBytes input)))
     (fn [bis buffer]  (.read bis buffer))])

  Reader
  (blob->channel [input buffer-size]
    [input
     (fn [bis nio-buffer]
       (let [char-array (make-array Character/TYPE buffer-size)
             size (.read bis char-array)]
         (try
           (when-not (= size -1)
             (let [char-array (java.util.Arrays/copyOf char-array size)]
               (.put nio-buffer (.getBytes (String. char-array)))))
           size
           (catch Exception e
             (throw e)))))]))

(extend
 byte-array-type
  BlobToChannel
  {:blob->channel (fn [input _]
                    [(Channels/newChannel (ByteArrayInputStream. input))
                     (fn [bis buffer] (.read bis buffer))])})

(extend
 char-array-type
  BlobToChannel
  {:blob->channel (fn [input _]
                    [(Channels/newChannel (ByteArrayInputStream. (.getBytes (String. input))))
                     (fn [bis buffer] (.read bis buffer))])})

