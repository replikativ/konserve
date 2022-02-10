(ns konserve.nio-helpers
  (:import [java.nio.channels Channels ReadableByteChannel]
           [java.io Reader File InputStream
            ByteArrayInputStream FileInputStream StringReader]
           (java.util Arrays)
           (java.nio ByteBuffer)))

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
     (fn [bis buffer]  (.read ^ReadableByteChannel bis ^ByteBuffer buffer))])

  File
  (blob->channel [input buffer-size]
    [(Channels/newChannel (FileInputStream. ^String input))
     (fn [bis buffer]  (.read ^ReadableByteChannel bis buffer))])

  String
  (blob->channel [input buffer-size]
    [(Channels/newChannel (ByteArrayInputStream. (.getBytes input)))
     (fn [bis buffer]  (.read ^ReadableByteChannel bis buffer))])

  Reader
  (blob->channel [input buffer-size]
    [input
     (fn [bis nio-buffer]
       (let [char-array (make-array Character/TYPE buffer-size)
             size (.read ^StringReader bis ^chars char-array)]
         (try
           (when-not (= size -1)
             (let [char-array-copy (Arrays/copyOf ^chars char-array size)]
               (.put ^ByteBuffer nio-buffer (.getBytes (String. char-array-copy)))))
           size
           (catch Exception e
             (throw e)))))]))

(extend
 byte-array-type
  BlobToChannel
  {:blob->channel (fn [input _]
                    [(Channels/newChannel (ByteArrayInputStream. input))
                     (fn [bis buffer] (.read ^ReadableByteChannel bis buffer))])})

(extend
 char-array-type
  BlobToChannel
  {:blob->channel (fn [input _]
                    [(Channels/newChannel (ByteArrayInputStream. (.getBytes (String. ^chars input))))
                     (fn [bis buffer] (.read ^ReadableByteChannel bis buffer))])})

