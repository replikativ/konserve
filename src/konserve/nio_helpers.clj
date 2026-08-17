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
  (blob->channel [input _buffer-size]
    [(Channels/newChannel input)
     (fn [bis buffer]  (.read ^ReadableByteChannel bis ^ByteBuffer buffer))])

  File
  (blob->channel [input _buffer-size]
    ;; ^File, not ^String: the hint used to say String, so the reflector chose
    ;; `FileInputStream(String)` and every `bassoc` given a File threw
    ;; ClassCastException — a documented input type that never worked.
    [(Channels/newChannel (FileInputStream. ^File input))
     (fn [bis buffer]  (.read ^ReadableByteChannel bis buffer))])

  String
  (blob->channel [input _buffer-size]
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

(def ^:private normalize-buffer-size (* 64 1024))

(defn blob->bytes
  "`input` as a byte array, whatever documented shape it arrived in.

   `konserve.core/bassoc` accepts an InputStream, a File, a String, a Reader or
   a byte array, but only backings that consume a stream can take them as they
   come. Everything else gets them through here, so one dispatch serves every
   backing instead of each reinventing it — or, as was the case, not
   implementing it at all and mishandling four of the five types.

   Materializes, necessarily: a backing that cannot stream has to hold the
   value anyway. A backing that CAN should say so via
   `PStreamingBinaryWrite` and skip this."
  ^bytes [input]
  (if (bytes? input)
    input
    (let [[bis read] (blob->channel input normalize-buffer-size)
          out (java.io.ByteArrayOutputStream.)
          buffer (ByteBuffer/allocate normalize-buffer-size)]
      (try
        (loop []
          (let [size (read bis buffer)]
            (when-not (= size -1)
              (.flip buffer)
              (let [arr (byte-array (.remaining buffer))]
                (.get buffer arr)
                (.write out arr 0 (alength arr)))
              (.clear buffer)
              (recur))))
        (.toByteArray out)
        (finally (.close ^java.io.Closeable bis))))))
