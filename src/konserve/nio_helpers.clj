(ns konserve.nio-helpers
  (:import [java.nio.channels Channels ReadableByteChannel]
           [java.nio CharBuffer]
           [java.nio.charset StandardCharsets CodingErrorAction]
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
    ;; A STATEFUL encoder, not `String.getBytes` per chunk. Chunking a Reader
    ;; by chars splits UTF-16 surrogate pairs at chunk boundaries, and each
    ;; half then encoded as a replacement — an emoji on a boundary came out
    ;; as `??`. The encoder keeps an unpaired high surrogate (`compact`) until
    ;; its partner arrives in the next chunk. REPLACE matches what
    ;; `String.getBytes` did for input that is genuinely malformed.
    ;;
    ;; At most a QUARTER of the buffer in chars: UTF-8 spends up to four
    ;; bytes on a pair (two chars) and three on any other char, so the
    ;; encoded chunk always fits the `buffer-size`-byte ByteBuffer.
    (when (< buffer-size 4)
      (throw (ex-info "Reader input needs a buffer-size of at least 4: one UTF-8 character can take four bytes."
                      {:type :konserve/buffer-too-small :buffer-size buffer-size})))
    (let [encoder (doto (.newEncoder StandardCharsets/UTF_8)
                    (.onMalformedInput CodingErrorAction/REPLACE)
                    (.onUnmappableCharacter CodingErrorAction/REPLACE))
          ;; Never fewer than TWO chars: a held-back high surrogate must leave
          ;; room for its partner, or the buffer is full of one unencodable
          ;; char, every read returns 0, and the writer loops forever. Two
          ;; chars encode to at most 6 bytes, which the byte buffer may not
          ;; hold at the smallest sizes — the encoder then reports OVERFLOW,
          ;; keeps the rest, and the next call makes progress.
          chars   (CharBuffer/allocate (max 2 (quot buffer-size 4)))]
      [input
       (fn [^Reader bis ^ByteBuffer nio-buffer]
         (let [n   (.read bis chars)
               eof (neg? n)]
           (.flip chars)
           (.encode encoder chars nio-buffer eof)
           (.compact chars)
           (when eof (.flush encoder nio-buffer))
           ;; -1 only once nothing is left to hand over: at EOF a held-back
           ;; char can still produce bytes, and the caller stops on -1.
           (if (and eof (zero? (.position nio-buffer))) -1 (.position nio-buffer))))])))

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
