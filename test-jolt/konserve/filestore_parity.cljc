(ns konserve.filestore-parity
  (:require [konserve.protocols :as p]
            #?(:clj [konserve.filestore :as fs] :cljs [konserve.node-filestore :as fs])))

(def opts {:sync? true})

(defn metadata-for [key type revision]
  ;; Writers ordinarily choose a timestamp and fresh revision per operation.
  ;; Feed both writers the same complete metadata and keep their serializers real.
  (array-map :key key :type type
             :last-write #inst "2020-01-02T03:04:05.006Z"
             :revision revision))

(defn exercise [mode dir & [codec generation]]
  (let [store (fs/connect-fs-store dir :opts opts
                                   :config {:encoding {:serializer (if (= codec "boring")
                                                                     :BoringSerializer :FressianSerializer)}})
        changed? (= generation "updated")
        revision (if changed? "fixture-2" "fixture-1")
        binary (if changed? [255 0] [0 1 127 128 255])
        edn-key [:fixture :edn]
        bin-key [:fixture :binary]
        value (array-map :name (if changed? "updated" "café") :items [nil true -42 256])]
    (case mode
      "write"
      (do
        (p/-assoc-in store [edn-key] (constantly (metadata-for edn-key :edn revision)) value opts)
        (p/-bassoc store bin-key (constantly (metadata-for bin-key :binary revision))
                   #?(:clj (byte-array (map unchecked-byte binary))
                      :cljs (js/Buffer.from (clj->js binary))) opts))
      "read"
      (do
        (assert (= value (p/-get-in store [edn-key] nil opts)))
        (doseq [[k t] [[edn-key :edn] [bin-key :binary]]]
          (assert (= (metadata-for k t revision) (p/-get-meta store k opts))))
        (assert (= binary
                   (p/-bget store bin-key
                            #?(:clj (fn [{:keys [input-stream]}]
                                      (with-open [stream ^java.io.InputStream input-stream]
                                        (mapv #(bit-and 255 %) (.readAllBytes stream))))
                               :cljs (fn [{:keys [blob]}] (vec (array-seq blob))))
                            opts)))))))

(defn -main [mode dir & args]
  (if (= mode "cross-check")
    (doseq [codec ["boring" "fressian"]
            generation [nil "updated"]]
      (let [suffix (str codec (when generation "-updated"))]
        (exercise "write" (str dir "/jvm-" suffix) codec)
        (when generation (exercise "write" (str dir "/jvm-" suffix) codec generation))
        (exercise "read" (str dir "/jolt-" suffix) codec generation)))
    (apply exercise mode dir args))
  #?(:cljs (js/process.exit 0)))
