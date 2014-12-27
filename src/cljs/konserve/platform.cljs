(ns konserve.platform
  "Platform specific io operations cljs."
  (:require [konserve.literals :refer [TaggedLiteral]]
            [cljs.reader :refer [read-string]]
            [cljs.core.async :as async :refer (take! <! >! put! close! chan)]
            [weasel.repl :as ws-repl])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))


#_(when-not (= (.indexOf (.- js/document URL) "file://") -1)
  (ws-repl/connect "ws://localhost:9001"))

(defn ^:dynamic log
  "Bindable log fn, defaults to console.log."
  [& s]
  (.log js/console (apply str s)))

(defn error-handler
  "Rebindable error handler, defaults to console.error."
  [topic res e]
  (.error js/console topic e) (close! res) (throw e))


(extend-protocol IPrintWithWriter
  konserve.literals.TaggedLiteral
  (-pr-writer [coll writer opts] (-write writer (str "#" (:tag coll) " " (:value coll)))))

(defn read-string-safe [tag-table s]
  (binding [cljs.reader/*tag-table* (atom (merge tag-table
                                                 (select-keys @cljs.reader/*tag-table*
                                                              #{"inst" "uuid" "queue"})))
            cljs.reader/*default-data-reader-fn*
            (atom (fn [tag val]
                    (konserve.literals.TaggedLiteral. tag val)))]
    (read-string s)))
