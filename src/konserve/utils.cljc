(ns konserve.utils
  (:require #?(:clj [clojure.core.async :refer [go <!]]
               :cljs [cljs.core.async :refer [<! >! alts! chan timeout put!
                                              close! promise-chan take!]
                      :as async])
            #?(:cljs (cljs.core.async.impl.protocols :refer [ReadPort])))
  #?(:cljs (:require-macros [superv.async :refer [wrap-abort! >? <? go-try go-loop-try
                                                  on-abort go-super go-loop-super go-for alts?]]
                            [cljs.core.async.macros :refer [go go-loop alt!]])))

(defn throw-if-exception
  "Helper method that checks if x is Exception and if yes, wraps it in a new
  exception, passing though ex-data if any, and throws it. The wrapping is done
  to maintain a full stack trace when jumping between multiple contexts."
  [x]
  (if (isa? Exception x)
    (throw (ex-info (or (.getMessage ^Exception x) (str x))
                    (or (ex-data x) {})
                     x))
    x))
 
(defmacro <?
  "Same as core.async <! but throws an exzeption if the channel returns a
  throwable error."
  [ch]
  `(throw-if-exception (<! ~ch)))

(defmacro go-try
  "Asynchronously executes the body in a go block. Returns a channel
  which will receive the result of the body when completed or the
  exception if an exception is thrown. You are responsible to take
  this exception and deal with it! This means you need to take the
  result from the cannel at some point or the supervisor will take
  care of the error."
  {:style/indent 1}
  [& body]
  (let [[body finally] (if (= (first (last body)) 'finally)
                         [(butlast body) (rest (last body))]
                         [body])]
    `(let [
           ]
       (go
         (try
           ~@body
           (catch Exception e#
             e#)
           (finally
             ~@finally))))))

(defn reduce<
  "Reduces over a sequence s with a go function go-f given the initial value
  init."
  [go-f init s]
  (go-try
      (loop [res init
             [f & r] s]
        (if f
          (recur (<? (go-f res f)) r)
          res))))


