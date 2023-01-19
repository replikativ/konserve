(ns hooks.superv.async)

(defmacro go-try- [_op & args] 
    `(go 
       (try 
       ~@args
       (finally))))
