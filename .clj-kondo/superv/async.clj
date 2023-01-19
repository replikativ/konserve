(ns superv.async)

(defmacro go-try- [_op & args] 
    `(try 
       ~@args
       (finally)))
