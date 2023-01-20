(ns superv.async)

#_(defmacro go-try- [_op & args] 
    `(try 
       ~@args
       (finally)))

(defmacro go-try- [_op & args]
  `(go 
     (try
       ~@args
       (finally))))
