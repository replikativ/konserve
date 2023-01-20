(ns hooks.go-try-
  (:require [clj-kondo.hooks-api :as api]))

(defn go-try- [{:keys [:node]}]
  (let [new-node (api/list-node
                  (apply list 
                   (concat 
                    [(api/token-node 'try)]
                    (rest (:children node))
                    [(api/list-node (list (api/token-node 'finally)))])))]
    {:node new-node}))
