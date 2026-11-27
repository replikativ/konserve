(ns filestore-cases
  (:require [konserve.filestore-parity :refer [exercise]]))

(defn -main [fixtures out]
  (doseq [[codec fixture] [["boring" "filestore-boring"] ["fressian" "filestore"]]]
    (exercise "read" (str fixtures "/" fixture) codec)
    (let [initial (str out "/jolt-" codec)
          updated (str initial "-updated")]
      (exercise "write" initial codec)
      (exercise "write" updated codec)
      (exercise "write" updated codec "updated")
      (exercise "read" updated codec "updated"))))
