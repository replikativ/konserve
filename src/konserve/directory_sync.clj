(ns konserve.directory-sync
  "Platform-specific directory persistence. Failures never imply rollback."
  (:import [java.nio.channels FileChannel]
           [java.nio.file Path Paths OpenOption]))

(defn windows? []
  (.startsWith (System/getProperty "os.name" "") "Windows"))

(defmacro ^:private windows-binding-call [path]
  ;; Compile a direct call for native-image, without loading a Java 22 class
  ;; when requiring this namespace on an older JVM. No reflective runtime call.
  (if (and (>= (.feature (Runtime/version)) 22)
           (try (Class/forName "konserve.internal.WindowsDirectorySync" false
                               (.getContextClassLoader (Thread/currentThread)))
                true
                (catch ClassNotFoundException _ false)))
    `(konserve.internal.WindowsDirectorySync/flush ~path)
    `(throw (ex-info "Windows directory sync requires JDK 22+ and a Konserve build containing the Windows binding"
                     {:type :konserve/windows-directory-sync-unavailable}))))

(defn windows-flush! [^Path path]
  (windows-binding-call path))

(defn sync-directory!
  "Persist a directory's entries. Uses the native Windows barrier or POSIX force.
  Does not create directories, request privilege elevation or suppress IO errors."
  [path]
  (let [^Path path (if (instance? Path path) path
                       (Paths/get (str path) (make-array String 0)))]
    (if (windows?)
      (windows-flush! path)
      (with-open [channel (FileChannel/open path (make-array OpenOption 0))]
        (.force channel true)))))
