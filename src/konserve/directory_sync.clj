(ns konserve.directory-sync
  "Platform-specific directory persistence. Failures never imply rollback."
  (:import [java.nio.channels FileChannel]
           [java.nio.file Path Paths OpenOption Files FileSystems LinkOption FileAlreadyExistsException]))

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

(defn provision-directory!
  "Synchronously provision path beneath a caller-trusted durable ancestor.
  Both paths use the default filesystem. The ancestor must already exist and
  its name must already be durable. The caller must exclude concurrent renames,
  deletion and symlink replacement throughout the managed tree. This is not a
  sandbox against hostile filesystem mutation.

  Every parent/name barrier is repeated on retry, including existing entries.
  Failures propagate and may leave directories behind; never infer rollback.
  This opt-in operation does not change ordinary or read-only store connects."
  [ancestor path]
  (let [as-path (fn [p]
                  (cond (instance? Path p) p
                        (and (string? p) (seq p)) (Paths/get p (make-array String 0))
                        :else (throw (ex-info "Provisioning requires a nonempty path"
                                              {:type :konserve/invalid-provisioning-path}))))
        ^Path ancestor (.normalize (.toAbsolutePath ^Path (as-path ancestor)))
        ^Path path (.normalize (.toAbsolutePath ^Path (as-path path)))
        nofollow (into-array LinkOption [LinkOption/NOFOLLOW_LINKS])
        directory! (fn [^Path p]
                     (when-not (Files/isDirectory p nofollow)
                       (throw (ex-info "Provisioning requires real directory components"
                                       {:type :konserve/invalid-provisioning-directory
                                        :path (str p)}))))]
    (when-not (and (= (FileSystems/getDefault) (.getFileSystem ancestor)
                      (.getFileSystem path))
                   (.startsWith path ancestor))
      (throw (ex-info "Provisioning target must be beneath its default-filesystem ancestor"
                      {:type :konserve/invalid-provisioning-path})))
    (directory! ancestor)
    (sync-directory! ancestor)
    (when-not (= ancestor path)
      (reduce (fn [^Path parent ^Path component]
                (let [child (.resolve parent component)]
                  (try
                    (Files/createDirectory child (make-array java.nio.file.attribute.FileAttribute 0))
                    (catch FileAlreadyExistsException _))
                  (directory! child)
                  (sync-directory! parent)
                  (sync-directory! child)
                  child))
              ancestor (iterator-seq (.iterator (.relativize ancestor path)))))
    path))
