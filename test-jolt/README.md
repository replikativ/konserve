# Running Konserve on Jolt

The memory and local file stores support synchronous and asynchronous operations on Linux/Chez. File operations return channels by default. Pass `{:sync? true}` for a direct result. The public lifecycle accepts an explicit serializer.

```clojure
(require '[konserve.core :as k])
(def config {:backend :file :id (random-uuid) :path "/tmp/example-store"
             :config {:encoding {:serializer :BoringSerializer}}})
(def store (k/create-store config {:sync? true}))
(k/assoc store :example [1 2 3] {:sync? true})
```

The file store stages each write in the store directory, forces the file before atomic replacement, and forces the directory afterward by default. Writers coordinate through the same permanent per-key lock file as the JVM backend. For crash recovery of the directory tree itself, durably provision the store directory before connecting. Binary writes take byte arrays and reads buffer the payload. Use Boring for registered records. Jolt Fressian supports Konserve's vector representation. Legacy file migration, in-place updates and custom Java filesystems are outside this backend.

## Runtime and dependencies

Use [Jolt 0.8.31](https://github.com/jolt-lang/jolt/releases/tag/v0.8.31) on Linux/Chez. The [consumer profile](deps.edn) selects Jolt's core.async and pins the time, Fressian and upstream [crypto 0.4.0](https://github.com/jolt-lang/crypto/tree/v0.4.0) providers. Boring 0.1.32 comes from Konserve's Maven dependencies. To test a packaged jar, replace the Konserve local root with the jar path and keep the exclusions.

## Verification

- From this directory run `jolt -M:test` for the public memory store.
- From the repository root run `interop/check-jolt-filestore.sh` for the public file lifecycle and full JVM/Node/Jolt store-directory parity with Boring and Fressian, including shorter replacements. The gate uses the Clojure CLI and Node.
- Run `interop/check-jolt-contention.sh` for competing JVM/Jolt conditional writes and read-modify-write increments. Both processes reopen the store and check the final value.

Set `JOLT` to the absolute path to the Jolt executable for the shell checks.
