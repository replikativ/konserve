# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands
- Run tests: `clojure -M:test`
- Run specific test: `clojure -M:test -i <test-file-path>`
- Format check: `clojure -M:format`
- Format fix: `clojure -M:ffix`
- Lint code: `clojure -M:lint`
- Build jar: `clojure -T:build jar`
- Install locally: `clojure -T:build install`
- Run ClojureScript: `npx shadow-cljs <command>`
- Run benchmarks: `clojure -M:benchmark`
- Browser tests: `npx shadow-cljs watch browser-tests` then http://localhost:8021/
- Node tests: `npx shadow-cljs release node-tests && node out/node-tests.js`

## Code Style Guidelines
- Use `.cljc` files for cross-platform code (Clojure/ClojureScript)
- Follow async/sync programming models with `core.async` and proper error handling
- Use reader conditionals `#?(:clj ... :cljs ...)` for platform-specific code
- Functions should support both sync (`{:sync? true}`) and async options
- Prefer protocol-based implementations for extensibility
- Use meaningful docstrings for public functions
- Follow Clojure naming conventions (kebab-case, predicate functions end with `?`)
- Handle errors with `go-try` and `<?` macros for async operations
- Format code with cljfmt (automated via format alias)

## Architecture

### Protocol Layers
konserve uses a 3-layer protocol architecture:
1. **API Layer** (`konserve.core`): Public functions like `get-in`, `assoc-in`, `multi-get`
2. **Protocol Layer** (`konserve.protocols`): Store-level protocols (`PEDNKeyValueStore`, `PMultiKeyEDNValueStore`)
3. **Backing Layer** (`konserve.impl.storage_layout`): Low-level blob operations (`PBackingStore`, `PMultiReadBackingStore`, `PMultiWriteBackingStore`)

### Multi-Key Operations
Multi-key operations (`multi-get`, `multi-assoc`, `multi-dissoc`) support atomic bulk operations:
- Check capability with `multi-key-capable?`
- Returns sparse maps (only found keys for `multi-get`)
- Implemented in: memory store, IndexedDB, tiered store
- Backing stores must implement `PMultiReadBackingStore` / `PMultiWriteBackingStore`

### Cross-Platform Code Pattern
Use the `async+sync` macro for code that should work both sync and async:
```clojure
(async+sync (:sync? opts)
            *default-sync-translation*
            (go-try-
             ;; async code using <?- for channel operations
             (<?- (some-async-op))))
```

### Key Files
- `src/konserve/core.cljc` - Public API
- `src/konserve/protocols.cljc` - Protocol definitions
- `src/konserve/impl/defaults.cljc` - DefaultStore implementation
- `src/konserve/impl/storage_layout.cljc` - Backing store protocols
- `src/konserve/tiered.cljc` - Tiered store (frontend cache + backend persistence)
- `src/konserve/indexeddb.cljs` - Browser IndexedDB backend
- `src/konserve/memory.cljc` - In-memory store