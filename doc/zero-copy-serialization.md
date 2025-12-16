# Zero-Copy Serialization Options for Konserve

This document summarizes research into zero-copy serialization options that could potentially improve deserialization performance in konserve, particularly for the tiered store sync use case.

## Problem Statement

When using `multi-get` to retrieve many keys at once (e.g., during tiered store initialization from IndexedDB), the deserialization loop becomes the bottleneck. Even with efficient bulk I/O via single IndexedDB transactions, the CPU-bound deserialization of each blob limits throughput.

Current deserialization path (defaults.cljc):
1. Read blob from backing store
2. Deserialize via Fressian (JVM) or transit/custom (CLJS)
3. Reconstruct Clojure data structures in memory

The deserialization step requires parsing and allocating every nested data structure, which doesn't parallelize well in JavaScript's single-threaded environment.

## Zero-Copy Serialization Overview

Zero-copy serialization formats allow direct access to serialized data without a deserialization step. The data is laid out in memory in a way that allows pointer arithmetic to access fields directly from the serialized bytes.

**Benefits:**
- Near-instant "deserialization" (just pointer setup)
- Reduced memory allocation pressure
- Can access individual fields without parsing entire structure

**Drawbacks:**
- Schema required at compile time
- Write performance often slower (must lay out data carefully)
- Less flexible than schema-less formats
- May require constraining what data types can be stored

## Options Evaluated

### 1. FlatBuffers

**Status:** Mature, cross-platform, good option for CLJ/CLJS

- Created by Google, widely used in games and mobile apps
- Schema compiler generates code for many languages including Java
- JavaScript support via official library
- Random access to nested data without parsing
- Forward/backward compatible schema evolution

**Compatibility with Konserve:**
- Works with konserve's byte-array based serializer interface
- Would require predefined schemas for stored data types
- Could work alongside existing Fressian serialization (schema-based types use FlatBuffers, dynamic types fall back to Fressian)

**For Datahike:**
- Index structures (PersistentSortedSet, Leaf, Branch) have fixed shapes - good fit
- Datom values are polymorphic but constrained by `:db/valueType`
- Could define FlatBuffer schemas covering: `:db.type/string`, `:db.type/long`, `:db.type/double`, `:db.type/boolean`, `:db.type/instant`, `:db.type/uuid`, `:db.type/ref`, `:db.type/keyword`
- Edge cases (arbitrary EDN in datom values) would need fallback or prohibition

**Implementation Effort:** Medium
- Need schema definitions for core types
- Custom serializer implementation
- Testing across platforms

### 2. Cap'n Proto

**Status:** Mature but less ecosystem support than FlatBuffers

- Designed by Kenton Varda (Protocol Buffers v2 author)
- True zero-copy with mmap support
- Java library available (capnproto-java)
- JavaScript support less mature

**Compatibility:**
- Similar to FlatBuffers
- Slightly more complex but faster in some benchmarks
- Less community adoption means fewer examples/support

**Implementation Effort:** Medium-High

### 3. rkyv (Rust)

**Status:** Fastest zero-copy option, but Rust-only

- Zero-copy deserialization with excellent performance
- Archive format that can be accessed directly as Rust types
- Benchmarks show 2-10x faster than other options

**Compatibility:**
- Not available for JVM or JavaScript
- Would require FFI/native integration
- Not practical for konserve's CLJ/CLJS targets

**Implementation Effort:** High (if possible at all)

### 4. Lite³

**Status:** Interesting research, C-only

- B-tree based zero-copy format
- Designed for database-like access patterns
- Very efficient for ordered key-value storage

**Compatibility:**
- C library only
- Would require native bindings for both JVM and JS (via WASM)
- Matches konserve's use case well conceptually

**Implementation Effort:** Very High

### 5. Protocol Buffers

**Status:** Mature but NOT zero-copy

- Included for comparison
- Requires full deserialization
- Would not solve the performance problem

## Comparison Matrix

| Feature | FlatBuffers | Cap'n Proto | rkyv | Lite³ |
|---------|-------------|-------------|------|-------|
| JVM Support | Yes (official) | Yes (community) | No | Via JNI |
| JS Support | Yes (official) | Limited | No | Via WASM |
| Zero-copy read | Yes | Yes | Yes | Yes |
| Schema required | Yes | Yes | Yes | No |
| Write performance | Good | Good | Excellent | Good |
| Read performance | Excellent | Excellent | Best | Excellent |
| Ecosystem/Maturity | High | Medium | Medium | Low |
| Implementation effort | Medium | Medium-High | N/A | Very High |

## Recommendation

### Short Term
FlatBuffers is the most practical option for CLJ/CLJS:
- Official support for both platforms
- Mature tooling and documentation
- Schema evolution support
- Large community

### Implementation Strategy for Datahike/Konserve

1. **Define schemas for index structures:**
   ```flatbuffers
   table Datom {
     e: long;
     a: int;  // attribute id
     v: DatomValue;  // union type
     tx: long;
     added: bool;
   }

   union DatomValue {
     StringValue,
     LongValue,
     DoubleValue,
     // ... other db types
   }

   table Leaf {
     datoms: [Datom];
   }

   table Branch {
     keys: [Datom];
     children: [ulong];  // store-keys as references
   }
   ```

2. **Create hybrid serializer:**
   - FlatBuffers for schema-conforming types (index nodes, datoms with standard value types)
   - Fressian/transit fallback for arbitrary EDN (if needed)
   - Type tag byte prefix to distinguish formats

3. **Integrate with konserve:**
   - New serializer implementation satisfying `PStoreSerializer`
   - Transparent to existing API
   - Optional - existing stores continue working with Fressian

### Constraints for Datahike

To fully benefit from zero-copy:
- Datom values must be constrained to `:db/valueType` set
- No arbitrary EDN in attribute values
- This is likely already true for performance-sensitive use cases

## Alternative Approaches

If zero-copy is too complex, other optimizations:

1. **Parallel deserialization (JVM only):**
   - Use `pmap` or thread pool for concurrent deserialization
   - Each blob can deserialize independently
   - ~3-4x improvement on multi-core systems

2. **Lazy deserialization:**
   - Deserialize on first access rather than at load time
   - Good if not all loaded data is immediately needed

3. **Better Fressian tuning:**
   - Custom handlers optimized for common types
   - Pre-allocated buffers
   - Marginal improvements (~10-20%)

4. **WebAssembly for CLJS:**
   - Compile fast deserializer to WASM
   - Still requires crossing JS/WASM boundary

## Conclusion

Zero-copy serialization via FlatBuffers is feasible for konserve/datahike but requires:
- Schema definitions for core data types
- Constraining datom values to known types
- Significant implementation effort

The payoff would be near-instant "deserialization" for index loads, potentially improving tiered store sync by 10-100x for the deserialization portion. However, given the implementation complexity, this should only be pursued if profiling confirms deserialization is a critical bottleneck in production use cases.

For now, the `multi-get` implementation provides significant I/O-level improvements. Zero-copy can be revisited when/if deserialization becomes the dominant bottleneck at scale.

---
*Document created: December 2024*
*Context: multi-get implementation for tiered store sync optimization*
