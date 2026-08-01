#!/usr/bin/env python3
"""Read a konserve blob from another language.

This is the payoff of storing values as CBOR: a konserve store written by
Clojure is readable by anything with a CBOR library, with no Clojure and no
JVM involved. What follows is the entire format -- there is nothing else to
know.

    python3 interop/read_konserve_blob.py path/to/blob.ksv

Requires `cbor2` and `zstandard`. Exercised by
`konserve.interop-python-test`, which writes a real store and diffs this
script's output against the Clojure value, so this file cannot drift away
from what konserve actually writes.

Only `:BoringSerializer` (byte 3) blobs are CBOR. Byte 1 is Fressian and byte
2 is clj-cbor's own dialect; both are refused here rather than misread.
"""

import sys

import cbor2
import zstandard

HEADER_SIZE = 20

SERIALIZERS = {0: "StringSerializer", 1: "FressianSerializer",
               2: "CBORSerializer", 3: "BoringSerializer"}
COMPRESSORS = {0: "null", 1: "lz4", 2: "zstd"}
ENCRYPTORS = {0: "null", 1: "aes"}


class Keyword(str):
    """Clojure keyword. A str subclass so it can be a dict key and print
    readably, but a DISTINCT type -- :a and "a" are different map keys in
    Clojure, and collapsing them silently merges entries."""
    __slots__ = ()

    def __repr__(self):
        return ":" + str.__str__(self)


class Symbol(str):
    __slots__ = ()

    def __repr__(self):
        return str.__str__(self)


def _tag_hook(decoder, tag):
    # Tag 39 (IANA "identifier") is how boring writes keywords and symbols: a
    # keyword is the string with a leading colon, a symbol the same without.
    if tag.tag == 39:
        s = tag.value
        return Keyword(s[1:]) if s.startswith(":") else Symbol(s)
    # Tag 27 is "generic object", [type-name, argument]. boring reserves a few
    # slash-bearing names for types CBOR has no tag for; a class name never
    # contains a slash, so there is no ambiguity with a Clojure record.
    if tag.tag == 27:
        name, arg = tag.value
        if name == "clojure/with-meta":
            return arg[1]                      # arg[0] is the metadata map
        if name in ("clojure/queue", "clojure/sorted-set", "clojure/sorted-map",
                    "clojure/char"):
            return arg
        return {"__record__": name, **(arg if isinstance(arg, dict) else {"args": arg})}
    return tag


def parse_header(blob):
    if len(blob) < HEADER_SIZE:
        raise ValueError("short blob: %d bytes, header alone is %d"
                         % (len(blob), HEADER_SIZE))
    version, serializer, compressor, encryptor = blob[0], blob[1], blob[2], blob[3]
    if version != 1:
        raise ValueError("unsupported konserve storage layout version %d" % version)
    # Bytes 4-7: metadata length, big-endian. Bytes 8-19 are spare and zero.
    meta_size = int.from_bytes(blob[4:8], "big")
    return {"version": version, "serializer": serializer,
            "compressor": compressor, "encryptor": encryptor,
            "meta_size": meta_size}


def read_blob(blob):
    """-> (header, metadata, value). The blob is header || meta || value, with
    meta and value each independently compressed."""
    h = parse_header(blob)
    if h["serializer"] != 3:
        raise ValueError(
            "serializer byte %d (%s) is not CBOR -- only :BoringSerializer (3) "
            "is readable outside Clojure" % (h["serializer"],
                                             SERIALIZERS.get(h["serializer"], "?")))
    if h["encryptor"] != 0:
        raise ValueError("blob is encrypted (%s); decrypt before decoding"
                         % ENCRYPTORS.get(h["encryptor"], "?"))

    meta_bytes = blob[HEADER_SIZE:HEADER_SIZE + h["meta_size"]]
    value_bytes = blob[HEADER_SIZE + h["meta_size"]:]

    if h["compressor"] == 2:
        # STREAMING decompression, not `ZstdDecompressor.decompress`.
        #
        # konserve writes through zstd-jni's ZstdOutputStream, which cannot
        # know the total length before it starts, so the frame header carries
        # no content size. The one-shot API needs that size and fails with
        # "could not determine content size in frame header". Every
        # non-Clojure reader hits this; it is a property of the writer, not a
        # broken blob.
        d = zstandard.ZstdDecompressor()
        # The two segments are compressed SEPARATELY, so metadata can be read
        # without touching the value -- which is the point of storing the
        # metadata length in the header.
        meta_bytes = d.decompressobj().decompress(meta_bytes)
        value_bytes = d.decompressobj().decompress(value_bytes)
    elif h["compressor"] == 1:
        raise ValueError("lz4 blobs need an lz4 block decoder; use zstd (byte 2) "
                         "for cross-language stores")
    elif h["compressor"] != 0:
        raise ValueError("unknown compressor byte %d" % h["compressor"])

    return (h,
            cbor2.loads(meta_bytes, tag_hook=_tag_hook),
            cbor2.loads(value_bytes, tag_hook=_tag_hook))


def main(argv):
    if len(argv) != 2:
        print(__doc__)
        return 2
    with open(argv[1], "rb") as f:
        header, meta, value = read_blob(f.read())
    print("header    %s serializer=%s compressor=%s meta=%d bytes"
          % (header["version"], SERIALIZERS.get(header["serializer"]),
             COMPRESSORS.get(header["compressor"]), header["meta_size"]))
    print("metadata  %r" % (meta,))
    print("value     %r" % (value,))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
