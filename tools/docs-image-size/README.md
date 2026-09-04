# Patched image-size for documentation builds

This private, MIT-licensed package vendors `image-size` **2.0.2** as
`@membrane/image-size` **2.0.2-patch.0**. Docusaurus calls the preserved
`image-size/fromFile` API to read local image dimensions. Both CommonJS and ESM
entrypoints, format handlers, declarations, and supported formats are retained.
The original npm `dist/` directory is named `lib/` here so Git tracks it normally.

The upstream repository is archived, and the reviewed advisories list no patched
release for the ICNS and JXL/HEIF infinite loops:

- [GHSA-w3rx-r6r6-pgpr](https://github.com/advisories/GHSA-w3rx-r6r6-pgpr)
- [GHSA-5p2g-fcmc-qvqq](https://github.com/advisories/GHSA-5p2g-fcmc-qvqq)

## Provenance and changes

`LICENSE` is copied unchanged from upstream. `UPSTREAM.json` records the official
npm tarball integrity, its SHA-256, original file hashes, and changed runtime files.
Every original file was checked byte for byte against that tarball before editing.
`upstream.patch` contains the complete runtime diff against the original files
with `dist/` renamed to `lib/`. Valid binary test images come from the upstream
`v2.0.2` test suite; their source URLs and SHA-256 hashes are recorded in
`tests/fixtures/UPSTREAM.json`.

The patch changes 20 compiled files because upstream bundles parsers separately
into the root, `fromFile`, detector, lookup, and format entrypoints:

- All 18 box-reader copies require a complete eight-byte header, a size of at
  least eight, and the existing input-span bound before returning a matching box.
  Box iteration therefore always advances, including when the requested type
  matches a malformed zero-length box.
- All 12 ICNS parser copies reject incomplete entry headers, entry lengths below
  eight, and entries crossing the declared file span before appending or advancing.
- HEIF nested scans stay inside their parent boxes; dimension and crop fields
  require sufficient box payload. JXL partial codestream boxes require their
  twelve-byte header. Other parsers and the asynchronous file queue are unchanged.

`fromFile` retains the upstream 512 KiB read limit. ICNS dimensions can still be
read from a complete entry header even when its payload is outside that prefix.
Large JXL files with an early complete codestream box still work. A JXL codestream
box itself extending beyond the prefix remains unsupported, as in upstream 2.0.2.
Zero-size and extended-size boxes were not correctly supported by upstream and
are rejected rather than interpreted as partial headers.

## Validation

From the repository root:

```sh
node --test tools/docs-image-size/tests/parser.test.cjs
```

Malformed zero, undersized, oversized, and truncated entries run in child
processes with hard timeouts so an event-loop regression cannot hang the suite.
Tests exercise CJS/ESM root, lookup, format, and `fromFile` APIs; valid upstream
ICNS, HEIC, AVIF, JXL, and JPEG images; PNG/SVG; and large-file prefix behavior.

To reproduce the original three hangs against an unpacked `image-size@2.0.2`
package, set `IMAGE_SIZE_UPSTREAM` to its directory. This command must fail with
three parser timeouts:

```sh
IMAGE_SIZE_UPSTREAM=/path/to/upstream/package node --test \
  --test-name-pattern='rejects zero' tools/docs-image-size/tests/parser.test.cjs
```

The root dependency/override selects this local package. It is a local maintained
patch, not a claimed upstream release; replace it only after an upstream or
compatible replacement passes these regressions and the documentation build.
