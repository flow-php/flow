---
package: flow-php/flow-php-ext
---

# Flow PHP Extension

[PACKAGE_NAV]

[TOC]

Flow stores durable datasets (`to_floe()`) and caches intermediate ones (`DataFrame::cache()`,
sort / join / group-by spill buckets) using the native **Floe** binary format (`.floe`) - the schema
is written once per file and rows carry raw values only, which makes both the payload and the
hydration dramatically cheaper than native PHP `serialize()`/`unserialize()`.

This extension encodes and decodes Floe **frames** natively in Rust via
[ext-php-rs](https://github.com/extphprs/ext-php-rs). The pure-PHP implementation in `Flow\Floe`
(flow-php/etl) is the canonical behavior reference and works without the extension - loading it is
purely an optimization. File header and footer assembly always stay in PHP.

> You never need to call this extension directly. `Flow\Floe\FloeReader`/`FloeWriter` - used by
> `from_floe()`/`to_floe()`, the cache and the sort / join / group-by buckets caches - route whole
> batches to it automatically when `extension_loaded('flow_php')` is true.

## Loading the Extension

### In php.ini

```ini
extension = flow_php
```

### During Development

```bash
php -d extension=./ext/modules/flow_php.so your_script.php
```

## Usage

The extension is used implicitly through the ETL cache:

```php
<?php

use function Flow\ETL\DSL\{df, from_array, to_stream};

df()
    ->read(from_array($bigDataset))
    ->cache('my-dataset') // serialized with the extension when loaded
    ->write(to_stream(__DIR__ . '/output.csv'))
    ->run();
```

The extension registers two native classes; the PHP side (`FloeStreamWriter`/`FloeStreamReader`) owns
file framing, sectioning, partitions, footer and - on read - skip/limit/padding, and picks the native
implementation automatically:

- **`Flow\Floe\RustFloeEncoderNative`** - the Floe ROW frame-body codec:
  `encode(list<TypedRowValues>, schemaBody)` returns the encoded frame bodies,
  `decode(list<string>, schemaBody)` returns `list<Flow\ETL\Row\RawRowValues>`. The userland wrapper
  `Flow\Floe\NativeFloeEncoder` carries the `Flow\ETL\Row\Encoder` interface, and
  `Flow\Floe\AdaptiveFloeEncoder` - built by every writer/reader - selects it over
  `Flow\Floe\PhpFloeEncoder` when the extension is loaded.
- **`Flow\ETL\Row\RustRowHydratorNative`** - the native `hydrate`/`cast`/`dehydrate` behind
  `Flow\ETL\Row\NativeRowHydrator`, which `Flow\ETL\Row\AdaptiveRowHydrator` (the config default)
  selects when the extension is loaded - used by adapter loaders and raw-scalar extractors such as
  CSV, JSON or XML. `cast(batch, Schema)` casts raw scalars and builds entries in a single native
  pass; values outside the proven native subset (and every exotic type such as enum, xml or time)
  cast per value through the schema's PHP `Type::cast`, so results and exceptions match
  `PhpRowHydrator` exactly. Schema-less `cast` (type inference) stays PHP. Hydrate and cast plans are
  cached on the `Schema` object identity and rebuilt only when a different schema arrives.

All extension failures throw `Flow\Floe\Exception\ExtensionException`; `FloeReader`/`FloeWriter` wrap
it as `Flow\Floe\Exception\FloeException`. There is no silent fallback to the PHP engine.

## Structure columns across the PHP/Rust boundary

A structure column crosses to Rust in the schema JSON as tag `structure_v2` with an ordered `fields`
list - one `{name, type, optional}` object per field, `name` always a JSON string (PHP casts integer
element names on `normalize()`). The Rust side reads the list in order; the per-field `optional` flag
is used only by the cast plan (`required = !optional`), never by the encoder or decoder, which emit
and read one flag byte per field in declared order.

A version-skewed pair fails loudly instead of writing wrong bytes:

- new `flow-php/etl` with a pre-`structure_v2` extension:
  `flow_php does not support values of type "structure_v2" in this build`
- old `flow-php/etl` with a current extension:
  `flow_php does not support values of type "structure" in this build`

Either message means the extension and `flow-php/etl` disagree on the structure schema format:
reinstall one to match the other, or set the Floe engine to `FloeEngine::php` while upgrading.
