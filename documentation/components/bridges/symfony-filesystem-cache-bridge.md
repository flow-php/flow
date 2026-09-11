---
package: flow-php/symfony-filesystem-cache-bridge
---

# Symfony Filesystem Cache Bridge

A Symfony Cache adapter backed by Flow PHP's native `Filesystem` library. Cache items are stored as files on top of any filesystem the library can mount - local disk, in-memory, AWS S3, Azure Blob - without depending on Symfony's `FilesystemAdapter` and the local-only assumptions baked into it.

[PACKAGE_NAV]

[TOC]

## Installation

```bash
composer require flow-php/symfony-filesystem-cache-bridge:~--FLOW_PHP_VERSION--
```

For Symfony framework integration (config-driven pool registration) use this bridge through the [Symfony Filesystem Bundle](/documentation/components/bridges/symfony-filesystem-bundle.md). The page below covers standalone usage.

## How It Works

`FlowFilesystemCacheAdapter` extends Symfony's `AbstractAdapter` and implements `PruneableInterface`. Items are stored one-per-file under a base directory you provide:

```
<base>/
  ab/
    qZ5K-...   ← single cache item
  c4/
    Mn7p-...
```

- The 2-character shard directory is `substr(str_replace('/', '-', base64_encode(hash('xxh128', $id, true))), 0, 2)` - the same recipe as Symfony's built-in `FilesystemAdapter`, so the on-disk layout is familiar.
- Each file body has three newline-separated parts: a 10-digit zero-padded expiry timestamp (`0000000000` for "no expiry"), the cache id, and the marshalled value.
- **Saves** write to a randomised temp path (`Path::randomize()`), then `mv()` to the final path. If `mv()` returns `false` the temp file is removed and the failure is reported to the configured logger as a `FilesystemCacheException`.
- **Reads** stream the file once, split on `"\n"`, drop the row when the expiry has passed and call `doDelete()` for the expired ids in the same call.
- **`prune()`** walks the base directory recursively, reads only the first line of every file, and deletes any file whose expiry is in the past.
- **Marshalling** uses Symfony's `DefaultMarshaller` by default; pass a custom `MarshallerInterface` to the constructor to override.

## Filesystem Ownership

The adapter does NOT create or own a `Filesystem`. You hand it any `Flow\Filesystem\Filesystem` instance and a `Path` for the base directory:

- **Local disk** - pass `new NativeLocalFilesystem()` and a `Path::from('/var/cache/app')`.
- **Remote object store** - pass an S3 or Azure-backed filesystem; the adapter writes through it transparently.
- **Same instance, multiple pools** - give two adapters the same `Filesystem` but different `Path` directories (or different `namespace` strings) to keep their files isolated.

The `Path` value is the only state the adapter holds about location - every `getItem`/`save`/`clear` call composes the per-item path from it, so changing the directory means re-instantiating the adapter.

## Usage

```php
use Flow\Bridge\Symfony\FilesystemCache\FlowFilesystemCacheAdapter;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

$cache = new FlowFilesystemCacheAdapter(
    filesystem: new NativeLocalFilesystem(),
    directory: Path::from('/var/cache/app'),
    namespace: 'app',
    defaultLifetime: 3600,
);

$item = $cache->getItem('greeting');

if (!$item->isHit()) {
    $item->set('hello world');
    $item->expiresAfter(600);
    $cache->save($item);
}

echo $cache->getItem('greeting')->get();
```

The same code works against `flow-php/filesystem-async-aws-bridge` or `flow-php/filesystem-azure-bridge` - swap the `Filesystem` argument and the cache lives in S3 or Azure Blob.

## Constructor

```php ignore
new FlowFilesystemCacheAdapter(
    Flow\Filesystem\Filesystem $filesystem,
    Flow\Filesystem\Path $directory,
    string $namespace = '',
    int $defaultLifetime = 0,
    ?Symfony\Component\Cache\Marshaller\MarshallerInterface $marshaller = null,
);
```

`$namespace` is validated against `[-+.A-Za-z0-9]` - same rule Symfony enforces. `$directory` must be a non-pattern path; the adapter creates the shard directories on demand via the underlying filesystem's `writeTo()`.

## File Layout

| Section | Format | Notes |
|---------|--------|-------|
| Line 1  | `printf('%010d', $expiry)` | Zero-padded UNIX timestamp; `0000000000` means "never expires" |
| Line 2  | raw cache id | Used by `clear($prefix)` to match items by namespace prefix |
| Line 3+ | marshaller output | Whatever `MarshallerInterface::marshall()` produced (binary safe) |

The shard prefix keeps any single directory bounded - even with millions of keys, no shard dir holds more than `~items / 256 / 256` entries on average, and the tree never grows wider than 256 sub-directories.

## Pruning

The filesystem has no built-in TTL, so expired files are removed by either of two paths:

- **Lazy** - a read of the same key triggers a delete of that one file.
- **Explicit** - calling `$cache->prune()` (or `bin/console cache:pool:prune` when the adapter is wired into a Symfony app) walks the base directory and deletes everything past its expiry.

Without periodic pruning, dead files accumulate even though they are invisible to readers.
