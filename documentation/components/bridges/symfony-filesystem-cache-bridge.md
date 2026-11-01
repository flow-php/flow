---
package: flow-php/symfony-filesystem-cache-bridge
---

# Symfony Filesystem Cache Bridge

A Symfony Cache adapter on the native `Filesystem` library of Flow PHP. Cache items are stored as files on each filesystem that the library can mount: local disk, in-memory, AWS S3, Azure Blob. It does not depend on the `FilesystemAdapter` of Symfony, which works only with local paths.

[PACKAGE_NAV]

[TOC]

## Installation

```bash
composer require flow-php/symfony-filesystem-cache-bridge:~--FLOW_PHP_VERSION--
```

For the Symfony framework integration (pool registration from configuration), use this bridge through the [Symfony Filesystem Bundle](/documentation/components/bridges/symfony-filesystem-bundle.md). This page covers the standalone usage.

## How It Works

`FlowFilesystemCacheAdapter` extends the `AbstractAdapter` of Symfony and implements `PruneableInterface`. Items are stored one item per file, under a base directory that you give:

```
<base>/
  ab/
    qZ5K-...   ← single cache item
  c4/
    Mn7p-...
```

- The 2-character shard directory is `substr(str_replace('/', '-', base64_encode(hash('xxh128', $id, true))), 0, 2)`. This is the same recipe as in the built-in `FilesystemAdapter` of Symfony, so the on-disk layout is familiar.
- Each file body has three parts, separated by newlines: a 10-digit zero-padded expiry timestamp (`0000000000` for "no expiry"), the cache id, and the marshalled value.
- A save writes to a random temp path (`Path::randomize()`), and then does `mv()` to the final path. If `mv()` returns `false`, the temp file is removed, and the configured logger gets the failure as a `FilesystemCacheException`.
- A read streams the file once and splits it on `"\n"`. It drops an expired row, and calls `doDelete()` for the expired ids in the same call.
- `prune()` goes through the base directory recursively. It reads only the first line of each file, and deletes each expired file.
- By default, marshalling uses the `DefaultMarshaller` of Symfony. To override it, give a custom `MarshallerInterface` to the constructor.

## Filesystem Ownership

The adapter does not create or own a `Filesystem`. You give it a `Flow\Filesystem\Filesystem` instance and a `Path` for the base directory:

- Local disk: give `new NativeLocalFilesystem()` and a `Path::from('/var/cache/app')`.
- Remote object store: give an S3 or Azure-backed filesystem. The adapter writes through it transparently.
- Same instance, multiple pools: give two adapters the same `Filesystem`, but different `Path` directories (or different `namespace` strings). Their files then stay separate.

The `Path` value is the only location state of the adapter. Each `getItem`/`save`/`clear` call builds the item path from it. To change the directory, create a new adapter.

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

The same code works with `flow-php/filesystem-async-aws-bridge` or `flow-php/filesystem-azure-bridge`. Change the `Filesystem` argument, and the cache is in S3 or Azure Blob.

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

`$namespace` must match `[-+.A-Za-z0-9]`, the same rule as in Symfony. `$directory` must be a path without a pattern. The adapter creates each missing shard directory through `writeTo()` of the filesystem.

## File Layout

| Section | Format                     | Notes                                                          |
|---------|----------------------------|----------------------------------------------------------------|
| Line 1  | `printf('%010d', $expiry)` | Zero-padded UNIX timestamp. `0000000000` means "never expires" |
| Line 2  | raw cache id               | `clear($prefix)` uses it to match items by namespace prefix    |
| Line 3+ | marshaller output          | The output of `MarshallerInterface::marshall()` (binary safe)  |

The shard prefix keeps the size of each directory limited. Also with millions of keys, a shard directory holds no more than `~items / 256 / 256` entries on average. The tree never has more than 256 sub-directories.

## Pruning

The filesystem has no built-in TTL, so one of two paths removes expired files:

- Lazy: a read of the same key deletes that one file.
- Explicit: `$cache->prune()` goes through the base directory and deletes all expired files. In a Symfony app with the adapter, `bin/console cache:pool:prune` does the same.

Without periodic pruning, dead files collect, also though readers do not see them.
