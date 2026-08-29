---
package: flow-php/filesystem-sftp-bridge
---

# Filesystem SFTP

[PACKAGE_NAV]

[TOC]

The Filesystem SFTP Bridge lets Flow treat an SFTP server as a filesystem, so `sftp://` paths work
anywhere a local or cloud path does - reading, writing, listing, moving and removing files, through
[phpseclib](https://phpseclib.com/docs/sftp).

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/filesystem-sftp-bridge.md).

## Opening a connection

The bridge never handles credentials itself, it takes an already authenticated phpseclib client.
`sftp_client()` covers the common cases and returns phpseclib's own `SFTP` object, so you can
configure it further before handing it over.

```php
use function Flow\Filesystem\Bridge\SFTP\DSL\{sftp_client, sftp_filesystem};

// password authentication
$sftp = sftp_client($_ENV['SFTP_HOST'], $_ENV['SFTP_USER'], $_ENV['SFTP_PASSWORD']);

// key based authentication
$sftp = sftp_client(
    $_ENV['SFTP_HOST'],
    $_ENV['SFTP_USER'],
    \phpseclib3\Crypt\PublicKeyLoader::load(\file_get_contents($_ENV['SFTP_PRIVATE_KEY'])),
    port: 2222,
);

$fstab = fstab(sftp_filesystem($sftp));
```

Authentication failures are reported as `Flow\Filesystem\Exception\RuntimeException`; the credential
itself never appears in the message.

The **mount protocol** - the URI scheme under which the filesystem is registered in the
`FilesystemTable` - defaults to `'sftp'`. Override it when you need to mount two servers at once:

```php
$fstab = fstab(
    sftp_filesystem($incoming, protocol: 'sftp-incoming'),
    sftp_filesystem($archive, protocol: 'sftp-archive'),
);
```

## Usage with Flow

Mount the filesystem on the configuration and every `sftp://` path becomes readable and writable
from a DataFrame.

```php
$config = config_builder()
    ->mount(sftp_filesystem(sftp_client($_ENV['SFTP_HOST'], $_ENV['SFTP_USER'], $_ENV['SFTP_PASSWORD'])));

data_frame($config)
    ->read(from_csv(path('sftp:///upload/orders.csv')))
    ->write(to_parquet(path('sftp:///archive/orders.parquet')))
    ->run();
```

Paths are absolute on the remote server and relative to the directory the SSH account is chrooted
into, so `sftp:///upload/orders.csv` is `/upload/orders.csv` as the account sees it.

Directories that do not exist yet are created when writing.

## Reading directories and patterns

`list()` walks the remote tree. A path without wildcards behaves like a prefix and yields everything
below it; a glob is matched against the entries and directories that cannot lead to a match are
never listed, which keeps the number of round trips down on deep trees.

```php
data_frame($config)
    ->read(from_csv(path('sftp:///upload/**/*.csv')))
    ->run();
```

`FileStatus` values returned from `list()` and `status()` carry `size` and `lastModifiedAt` taken
from the directory listing, so `flow:filesystem:ls --long` needs no extra request per file.

## Writing in blocks

Writes are buffered into local blocks and each full block is uploaded on its own, so a dataset never
has to fit in memory. `writeTo()` truncates whatever was under the path before the first block lands,
`appendTo()` continues at the end of an existing file. Block files are removed as soon as the server
has accepted them.

```php
use function Flow\Filesystem\Bridge\SFTP\DSL\{sftp_filesystem, sftp_filesystem_options};

$filesystem = sftp_filesystem(
    $sftp,
    sftp_filesystem_options()
        ->withBlockSize(1024 * 1024 * 16)   // fewer, larger uploads
        ->withReadChunkSize(1024 * 1024 * 4) // bytes pulled per request by readLines()
);
```

| option | default | meaning |
|---|---|---|
| `withBlockSize()` | 4 MB | size of a single block buffered locally before it is uploaded |
| `withReadChunkSize()` | 1 MB | bytes `readLines()` pulls in one request when no length is given |

## Lost connections

This bridge does not reconnect on your behalf. phpseclib answers a dropped session with the same
`false` it uses for "no such file", so every operation checks the session before reporting an empty
result, and raises `Flow\Filesystem\Exception\RuntimeException` naming the operation that failed:

```
SFTP session is no longer usable, cannot read /upload/orders.csv. The connection was closed or lost,
this bridge does not reconnect on your behalf.
```

Long running pipelines against servers with an idle timeout should keep that in mind and build a
fresh client when a run is retried.
