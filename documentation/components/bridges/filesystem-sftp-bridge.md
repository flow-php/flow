---
package: flow-php/filesystem-sftp-bridge
---

# Filesystem SFTP

[PACKAGE_NAV]

[TOC]

The Filesystem SFTP Bridge lets Flow use an SFTP server as a filesystem. It uses
[phpseclib](https://phpseclib.com/docs/sftp) for this. `sftp://` paths then work in each place where a local or
cloud path works: read, write, list, move and remove files.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/filesystem-sftp-bridge.md).

## Opening a connection

The plain form takes the host, the user and one credential, and logs in at the first use:

```php
use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_filesystem;

// password authentication
$sftp = sftp_filesystem(['host' => $_ENV['SFTP_HOST'], 'user' => $_ENV['SFTP_USER'], 'password' => $_ENV['SFTP_PASSWORD']]);

// key based authentication, port 2222
$sftp = sftp_filesystem([
    'host' => $_ENV['SFTP_HOST'],
    'user' => $_ENV['SFTP_USER'],
    'private_key' => \file_get_contents($_ENV['SFTP_PRIVATE_KEY']),
    'passphrase' => $_ENV['SFTP_PASSPHRASE'] ?? null,
    'port' => 2222,
]);
```

A filesystem built from an already authenticated phpseclib `SFTP` client stays in this process and reads serially.
`sftp_client()` covers the common cases and returns the `SFTP` object of phpseclib. You can therefore configure it
more before you give it to the filesystem.

```php
use function Flow\Filesystem\Bridge\SFTP\DSL\{sftp_client, sftp_filesystem};

// password authentication
$sftp = sftp_client($_ENV['SFTP_HOST'], $_ENV['SFTP_USER'], $_ENV['SFTP_PASSWORD']);

// key based authentication
$sftp = sftp_client(
    $_ENV['SFTP_HOST'],
    $_ENV['SFTP_USER'],
    \phpseclib4\Crypt\PublicKeyLoader::load(\file_get_contents($_ENV['SFTP_PRIVATE_KEY'])),
    port: 2222,
);

$fstab = fstab(sftp_filesystem($sftp));
```

Authentication failures throw `Flow\Filesystem\Exception\RuntimeException`. The message never contains the
credential.

The mount protocol is the URI scheme of the filesystem in the `FilesystemTable`. The default is `'sftp'`. To mount
two servers at the same time, override it:

```php
$fstab = fstab(
    sftp_filesystem($incoming, protocol: 'sftp-incoming'),
    sftp_filesystem($archive, protocol: 'sftp-archive'),
);
```

## Usage with Flow

Give the filesystem to the source or sink that reads or writes the `sftp://` path. Keep one instance,
and give the same instance to both sides.

```php
$sftp = sftp_filesystem(sftp_client($_ENV['SFTP_HOST'], $_ENV['SFTP_USER'], $_ENV['SFTP_PASSWORD']));

data_frame()
    ->read(from_csv(path('sftp:///upload/orders.csv'), filesystem: $sftp))
    ->write(to_parquet(path('sftp:///archive/orders.parquet'), filesystem: $sftp))
    ->run();
```

If the filesystem of a source or sink does not serve its path, the construction throws. The message names the
`filesystem:` argument.

Paths are absolute on the remote server, and relative to the chroot directory of the SSH account.
`sftp:///upload/orders.csv` is therefore `/upload/orders.csv` for the account.

A write creates the missing directories.

## Reading directories and patterns

`list()` goes through the remote tree. A path without wildcards works like a prefix and yields everything
below it. A glob is matched against the entries. Directories that cannot contain a match are
never listed. This keeps the number of round trips low on deep trees.

```php
data_frame()
    ->read(from_csv(path('sftp:///upload/**/*.csv'), filesystem: $sftp))
    ->run();
```

The `FileStatus` values from `list()` and `status()` carry `size` and `lastModifiedAt` from the directory
listing. `flow:filesystem:ls` therefore needs no extra request for each file.

## Writing in blocks

Writes go into local blocks, and each full block is uploaded separately. A dataset therefore never
has to fit in memory. `writeTo()` truncates the existing content of the path before the first block arrives.
`appendTo()` continues at the end of an existing file. Block files are removed immediately after the server
accepts them.

```php
use function Flow\Filesystem\Bridge\SFTP\DSL\{sftp_filesystem, sftp_filesystem_options};

$filesystem = sftp_filesystem(
    $sftp,
    sftp_filesystem_options()
        ->withBlockSize(1024 * 1024 * 16)   // fewer, larger uploads
        ->withReadChunkSize(1024 * 1024 * 4) // bytes pulled per request by readLines()
);
```

| option                | default | meaning                                                       |
|-----------------------|---------|---------------------------------------------------------------|
| `withBlockSize()`     | 4 MB    | size of a single block buffered locally before it is uploaded |
| `withReadChunkSize()` | 1 MB    | bytes that `readLines()` gets in one request without a length |

`create()` is not supported and throws `RuntimeException`.

## Lost connections

This bridge does not reconnect for you. phpseclib throws `InvalidStateException` for a lost session. It
throws `FileSystemException` for a missing or unreadable path. The bridge turns the first one into
`Flow\Filesystem\Exception\RuntimeException`, which names the failed operation. It keeps the original exception as
`previous`:

```
SFTP session is no longer usable, cannot read /upload/orders.csv
```

Long-running pipelines against servers with an idle timeout must know this. For a retry of a run, build a new
client.
