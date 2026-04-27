# Symfony Filesystem Bundle

Symfony bundle integrating Flow PHP's Filesystem library with Symfony applications, providing a typed
`FilesystemTable` service that mounts multiple filesystems under URI protocols, pluggable factories,
console commands for managing files across mounted protocols, and first-class OpenTelemetry integration.

- [Back](/documentation/introduction.md)
- [Packagist](https://packagist.org/packages/flow-php/symfony-filesystem-bundle)
- [Installation](/documentation/installation/packages/symfony-filesystem-bundle.md)
- [GitHub](https://github.com/flow-php/symfony-filesystem-bundle)

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/symfony-filesystem-bundle.md).

Register the bundle in `config/bundles.php`:

```php
return [
    // ...
    Flow\Bridge\Symfony\FilesystemBundle\FlowFilesystemBundle::class => ['all' => true],
];
```

## Overview

This bundle integrates Flow PHP's Filesystem library with Symfony applications. It provides:

- **Mount-protocol routing** — mount filesystems under URI protocols of your choice (`file`, `memory`, `aws-s3`, `warehouse`, `archive`, …) and resolve them at runtime via `$table->for(...)`
- **Pluggable filesystem factories** — register custom backends with the `#[AsFilesystemFactory]` attribute or a DI tag
- **Built-in factories** — `file`, `memory`, `stdout`, `aws_s3`, and `azure_blob` ship out of the box
- **Console commands** — `flow:filesystem:*` (alias `flow:fs:*`) for `ls`, `cat`, `cp`, `mv`, `rm`, `stat`, `touch` against any configured filesystem
- **Symfony Cache pools** — register PSR-6 cache pools backed by any mounted filesystem (local disk, S3, Azure Blob …) when [flow-php/symfony-filesystem-cache-bridge](/documentation/components/bridges/symfony-filesystem-cache-bridge.md) is installed
- **Telemetry integration** — wrap every filesystem in `TraceableFilesystem` via OpenTelemetry
- **Multi-fstab support** *(advanced)* — configure several independent `FilesystemTable` services when you really need them

## Quick Start

A minimal configuration mounts one or more filesystems inside a single fstab. The fstab name is arbitrary —
the bundle auto-detects the only fstab and exposes it as `Flow\Filesystem\FilesystemTable`:

```yaml
# config/packages/flow_filesystem.yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        file:
          type: file
        memory:
          type: memory
```

The YAML key under `filesystems:` (`file`, `memory`) is the **mount protocol** the filesystem will be
registered under. The `type:` field identifies which built-in (or custom) factory builds it.

Inject the table anywhere via autowiring:

```php
use Flow\Filesystem\FilesystemTable;

final class ReportBuilder
{
    public function __construct(private readonly FilesystemTable $fstab)
    {
    }

    public function read(string $uri) : string
    {
        return $this->fstab->for('file')->readFrom(/* path */)->content();
    }
}
```

`FilesystemTable::for()` accepts either a raw protocol string (`'file'`, `'aws-s3'`, `'warehouse'`) or a
`Path` — when given a `Path`, the table resolves the filesystem by the path's URI scheme.

> Most applications need exactly one fstab with multiple filesystems mounted under it. Multi-fstab support
> exists for advanced cases — see [Multi-Fstab Support](#multi-fstab-support).

## Configuration Reference

### Fstabs

At least one fstab is required. Each fstab declares a set of filesystems. The YAML key under
`filesystems:` is the mount protocol and must match the regex `/^[a-zA-Z][a-zA-Z0-9+.-]+$/` (letters,
digits, `.`, `-`, `+`). The `type:` field picks the factory that builds the filesystem.

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        file:
          type: file
        memory:
          type: memory
        warehouse:                  # mount protocol
          type: aws_s3              # factory lookup key
          bucket: '%env(S3_BUCKET)%'
```

Because protocol and `type` are independent, one fstab can mount the same backend multiple times under
different protocols — two S3 buckets as `warehouse` and `archive`, for example. Mount each explicitly:

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        warehouse:
          type: aws_s3
          bucket: '%env(S3_WAREHOUSE_BUCKET)%'
          client_service_id: app.s3_client
        archive:
          type: aws_s3
          bucket: '%env(S3_ARCHIVE_BUCKET)%'
          client_service_id: app.s3_client
```

The fstab is wired as a private `.flow_filesystem.fstab.<name>` service and aliased to
`Flow\Filesystem\FilesystemTable` for autowiring.

### Default Fstab

When you only have one fstab, the bundle picks it as the default automatically — no `default_fstab` key
needed, and the fstab name does not have to be `default`. Set `default_fstab` explicitly only when you
have multiple fstabs and the one you want as the default is not literally named `default`.

### Telemetry

Enable telemetry per fstab to wrap every mounted filesystem with `Flow\Filesystem\Telemetry\TraceableFilesystem`.
You must wire a `Flow\Telemetry\Telemetry` service and a `Psr\Clock\ClockInterface` service yourself —
the bundle does NOT provide defaults.

```yaml
flow_filesystem:
  fstabs:
    default:
      telemetry:
        enabled: true
        telemetry_service_id: app.flow_telemetry   # Required: Telemetry service ID
        clock_service_id: app.system_clock         # Required: PSR-20 clock service
        options:
          trace_streams: true                       # Record stream open/close in traces
          collect_metrics: true                     # Collect read/write metrics
      filesystems:
        file:
          type: file
        memory:
          type: memory
```

With `telemetry.enabled: false` (the default) the fstab is wired without any telemetry overhead.

## Built-in Filesystems

The bundle ships with factories for these types out of the box:

| `type`       | Options | Notes |
|--------------|---------|-------|
| `file`       | none | Local filesystem via native PHP calls. |
| `memory`     | none | In-memory filesystem, lifetime of the kernel. |
| `stdout`     | none | Write-only sink that targets STDOUT. |
| `aws_s3`     | `bucket`, `client*`, `options` | Requires `flow-php/filesystem-async-aws-bridge`. |
| `azure_blob` | `container`, `client*`, `options` | Requires `flow-php/filesystem-azure-bridge`. |

Each entry under `filesystems:` is resolved against a registered `FilesystemFactory` by its `type()`
value. To introduce a new type, register your own factory (see [Custom Filesystem Factory](#custom-filesystem-factory)).

### AWS S3

Install the bridge:

```bash
composer require flow-php/filesystem-async-aws-bridge
```

**Mode A — bring your own `AsyncAws\S3\S3Client`:**

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        aws-s3:
          type: aws_s3
          bucket: my-bucket
          client_service_id: app.async_aws_s3_client
```

**Mode B — inline client config:**

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        aws-s3:
          type: aws_s3
          bucket: my-bucket
          client:
            region: '%env(AWS_REGION)%'
            access_key_id: '%env(AWS_ACCESS_KEY_ID)%'
            access_key_secret: '%env(AWS_SECRET_ACCESS_KEY)%'
            endpoint: ~                # optional, MinIO/R2/Scaleway/LocalStack
            path_style_endpoint: false
            profile: ~
            debug: false
            http_client_service_id: ~
            logger_service_id: ~
          options:
            block_size: 6291456
```

Any null `client.*` key is omitted from the underlying AsyncAws resolution chain, so environment-based
credential discovery still works.

### Azure Blob Storage

Install the bridge:

```bash
composer require flow-php/filesystem-azure-bridge
```

**Mode A — bring your own `Flow\Azure\SDK\BlobServiceInterface`:**

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        azure-blob:
          type: azure_blob
          container: my-container
          client_service_id: app.azure_blob_service
```

**Mode B — inline client config (shared key auth):**

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        azure-blob:
          type: azure_blob
          container: my-container
          client:
            account_name: '%env(AZURE_ACCOUNT_NAME)%'
            auth:
              shared_key: '%env(AZURE_ACCOUNT_KEY)%'
            url_factory:                  # optional, e.g. Azurite
              host: 127.0.0.1
              port: '10000'
              https: false
            http_client_service: ~        # optional PSR-18 client
            request_factory_service: ~    # optional PSR-17 request factory
            stream_factory_service: ~     # optional PSR-17 stream factory
            logger_service_id: ~          # optional PSR-3 logger
          options:
            block_size: 4194304
            list_blob_max_results: 100
```

SAS token auth is not supported yet.

## Console Commands

The bundle ships a small set of `flow:filesystem:*` console commands that operate on any mounted
filesystem. Unlike `league/flysystem-bundle`, which does not provide any CLI tools, these commands are a
Flow-specific differentiator: you can inspect, read, write, copy, move, and delete files across mounted
protocols straight from `bin/console`.

Each command also has a shorter `flow:fs:*` alias (e.g. `flow:fs:ls`).

### Filesystem Commands

| Command | Description |
|---------|-------------|
| `flow:filesystem:ls` | List entries under a directory URI |
| `flow:filesystem:cat` | Stream a file URI to STDOUT |
| `flow:filesystem:cp` | Copy a file between two URIs |
| `flow:filesystem:mv` | Move a file between two URIs (copy + delete source) |
| `flow:filesystem:rm` | Remove a file or directory |
| `flow:filesystem:stat` | Print metadata about a file or directory URI |
| `flow:filesystem:touch` | Create an empty file at a URI |

**Common options:**

| Option | Description |
|--------|-------------|
| `--fstab` (`-f`) | *(advanced)* Target a specific fstab when more than one is configured; defaults to the bundle default fstab |

**Per-command options:**

| Command | Options |
|---------|---------|
| `ls` | `--recursive` (`-r`), `--short` (`-s`), `--limit=N`, `--offset=N`, `--page-size=N`, `--format=json` |
| `rm` | `--recursive` (`-r`) — required for directories |
| `stat` | `--format=json` |
| `touch` | `--force` (`-F`) — overwrite existing file with empty content |

### `ls` output

By default `ls` prints a table with **Type**, **Size**, **Modified**, **URI** columns. Size is formatted
with binary units (`3.12 MiB`, `2.50 KiB`, raw bytes below 1 KiB). Modified is ISO-8601. Both come from
the backend's listing response — no per-file HEAD or GET is issued.

- `--short` drops the metadata columns, leaves only URI.
- `--format=json` emits newline-delimited JSON (NDJSON), one object per line — pipe-friendly for
  scripting.

Results are always paginated in tables of `--page-size` rows (default **10**). When stdout is attached
to an interactive terminal, `ls` prompts between pages with "Show next N entries? [yes/no] (yes)" —
Enter continues, `no` stops. Piped or redirected output flows all pages continuously without prompting.

`--limit=N` caps total entries (no cap by default). When limit is reached, the command emits a
truncated warning and skips the page prompt. `--offset=N` skips the first N entries before listing;
note that offset is client-side, so on S3 the backend still walks the skipped keys — deep offsets on
huge buckets are slow.

### `stat` output

`stat` prints a definition list with URI, Protocol, Path, Type, Size (human-readable), and Modified.
Same metadata source as `ls --long` (now default) — the backend's single `status()` call, no extra
stream opened. Pattern paths (`memory://*.txt`, `**/*.parquet`) are rejected with a clear error.

### URI Format

Path arguments are full URIs in the form `<protocol>://<path>`. When a protocol is omitted, the bundle
assumes the local filesystem (`file://`) and resolves relative paths against the current working directory:

```bash
bin/console flow:filesystem:ls ./reports          # → file:///<cwd>/reports
bin/console flow:filesystem:ls /var/log           # → file:///var/log
bin/console flow:filesystem:ls memory://reports   # explicit protocol
```

### Examples

```bash
# Paginated table with type/size/modified; press Enter to go through pages
bin/console flow:filesystem:ls aws-s3://bucket/

# Top 50 entries only, skipping the first 20
bin/console flow:filesystem:ls aws-s3://bucket/ --offset=20 --limit=50

# Larger pages for a bigger terminal
bin/console flow:filesystem:ls aws-s3://bucket/ --page-size=50

# URI-only output, piped into another command
bin/console flow:filesystem:ls aws-s3://bucket/ --short --limit=1000 | sort

# NDJSON for scripts
bin/console flow:filesystem:ls aws-s3://bucket/ --format=json | jq '.uri'

bin/console flow:filesystem:cat file:///tmp/dump.csv
bin/console flow:filesystem:cp memory://staging/out.parquet file:///var/lib/flow/out.parquet
bin/console flow:filesystem:mv memory://tmp/result.json aws-s3://bucket/result.json
bin/console flow:filesystem:rm file:///var/cache/flow --recursive
bin/console flow:filesystem:stat aws-s3://bucket/key.bin --format=json
bin/console flow:filesystem:touch file:///tmp/.flow_smoke_test
```

### `cp` / `mv` work across protocols, not across fstabs

`cp` and `mv` happily copy between any two protocols mounted in the same fstab — e.g.
`memory://… → aws-s3://…` or `file:///… → azure-blob://…`. Internally they use the
`Flow\Filesystem\Operations\Copy` / `Move` operations which stream in 8 KiB chunks by default, so large
files don't blow up memory. Same-filesystem moves (e.g. `aws-s3://a → aws-s3://b`) use the backend's
native `mv` for server-side optimizations (rename on local, `CopyObject+DeleteObject` on S3,
`CopyBlob+DeleteBlob` on Azure).

Cross-filesystem `mv` is **not atomic**: if the source removal fails after a successful write, the
destination is present and the source remains; re-running is idempotent.

What these commands do **not** do is bridge two separate fstabs: source and destination must resolve
through the same `FilesystemTable`. Since most apps use a single fstab with everything mounted under it,
this is rarely a real constraint. If you genuinely need to move data between two fstabs, run two
commands and stage through a temporary location:

```bash
bin/console flow:filesystem:cp aws-s3://bucket/file.csv file:///tmp/file.csv --fstab=warehouse
bin/console flow:filesystem:cp file:///tmp/file.csv azure-blob://container/file.csv --fstab=archive
```

### `mkdir` is intentionally absent

Remote object stores (S3, Azure Blob, …) do not have a real concept of directories — they expose flat
keyspaces with `/` as a convention. Rather than emulate `mkdir` inconsistently across backends, the bundle
omits the command entirely. Directories appear when files appear inside them.

## Symfony Cache Integration

The bundle integrates with [flow-php/symfony-filesystem-cache-bridge](/documentation/components/bridges/symfony-filesystem-cache-bridge.md) to provide PSR-6 / Symfony Cache pools backed by any filesystem already mounted in a fstab — local disk, S3, Azure Blob, anything the bundle's factories can build. The adapter implements `PruneableInterface`, so `cache:pool:prune` works out of the box.

Each pool resolves its filesystem **through a fstab mount**, not by referencing a service id directly. Filesystems must already be declared under `flow_filesystem.fstabs.<fstab>.filesystems.<protocol>` before a cache pool can target them. This keeps fstab the single place where filesystems live and avoids the cache and the rest of the app drifting into separate filesystem definitions.

### Setup

1. Install the cache bridge:

```bash
composer require flow-php/symfony-filesystem-cache-bridge:~--FLOW_PHP_VERSION--
```

2. Define one or more pools under `flow_filesystem.cache.pools`. Each pool names a fstab mount (the YAML key under `filesystems:`) and a base path inside it:

```yaml
# config/packages/flow_filesystem.yaml
flow_filesystem:
    fstabs:
        default:
            filesystems:
                file:
                    type: file

    cache:
        pools:
            app:
                filesystem: file                                       # mount protocol from the default fstab
                path: '%kernel.project_dir%/var/cache/flow/app'
                default_lifetime: 3600

            sessions:
                filesystem: file
                path: '%kernel.project_dir%/var/cache/flow/sessions'
                namespace: 'sess.'
                default_lifetime: 86400
```

`fstab` is optional and defaults to the bundle's resolved default fstab — same rule the `flow:filesystem:*` CLI commands follow. Set it explicitly when you want a pool to use a non-default fstab:

```yaml
flow_filesystem:
    default_fstab: primary
    fstabs:
        primary:
            filesystems:
                file:
                    type: file
        archive:
            filesystems:
                aws-s3:
                    type: aws_s3
                    bucket: '%env(ARCHIVE_BUCKET)%'

    cache:
        pools:
            cold_storage:
                fstab: archive
                filesystem: aws-s3
                path: '/cache/cold'
                default_lifetime: 86400
```

Each pool registers as `flow_filesystem.cache.pool.<name>` (public).

3. Wire the pools into Symfony's cache framework via `cache.adapter.psr6`:

```yaml
# config/packages/framework.yaml
framework:
    cache:
        pools:
            cache.app_fs:
                adapter: cache.adapter.psr6
                provider: flow_filesystem.cache.pool.app

            cache.sessions_fs:
                adapter: cache.adapter.psr6
                provider: flow_filesystem.cache.pool.sessions
```

The `cache.adapter.psr6` wrapper is required because Symfony's `CachePoolPass` overwrites the first constructor argument of any service used directly as `adapter:`, which conflicts with this bridge's strict `Filesystem` typing on argument 0.

### Configuration Options (per pool)

| Option                   | Default        | Description                                                                                            |
|--------------------------|----------------|--------------------------------------------------------------------------------------------------------|
| `fstab`                  | default fstab  | Fstab name. Defaults to the bundle's resolved default fstab when omitted.                              |
| `filesystem`             | *required*     | Mount protocol within the chosen fstab (the YAML key under `filesystems:`).                            |
| `path`                   | *required*     | Base directory inside the chosen filesystem where cache files are stored.                              |
| `namespace`              | `''`           | Cache pool namespace; chars in `[-+.A-Za-z0-9]` only.                                                  |
| `default_lifetime`       | `0`            | Default TTL in seconds; `0` means no expiry.                                                           |
| `marshaller_service_id`  | `null`         | Service ID of a custom `MarshallerInterface`.                                                          |

Validation runs at container compile time:

- A pool referencing a missing fstab fails with `flow_filesystem.cache.pools.<name>: fstab "<x>" is not declared. Available fstabs: [...]`.
- A pool referencing a mount protocol that is not registered in the chosen fstab fails with `flow_filesystem.cache.pools.<name>: filesystem "<x>" is not mounted in fstab "<y>". Available mounts: [...]`.
- `flow_filesystem.cache.pools` is configured but `flow-php/symfony-filesystem-cache-bridge` is not installed → fails fast with a message pointing at the missing package.

### Pruning

Schedule the standard Symfony command on a cron to remove expired files:

```bash
php bin/console cache:pool:prune
```

Without pruning, expired files accumulate under each pool's directory. They are filtered out on read but only deleted when either the same key is fetched again or `cache:pool:prune` runs.

For full documentation, see the [Symfony Filesystem Cache Bridge](/documentation/components/bridges/symfony-filesystem-cache-bridge.md).

## Multi-Fstab Support

> **Advanced.** Most applications should stick to a single fstab with multiple filesystems mounted under
> it. Reach for multi-fstab only when you need fully isolated tables — e.g. strict separation between
> read-only archive storage and a read/write working area, or two completely independent configurations
> that should never bleed across to one another.

The bundle supports multiple independent fstabs, each with its own protocol keyspace:

```yaml
flow_filesystem:
  default_fstab: primary
  fstabs:
    primary:
      filesystems:
        file:
          type: file

    analytics:
      filesystems:
        memory:
          type: memory
        file:
          type: file

    archive:
      filesystems:
        aws-s3:
          type: aws_s3
          bucket: '%env(ARCHIVE_BUCKET)%'
```

Each fstab gets its own `.flow_filesystem.fstab.<name>` service. The default fstab is automatically aliased
to `Flow\Filesystem\FilesystemTable`, allowing direct type-hint injection without specifying a fstab name.
Each named fstab is also aliased as `Flow\Filesystem\FilesystemTable $<camelCasedName>Fstab` for
named-argument autowiring:

```php
use Flow\Filesystem\FilesystemTable;

final class ReportBuilder
{
    public function __construct(
        private readonly FilesystemTable $fstab,           // → primary (default)
        private readonly FilesystemTable $analyticsFstab,  // → analytics
        private readonly FilesystemTable $archiveFstab,    // → archive
    ) {
    }

    public function read() : string
    {
        return $this->fstab->for('file')->readFrom(/* path */)->content();
    }
}
```

Target a specific fstab with any console command:

```bash
bin/console flow:filesystem:ls aws-s3://bucket/reports --fstab=archive
```

## Custom Filesystem Factory

The bundle has no concept of "custom" filesystems — every filesystem is created by a `FilesystemFactory`.
To plug in your own backend, implement
`Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory`:

```php
namespace App\Flow;

use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\Filesystem;

final class MyFilesystemFactory implements FilesystemFactory
{
    public function type() : string
    {
        return 'my_backend';
    }

    public function create(string $protocol, array $config) : Filesystem
    {
        // $protocol is the YAML key (mount protocol) the user chose.
        // $config is the array under filesystems.<protocol> in YAML with `type` already stripped.
    }
}
```

`type()` returns the factory lookup key — it's what users put under `type:` in YAML. The mount protocol
is a separate concept and comes from the YAML key (passed to `create()` as `$protocol`).

There are two ways to register the factory.

### Attribute-Based Discovery

Annotate the factory class with `#[AsFilesystemFactory]` for automatic discovery:

```php
namespace App\Flow;

use Flow\Bridge\Symfony\FilesystemBundle\Attribute\AsFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\Filesystem;

#[AsFilesystemFactory(type: 'my_backend')]
final class MyFilesystemFactory implements FilesystemFactory
{
    public function type() : string { return 'my_backend'; }
    public function create(string $protocol, array $config) : Filesystem { /* ... */ }
}
```

As long as your service is autoconfigured (the default in `services.yaml` for everything under your `App\`
namespace), the bundle automatically attaches the `flow_filesystem.factory` tag with the right `type`
attribute.

### Explicit Tag

Useful if your factory lives in a service that doesn't have autoconfigure enabled (e.g. third-party
namespaces, manual definitions):

```yaml
services:
  app.flow_filesystem.factory.my_backend:
    class: App\Flow\MyFilesystemFactory
    tags:
      - { name: flow_filesystem.factory, type: my_backend }
```

Either way, you can then mount the backend under any protocol in any fstab:

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        my-mount:
          type: my_backend
          foo: bar    # arbitrary options handed to the factory's create()
```

**Notes:**

- The `type` attribute on the tag (or on `#[AsFilesystemFactory]`) must match `type()` exactly.
- A `type` can be served by exactly **one** factory; registering two factories for the same type fails
  at compile time.
- Within a single fstab, a mount protocol (the YAML key under `filesystems:`) can be used exactly
  **once** — this is enforced by `FilesystemTable` itself. To mount the same backend twice with
  different options (e.g. two S3 buckets), use two distinct mount protocols pointing to the same `type`.

## Comparison with `league/flysystem-bundle`

- **Multi-fstab:** each fstab is an independent `FilesystemTable` service with its own protocol keyspace.
  Flysystem Bundle wires individual filesystems keyed only by name.
- **Mount-protocol routing:** filesystems inside a fstab are resolved at runtime via
  `$table->for('warehouse')` / `$table->for($path)`, so application code can hand-off across protocols
  without knowing service ids.
- **Factory tag:** filesystem types are pluggable via a standard `flow_filesystem.factory` DI tag;
  third-party libraries can ship their own factory without bundle changes.
- **CLI commands:** `flow:filesystem:*` ship with the bundle and operate on any configured fstab.
  Flysystem Bundle does not ship any console commands.
- **Telemetry first-class:** OTel `TraceableFilesystem` is wired by simply toggling
  `telemetry.enabled: true` per fstab.
- **No autoconfigure magic:** every tagged service and compiler-pass action is explicit, matching the
  Symfony 6.4+/7.4+/8.0+ bundle best practices.

## Complete Example

A typical single-fstab setup with local + S3 mounts and telemetry enabled:

```yaml
# config/packages/flow_filesystem.yaml
flow_filesystem:
  fstabs:
    default:
      telemetry:
        enabled: true
        telemetry_service_id: app.flow_telemetry
        clock_service_id: app.system_clock
        options:
          trace_streams: true
          collect_metrics: true
      filesystems:
        file:
          type: file
        memory:
          type: memory
        aws-s3:
          type: aws_s3
          bucket: '%env(S3_BUCKET)%'
          client:
            region: '%env(AWS_REGION)%'
            access_key_id: '%env(AWS_ACCESS_KEY_ID)%'
            access_key_secret: '%env(AWS_SECRET_ACCESS_KEY)%'
          options:
            block_size: 6291456
```
