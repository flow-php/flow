---
package: flow-php/symfony-filesystem-bundle
---

# Symfony Filesystem Bundle

This Symfony bundle connects the Flow PHP Filesystem library to Symfony applications. It gives you a typed
`FilesystemTable` service that mounts multiple filesystems under URI protocols. It also gives you pluggable
factories, console commands that manage files across mounted protocols, and OpenTelemetry integration.

[PACKAGE_NAV]

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

This bundle connects the Flow PHP Filesystem library to Symfony applications. It provides:

- Mount-protocol routing: you mount filesystems under URI protocols that you select (`file`, `memory`, `aws-s3`, `warehouse`, `archive`, ...). At runtime, you get them with `$table->for(...)`.
- Pluggable filesystem factories: you register custom backends with the `#[AsFilesystemFactory]` attribute or a DI tag.
- Built-in factories: the bundle includes `file`, `memory`, `stdout`, `aws_s3`, and `azure_blob`.
- Console commands: `flow:filesystem:*` (alias `flow:fs:*`) runs `ls`, `cat`, `cp`, `mv`, `rm`, `stat`, `touch` on any configured filesystem.
- Symfony Cache pools: if [flow-php/symfony-filesystem-cache-bridge](/documentation/components/bridges/symfony-filesystem-cache-bridge.md) is installed, you can register PSR-6 cache pools on any mounted filesystem (local disk, S3, Azure Blob).
- Telemetry integration: you can wrap every filesystem in `TraceableFilesystem` for OpenTelemetry.
- Multi-fstab support (advanced): if you need them, you can configure several independent `FilesystemTable` services.

## Quick Start

A minimal configuration mounts one or more filesystems in a single fstab. An fstab is a named table of
mounted filesystems. You can give the fstab any name. If there is only one fstab, the bundle finds it and
exposes it as `Flow\Filesystem\FilesystemTable`:

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

The YAML key under `filesystems:` (`file`, `memory`) is the mount protocol of the filesystem. The `type:`
field names the built-in or custom factory that builds it.

Use autowiring to inject the table into any service:

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

`FilesystemTable::for()` accepts a raw protocol string (`'file'`, `'aws-s3'`, `'warehouse'`) or a `Path`.
If you give a `Path`, the table finds the filesystem from the URI scheme of the path.

> Most applications need exactly one fstab with multiple filesystems mounted in it. The
> [Multi-Fstab Support](#multi-fstab-support) section covers advanced cases.

### Injecting a Single Filesystem

If a service needs only one mounted filesystem, inject `Flow\Filesystem\Filesystem` instead of the whole
table. The bundle registers every mount as a private service and exposes it in two ways.

Named-argument autowiring: each mount of the default fstab has the alias `Filesystem $<protocol>`. Every
mount of any fstab also has the alias `Filesystem $<fstab><Protocol>`. If a protocol contains `-`, `.` or
`+`, the bundle converts it to camel case (`aws-s3` becomes `awsS3`). This follows the
`FilesystemTable $<name>Fstab` convention:

```php
use Flow\Filesystem\Filesystem;

final class ReportBuilder
{
    public function __construct(
        private readonly Filesystem $warehouse,       // → default fstab, 'warehouse' mount
        private readonly Filesystem $analyticsFile,   // → 'analytics' fstab, 'file' mount
    ) {
    }
}
```

`#[AsFilesystem]` attribute: use it to select a mount explicitly, or to give the argument a name that is
different from the protocol. If you omit `fstab`, the attribute targets the default fstab:

```php
use Flow\Bridge\Symfony\FilesystemBundle\Attribute\AsFilesystem;
use Flow\Filesystem\Filesystem;

final class ReportBuilder
{
    public function __construct(
        #[AsFilesystem('warehouse')]
        private readonly Filesystem $primary,
        #[AsFilesystem('cold', fstab: 'archive')]
        private readonly Filesystem $coldStorage,
    ) {
    }
}
```

Named-argument aliases work for any autowired service. If you use the attribute, the consuming service must be
autoconfigured (the default for everything under your `App\` namespace). Both resolve through
`FilesystemTable::for()` of the fstab, so telemetry decoration stays in place. If a protocol or fstab is
unknown, the container compile fails and lists the available options.

## Configuration Reference

### Fstabs

You must configure at least one fstab. Each fstab declares a set of filesystems. The YAML key under
`filesystems:` is the mount protocol. It must match the regex `/^[a-zA-Z][a-zA-Z0-9+.-]+$/` (letters,
digits, `.`, `-`, `+`). The `type:` field selects the factory that builds the filesystem.

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

The protocol and `type` are independent. Thus one fstab can mount the same backend multiple times under
different protocols, for example two S3 buckets as `warehouse` and `archive`. Mount each one explicitly:

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

The bundle registers the fstab as a private `.flow.filesystem.fstab.<name>` service. It also aliases the
fstab to `Flow\Filesystem\FilesystemTable` for autowiring.

### Default Fstab

If you have only one fstab, the bundle makes it the default. You do not need a `default_fstab` key, and the
fstab name does not have to be `default`. If you have multiple fstabs and the default one is not named
`default`, set `default_fstab` explicitly.

### Telemetry

Enable telemetry for each fstab that you want to trace. The bundle then wraps every mounted filesystem of
that fstab with `Flow\Filesystem\Telemetry\TraceableFilesystem`. You must register a
`Flow\Telemetry\Telemetry` service and a `Psr\Clock\ClockInterface` service yourself. The bundle does not
provide defaults.

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

With `telemetry.enabled: false` (the default), the fstab has no telemetry overhead.

## Built-in Filesystems

The bundle includes factories for these types:

| `type`       | Options                           | Notes                                                 |
|--------------|-----------------------------------|-------------------------------------------------------|
| `file`       | none                              | Local filesystem through native PHP calls.            |
| `memory`     | none                              | In-memory filesystem. It lives as long as the kernel. |
| `stdout`     | none                              | Write-only sink that writes to STDOUT.                |
| `aws_s3`     | `bucket`, `client*`, `options`    | Requires `flow-php/filesystem-async-aws-bridge`.      |
| `azure_blob` | `container`, `client*`, `options` | Requires `flow-php/filesystem-azure-bridge`.          |

The bundle matches each entry under `filesystems:` to a registered `FilesystemFactory` by its `type()`
value. To add a new type, register your own factory. See [Custom Filesystem Factory](#custom-filesystem-factory) for the steps.

### AWS S3

Install the bridge:

```bash
composer require flow-php/filesystem-async-aws-bridge
```

Mode A: use your own `AsyncAws\S3\S3Client`:

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

Mode B: inline client configuration:

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
            endpoint: ~                # optional, RustFS/R2/Scaleway/LocalStack
            path_style_endpoint: false
            profile: ~
            debug: false
            http_client_service_id: ~
            logger_service_id: ~
          options:
            block_size: 6291456
```

If a `client.*` key is null, the bundle omits it from the AsyncAws resolution chain. Thus credential
discovery from the environment still works.

### Azure Blob Storage

Install the bridge:

```bash
composer require flow-php/filesystem-azure-bridge
```

Mode A: use your own `Flow\Azure\SDK\BlobServiceInterface`:

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

Mode B: inline client configuration (shared key auth):

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

The bundle includes a small set of `flow:filesystem:*` console commands. They work on any mounted
filesystem. `league/flysystem-bundle` does not provide CLI tools. With these commands, you can inspect,
read, write, copy, move, and delete files across mounted protocols from `bin/console`.

Each command also has a shorter `flow:fs:*` alias, for example `flow:fs:ls`.

### Filesystem Commands

| Command                 | Description                                         |
|-------------------------|-----------------------------------------------------|
| `flow:filesystem:ls`    | List entries under a directory URI                  |
| `flow:filesystem:cat`   | Stream a file URI to STDOUT                         |
| `flow:filesystem:cp`    | Copy a file between two URIs                        |
| `flow:filesystem:mv`    | Move a file between two URIs (copy + delete source) |
| `flow:filesystem:rm`    | Remove a file or directory                          |
| `flow:filesystem:stat`  | Print metadata about a file or directory URI        |
| `flow:filesystem:touch` | Create an empty file at a URI                       |

Common options:

| Option           | Description                                                                                                                  |
|------------------|------------------------------------------------------------------------------------------------------------------------------|
| `--fstab` (`-f`) | Advanced option. If more than one fstab is configured, it selects one fstab. The default is the default fstab of the bundle. |

Per-command options:

| Command | Options                                                                                             |
|---------|-----------------------------------------------------------------------------------------------------|
| `ls`    | `--recursive` (`-r`), `--short` (`-s`), `--limit=N`, `--offset=N`, `--page-size=N`, `--format=json` |
| `rm`    | `--recursive` (`-r`): required for directories                                                      |
| `stat`  | `--format=json`                                                                                     |
| `touch` | `--force` (`-F`): overwrites the existing file with empty content                                   |

### `ls` output

By default, `ls` prints a table with the columns Type, Size, Modified, and URI. Size uses binary units
(`3.12 MiB`, `2.50 KiB`, raw bytes below 1 KiB). Modified uses ISO-8601. Both values come from the listing
response of the backend. The command sends no HEAD or GET request for each file.

- `--short` removes the metadata columns and shows only the URI.
- `--format=json` writes newline-delimited JSON (NDJSON), one object per line. Use it to pipe the output
  into scripts.

The output is always split into tables of `--page-size` rows (default 10). If stdout is an interactive
terminal, `ls` asks "Show next N entries? [yes/no] (yes)" between pages. Enter continues, and `no` stops.
If the output is piped or redirected, the command writes all pages without a prompt.

`--limit=N` sets a maximum number of entries (no maximum by default). When the command reaches the limit,
it writes a truncation warning and skips the page prompt. `--offset=N` skips the first N entries before
listing. The offset is applied on the client. Thus on S3 the backend still reads the skipped keys, and deep
offsets on large buckets are slow.

### `stat` output

`stat` prints a definition list with URI, Protocol, Path, Type, Size (human-readable), and Modified. The
metadata comes from the same source as `ls`: one `status()` call to the backend, with
no extra stream. If you give a pattern path (`memory://*.txt`, `**/*.parquet`), the command fails with a
clear error.

### URI Format

Path arguments are full URIs in the form `<protocol>://<path>`. If you omit the protocol, the bundle uses
the local filesystem (`file://`). It resolves relative paths against the current working directory:

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

`cp` and `mv` copy between any two protocols that are mounted in the same fstab, for example
`memory://... → aws-s3://...` or `file:///... → azure-blob://...`. They use the
`Flow\Filesystem\Operations\Copy` / `Move` operations. These operations stream data in 8 KiB chunks by
default, so large files do not fill the memory. A move inside one filesystem (for example
`aws-s3://a → aws-s3://b`) uses the native `mv` of the backend. On local disk that is a rename. On S3 it is
`CopyObject+DeleteObject`, and on Azure it is `CopyBlob+DeleteBlob`.

A `mv` between two filesystems is not atomic. If the removal of the source fails after a successful write,
the destination exists and the source stays. You can run the command again with the same result.

These commands do not connect two separate fstabs. The source and the destination must resolve through the
same `FilesystemTable`. Most applications use a single fstab with all filesystems mounted in it, so this
limit is rarely a problem. To move data between two fstabs, run two commands and use a temporary location:

```bash
bin/console flow:filesystem:cp aws-s3://bucket/file.csv file:///tmp/file.csv --fstab=warehouse
bin/console flow:filesystem:cp file:///tmp/file.csv azure-blob://container/file.csv --fstab=archive
```

### `mkdir` is intentionally absent

Remote object stores (S3, Azure Blob) do not have real directories. They expose flat keyspaces and use `/`
as a convention. The bundle does not include `mkdir`, because it cannot emulate it the same way on all
backends. If you write a file in a directory, the directory appears.

## Symfony Cache Integration

The bundle works with [flow-php/symfony-filesystem-cache-bridge](/documentation/components/bridges/symfony-filesystem-cache-bridge.md) to provide PSR-6 / Symfony Cache pools. A pool can use any filesystem that is mounted in a fstab: local disk, S3, Azure Blob, or anything that the factories of the bundle can build. The adapter implements `PruneableInterface`, so `cache:pool:prune` works without extra configuration.

Each pool finds its filesystem through a fstab mount, not through a service id. You must declare a filesystem under `flow_filesystem.fstabs.<fstab>.filesystems.<protocol>` before a cache pool can use it. Thus the fstab is the single place where filesystems are defined. The cache and the rest of the application cannot drift into separate filesystem definitions.

### Setup

1. Install the cache bridge:

```bash
composer require flow-php/symfony-filesystem-cache-bridge:~--FLOW_PHP_VERSION--
```

2. Define one or more pools under `flow_filesystem.cache.pools`. Each pool names a fstab mount (the YAML key under `filesystems:`) and a base path in it:

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

`fstab` is optional. Its default is the resolved default fstab of the bundle. The `flow:filesystem:*` CLI commands use the same rule. To make a pool use a different fstab, set `fstab` explicitly:

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

Each pool is registered as the public service `flow.filesystem.cache.pool.<name>`.

3. Connect the pools to the Symfony cache framework with `cache.adapter.psr6`:

```yaml
# config/packages/framework.yaml
framework:
    cache:
        pools:
            cache.app_fs:
                adapter: cache.adapter.psr6
                provider: flow.filesystem.cache.pool.app

            cache.sessions_fs:
                adapter: cache.adapter.psr6
                provider: flow.filesystem.cache.pool.sessions
```

The `cache.adapter.psr6` wrapper is necessary. The Symfony `CachePoolPass` replaces the first constructor argument of any service that you use directly as `adapter:`. That conflicts with the strict `Filesystem` type of argument 0 in this bridge.

### Configuration Options (per pool)

| Option                  | Default       | Description                                                                         |
|-------------------------|---------------|-------------------------------------------------------------------------------------|
| `fstab`                 | default fstab | Fstab name. If omitted, the bundle uses its resolved default fstab.                 |
| `filesystem`            | *required*    | Mount protocol in the chosen fstab (the YAML key under `filesystems:`).             |
| `path`                  | *required*    | Base directory in the chosen filesystem where the cache files are stored.           |
| `namespace`             | `''`          | Cache pool namespace. Only chars in `[-+.A-Za-z0-9]` are allowed.                   |
| `default_lifetime`      | `0`           | Default TTL in seconds. `0` means no expiry.                                        |
| `marshaller_service_id` | `null`        | Service ID of a custom `MarshallerInterface`.                                       |

At container compile time, the bundle makes sure that each pool is correct:

- If a pool references a missing fstab, the compile fails with `flow_filesystem.cache.pools.<name>: fstab "<x>" is not declared. Available fstabs: [...]`.
- If a pool references a mount protocol that is not registered in the chosen fstab, the compile fails with `flow_filesystem.cache.pools.<name>: filesystem "<x>" is not mounted in fstab "<y>". Available mounts: [...]`.
- If `flow_filesystem.cache.pools` is configured but `flow-php/symfony-filesystem-cache-bridge` is not installed, the compile fails immediately. The message names the missing package.

### Pruning

Run the standard Symfony command from a cron job to remove expired files:

```bash
php bin/console cache:pool:prune
```

If you do not prune, expired files collect in the directory of each pool. A read ignores them. Two events delete them: a new fetch of the same key, or a run of `cache:pool:prune`.

The [Symfony Filesystem Cache Bridge](/documentation/components/bridges/symfony-filesystem-cache-bridge.md) page has the full documentation.

## Multi-Fstab Support

> Advanced. Most applications use a single fstab with multiple filesystems mounted in it. If you need fully
> isolated tables, use multi-fstab. One example is strict separation between read-only archive
> storage and a read/write work area. Another example is two independent configurations that must never mix.

The bundle supports multiple independent fstabs. Each fstab has its own protocol keyspace:

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

Each fstab gets its own `.flow.filesystem.fstab.<name>` service. The bundle aliases the default fstab to
`Flow\Filesystem\FilesystemTable`. Thus you can inject it by type-hint without a fstab name. Each named
fstab also has the alias `Flow\Filesystem\FilesystemTable $<camelCasedName>Fstab` for named-argument
autowiring:

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

To target a specific fstab, use the `--fstab` option with any console command:

```bash
bin/console flow:filesystem:ls aws-s3://bucket/reports --fstab=archive
```

## Custom Filesystem Factory

The bundle has no concept of "custom" filesystems. A `FilesystemFactory` creates every filesystem. To add
your own backend, implement `Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory`:

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

`type()` returns the factory lookup key. Users write this key under `type:` in YAML. The mount protocol is
a different concept. It comes from the YAML key, and `create()` receives it as `$protocol`.

You can register the factory in two ways.

### Attribute-Based Discovery

Add `#[AsFilesystemFactory]` to the factory class for automatic discovery:

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

If your service is autoconfigured (the default in `services.yaml` for everything under your `App\`
namespace), the bundle adds the `flow.filesystem.factory` tag. The tag gets the correct `type` attribute.

### Explicit Tag

If your factory is a service without autoconfigure, use the tag. Examples are services in third-party
namespaces or manual definitions:

```yaml
services:
  app.flow.filesystem.factory.my_backend:
    class: App\Flow\MyFilesystemFactory
    tags:
      - { name: flow.filesystem.factory, type: my_backend }
```

After you register the factory, you can mount the backend under any protocol in any fstab:

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        my-mount:
          type: my_backend
          foo: bar    # arbitrary options handed to the factory's create()
```

Notes:

- The `type` attribute on the tag (or on `#[AsFilesystemFactory]`) must match `type()` exactly.
- Exactly one factory can serve a `type`. If you register two factories for the same type, the compile
  fails.
- In a single fstab, you can use a mount protocol (the YAML key under `filesystems:`) only once.
  `FilesystemTable` enforces this rule. To mount the same backend twice with different options (for
  example two S3 buckets), use two mount protocols with the same `type`.

## Comparison with `league/flysystem-bundle`

- Multi-fstab: each fstab is an independent `FilesystemTable` service with its own protocol keyspace.
  Flysystem Bundle registers individual filesystems by name only.
- Mount-protocol routing: the table resolves filesystems at runtime with `$table->for('warehouse')` /
  `$table->for($path)`. Application code can pass work across protocols without service ids.
- Factory tag: a standard `flow.filesystem.factory` DI tag makes filesystem types pluggable. Third-party
  libraries can ship their own factory without changes to the bundle.
- CLI commands: the bundle includes `flow:filesystem:*`, which operate on any configured fstab. Flysystem
  Bundle has no console commands.
- Telemetry: to wire OTel `TraceableFilesystem`, set `telemetry.enabled: true` for each fstab.
- Attribute discovery uses autoconfiguration for `#[AsFilesystemFactory]` and `#[AsFilesystem]`. All other tagged
  services and compiler-pass actions are explicit. This follows the bundle best practices of Symfony 6.4+/7.4+/8.0+.

## Complete Example

A typical single-fstab setup with local and S3 mounts and telemetry enabled:

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
