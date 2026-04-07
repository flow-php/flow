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

- **Protocol-keyed routing** — mount filesystems under URI protocols (`file://`, `memory://`, `aws-s3://`, `azure-blob://`, …) and resolve them at runtime via `$table->for(...)`
- **Pluggable filesystem factories** — register custom protocols with the `#[AsFilesystemFactory]` attribute or a DI tag
- **Built-in factories** — `file`, `memory`, `stdout`, `aws-s3`, and `azure-blob` ship out of the box
- **Console commands** — `flow:filesystem:*` (alias `flow:fs:*`) for `ls`, `cat`, `cp`, `mv`, `rm`, `stat`, `touch` against any configured filesystem
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
        file: ~
        memory: ~
```

Inject the table anywhere via autowiring:

```php
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Protocol;

final class ReportBuilder
{
    public function __construct(private readonly FilesystemTable $fstab)
    {
    }

    public function read(string $uri) : string
    {
        return $this->fstab->for(new Protocol('file'))->readFrom(/* path */)->content();
    }
}
```

> Most applications need exactly one fstab with multiple filesystems mounted under it. Multi-fstab support
> exists for advanced cases — see [Multi-Fstab Support](#multi-fstab-support).

## Configuration Reference

### Fstabs

At least one fstab is required. Each fstab defines a set of filesystems keyed by protocol. The YAML key
under `filesystems:` is the protocol the filesystem will be mounted under.

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        file: ~
        memory: ~
        aws-s3:
          bucket: '%env(S3_BUCKET)%'
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
        file: ~
        memory: ~
```

With `telemetry.enabled: false` (the default) the fstab is wired without any telemetry overhead.

## Built-in Filesystems

The bundle ships with factories for these protocols out of the box:

| Protocol | Options | Notes |
|----------|---------|-------|
| `file` | none | Local filesystem via native PHP calls. |
| `memory` | none | In-memory filesystem, lifetime of the kernel. |
| `stdout` | none | Write-only sink that targets STDOUT. |
| `aws-s3` | `bucket`, `client*`, `options` | Requires `flow-php/filesystem-async-aws-bridge`. |
| `azure-blob` | `container`, `client*`, `options` | Requires `flow-php/filesystem-azure-bridge`. |

The protocol used as a YAML key is matched against a registered `FilesystemFactory` via that factory's
`protocol()` value. To mount a custom protocol, register your own factory (see [Custom Filesystem Factory](#custom-filesystem-factory)).

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
| `ls` | `--recursive` (`-r`), `--long` (`-l`), `--format=json` |
| `rm` | `--recursive` (`-r`) — required for directories |
| `stat` | `--format=json` |
| `touch` | `--force` (`-F`) — overwrite existing file with empty content |

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
bin/console flow:filesystem:ls memory://reports --long
bin/console flow:filesystem:cat file:///tmp/dump.csv
bin/console flow:filesystem:cp memory://staging/out.parquet file:///var/lib/flow/out.parquet
bin/console flow:filesystem:mv memory://tmp/result.json aws-s3://bucket/result.json
bin/console flow:filesystem:rm file:///var/cache/flow --recursive
bin/console flow:filesystem:stat aws-s3://bucket/key.bin --format=json
bin/console flow:filesystem:touch file:///tmp/.flow_smoke_test
```

### `cp` / `mv` work across protocols, not across fstabs

`cp` and `mv` happily copy between any two protocols mounted in the same fstab — e.g.
`memory://… → aws-s3://…` or `file:///… → azure-blob://…`. What they do **not** do is bridge two
separate fstabs: source and destination must resolve through the same `FilesystemTable`. Since most apps
use a single fstab with everything mounted under it, this is rarely a real constraint.

If you genuinely need to move data between two fstabs, run two commands and stage through a temporary
location:

```bash
bin/console flow:filesystem:cp aws-s3://bucket/file.csv file:///tmp/file.csv --fstab=warehouse
bin/console flow:filesystem:cp file:///tmp/file.csv azure-blob://container/file.csv --fstab=archive
```

### `mkdir` is intentionally absent

Remote object stores (S3, Azure Blob, …) do not have a real concept of directories — they expose flat
keyspaces with `/` as a convention. Rather than emulate `mkdir` inconsistently across backends, the bundle
omits the command entirely. Directories appear when files appear inside them.

## Multi-Fstab Support

> **Advanced.** Most applications should stick to a single fstab with multiple filesystems mounted under
> it. Reach for multi-fstab only when you need fully isolated tables — e.g. the same protocol mounted
> against two different backends (two S3 buckets, two Azure containers), or strict separation between
> read-only archive storage and a read/write working area.

The bundle supports multiple independent fstabs, each with its own protocol keyspace:

```yaml
flow_filesystem:
  default_fstab: primary
  fstabs:
    primary:
      filesystems:
        file: ~

    analytics:
      filesystems:
        memory: ~
        file: ~

    archive:
      filesystems:
        aws-s3:
          bucket: '%env(ARCHIVE_BUCKET)%'
```

Each fstab gets its own `.flow_filesystem.fstab.<name>` service. The default fstab is automatically aliased
to `Flow\Filesystem\FilesystemTable`, allowing direct type-hint injection without specifying a fstab name.
Each named fstab is also aliased as `Flow\Filesystem\FilesystemTable $<camelCasedName>Fstab` for
named-argument autowiring:

```php
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Protocol;

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
        return $this->fstab->for(new Protocol('file'))->readFrom(/* path */)->content();
    }
}
```

Target a specific fstab with any console command:

```bash
bin/console flow:filesystem:ls aws-s3://bucket/reports --fstab=archive
```

## Custom Filesystem Factory

The bundle has no concept of "custom" filesystems — every filesystem is created by a `FilesystemFactory`.
To plug in your own protocol, implement
`Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory`:

```php
namespace App\Flow;

use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\{Filesystem, Protocol};

final class MyFilesystemFactory implements FilesystemFactory
{
    public function protocol() : Protocol
    {
        return new Protocol('my-fs');
    }

    public function create(Protocol $protocol, array $config) : Filesystem
    {
        // build and return the Filesystem; $config is the array under
        // filesystems.<protocol> in YAML.
    }
}
```

There are two ways to register the factory.

### Attribute-Based Discovery

Annotate the factory class with `#[AsFilesystemFactory]` for automatic discovery:

```php
namespace App\Flow;

use Flow\Bridge\Symfony\FilesystemBundle\Attribute\AsFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\{Filesystem, Protocol};

#[AsFilesystemFactory(protocol: 'my-fs')]
final class MyFilesystemFactory implements FilesystemFactory
{
    public function protocol() : Protocol { return new Protocol('my-fs'); }
    public function create(Protocol $protocol, array $config) : Filesystem { /* ... */ }
}
```

As long as your service is autoconfigured (the default in `services.yaml` for everything under your `App\`
namespace), the bundle automatically attaches the `flow_filesystem.factory` tag with the right `protocol`
attribute.

### Explicit Tag

Useful if your factory lives in a service that doesn't have autoconfigure enabled (e.g. third-party
namespaces, manual definitions):

```yaml
services:
  app.flow_filesystem.factory.my_fs:
    class: App\Flow\MyFilesystemFactory
    tags:
      - { name: flow_filesystem.factory, protocol: my-fs }
```

Either way, you can then mount the protocol in any fstab:

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        my-fs:
          foo: bar    # arbitrary options handed to the factory's create()
```

**Notes:**

- The `protocol` attribute on the tag (or on `#[AsFilesystemFactory]`) must match `protocol()->name` exactly.
  The bundle does not enforce this at compile time, but the registry will fail at boot if mismatched.
- A protocol can be served by exactly **one** factory; registering two factories for the same protocol
  fails at compile time.
- Within a single fstab, a protocol can be mounted exactly **once** — this is enforced by `FilesystemTable`
  itself. To use the same backend twice (e.g. two S3 buckets), put each one in its own fstab.

## Comparison with `league/flysystem-bundle`

- **Multi-fstab:** each fstab is an independent `FilesystemTable` service with its own protocol keyspace.
  Flysystem Bundle wires individual filesystems keyed only by name.
- **Protocol-keyed routing:** filesystems inside a fstab are resolved at runtime via
  `$table->for(new Protocol('...'))`, so application code can hand-off across protocols without knowing
  service ids.
- **Factory tag:** filesystem protocols are pluggable via a standard `flow_filesystem.factory` DI tag;
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
        file: ~
        memory: ~
        aws-s3:
          bucket: '%env(S3_BUCKET)%'
          client:
            region: '%env(AWS_REGION)%'
            access_key_id: '%env(AWS_ACCESS_KEY_ID)%'
            access_key_secret: '%env(AWS_SECRET_ACCESS_KEY)%'
          options:
            block_size: 6291456
```
