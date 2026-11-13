---
package: flow-php/filesystem-azure-bridge
---

# Filesystem Azure Bridge

[PACKAGE_NAV]

[TOC]

With the Filesystem Azure Bridge, you can use Azure Blob Storage as a filesystem in your application.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/filesystem-azure-bridge.md).

## Usage

The plain form takes the account, the container and the shared key, and builds the service itself:

```php
<?php

use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;

$azure = azure_filesystem([
    'account_name' => $_ENV['AZURE_ACCOUNT'],
    'container' => $_ENV['AZURE_CONTAINER'],
    'shared_key' => $_ENV['AZURE_SHARED_KEY'],
    // 'url' => ['azurite' => ['host' => '127.0.0.1', 'port' => '10000', 'https' => false]],
]);
```

### HTTP limits

```php
<?php

use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;

$azure = azure_filesystem([
    'account_name' => $_ENV['AZURE_ACCOUNT'],
    'container' => $_ENV['AZURE_CONTAINER'],
    'shared_key' => $_ENV['AZURE_SHARED_KEY'],
    'http' => ['timeout' => 30, 'max_duration' => 120, 'max_retries' => 1],
]);
```

| Key            | Default | Meaning                                                                         |
|----------------|---------|---------------------------------------------------------------------------------|
| `timeout`      | `30`    | Seconds that a connection can stay idle.                                        |
| `max_duration` | `300`   | Seconds for one call, its retries and their backoff included. 0 means no limit. |
| `max_retries`  | `3`     | Retries of a call that failed with a transport error or a retryable status.     |

The limits are part of the filesystem description, so a worker process builds the same service.

You can also build a filesystem from your own `BlobServiceInterface`, with your own HTTP client, factories or
logger. Such a filesystem stays in this process and reads serially.

> [!NOTE]  
> The Azure SDK does not include an http client or factories, so you must install them yourself.
> The example below uses the `php-http/discovery` package to find the factories in the existing dependencies of your project.
> Use these links to find implementations of the client and the factories:

- [Http Client](https://packagist.org/providers/psr/http-client-implementation)
- [Http Factories](https://packagist.org/providers/psr/http-factory-implementation)


```php
<?php

use Http\Discovery\Psr17FactoryDiscovery;
use Http\Discovery\Psr18ClientDiscovery;
use function Flow\Azure\SDK\DSL\azure_blob_service;
use function Flow\Azure\SDK\DSL\azure_blob_service_config;
use function Flow\Azure\SDK\DSL\azure_http_factory;
use function Flow\Azure\SDK\DSL\azure_shared_key_authorization_factory;
use function Flow\Azure\SDK\DSL\azure_url_factory;

$sdk = azure_blob_service(
    azure_blob_service_config($account, $container),
    azure_shared_key_authorization_factory($account, $accountKey),
    Psr18ClientDiscovery::find(),
    azure_http_factory(Psr17FactoryDiscovery::findRequestFactory(), Psr17FactoryDiscovery::findStreamFactory()),
    azure_url_factory(),
    $logger,
);
```

### mv()

```php
<?php

use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem_options;

$azure = azure_filesystem(
    [
        'account_name' => $_ENV['AZURE_ACCOUNT'],
        'container' => $_ENV['AZURE_CONTAINER'],
        'shared_key' => $_ENV['AZURE_SHARED_KEY'],
    ],
    azure_filesystem_options()->withCopyPartSize(64 * 1024 * 1024),
);
```

`mv()` sends one synchronous Copy Blob up to `copy_part_size` (default 256 MiB, 1 byte to 256 MiB). Above it, `mv()`
copies ranged blocks with Put Block From URL and commits them with one Put Block List. The commit carries the content
headers and the metadata of the source. The source is deleted only after the copy succeeded. Blob tags are not
carried. The source is signed with a SAS, so the service must use Shared Key authorization.

## Usage with Flow

To use the Azure Blob filesystem with Flow, give it to the source or sink that reads or writes the
`azure-blob://` path. Keep one instance, and give the same instance to both sides.

The mount protocol is the URI scheme that the filesystem serves. The default is `'azure-blob'`. For paths with a
different scheme, override it with the third argument
(`azure_filesystem($blobService, $options, protocol: 'warehouse')`).

```php
$azure = azure_filesystem(
    azure_blob_service(
        azure_blob_service_config(
            $_ENV['AZURE_ACCOUNT'],
            $_ENV['AZURE_CONTAINER']
        ),
        azure_shared_key_authorization_factory(
            $_ENV['AZURE_ACCOUNT'],
            $_ENV['AZURE_ACCOUNT_KEY']
        ),
    )
);

data_frame()
    ->read(from_csv(path('azure-blob://test.csv'), filesystem: $azure))
    ->write(to_parquet(path('azure-blob://test.parquet'), filesystem: $azure))
    ->run();
```

If the filesystem of a source or sink does not serve its path, the construction throws. The message names the
`filesystem:` argument.

The `FileStatus` values from `list()` and `status()` carry `size` and `lastModifiedAt`. `size` comes from the
`Content-Length` header / listing property. `lastModifiedAt` comes from the `Last-Modified` header, parsed as RFC 7231. Both come directly from the
Azure response. The CLI commands `flow:filesystem:ls` and `flow:filesystem:stat` print them without an extra
stream.

`create()` commits the blob with `If-None-Match: *`. For an existing blob, it therefore throws
`FileExistsException`. A blob written in blocks stays invisible until `close()`.
