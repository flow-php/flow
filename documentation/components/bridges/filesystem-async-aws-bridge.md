# Filesystem Async AWS

- [⬅️️ Back](/documentation/introduction.md)
- [📦Packagist](https://packagist.org/packages/flow-php/filesystem-async-aws-bridge)
- [➡️ Installation](/documentation/installation/packages/filesystem-async-aws-bridge.md)
- [🐙GitHub](https://github.com/flow-php/filesystem-async-aws-bridge)
- [📚API Reference](/documentation/api/bridge/filesystem/async-aws)
- [📁Files](/documentation/api/bridge/filesystem/async-aws/indices/files.html)

The Filesystem Async AWS Bridge is a bridge that allows you to use the S3 as a filesystem in your application
through [Async AWS SDK](https://github.com/async-aws/s3).

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/filesystem-async-aws-bridge.md).

```php
use function Flow\Filesystem\Bridge\AsyncAWS\DSL\{aws_s3_client, aws_s3_filesystem};

$aws = aws_s3_filesystem(
    $_ENV['AWS_S3_BUCKET'],
    aws_s3_client([
        'region' => $_ENV['AWS_S3_REGION'],
        'accessKeyId' => $_ENV['AWS_S3_KEY'],
        'accessKeySecret' => $_ENV['AWS_S3_SECRET'],
    ])
);

$fstab = fstab($aws);
```

The **mount protocol** — the URI scheme under which the filesystem is registered in the
`FilesystemTable` — defaults to `'aws-s3'`. Override by passing a fourth argument (e.g.
`aws_s3_filesystem($bucket, $client, options: new Options(), protocol: 'warehouse')`) when you
need to mount the same bucket twice under distinct names or pick a scheme more meaningful to
your application.

## Usage with Flow

To use the AWS S3 filesystem with Flow, you need to mount the filesystem to the configuration.
This operation will mount the S3 filesystem to the fstab instance available in the DataFrame runtime.

```php
$config = config_builder()
    ->mount(
        aws_s3_filesystem(
            $_ENV['AWS_S3_BUCKET'],
            aws_s3_client([
                'region' => $_ENV['AWS_S3_REGION'],
                'accessKeyId' => $_ENV['AWS_S3_KEY'],
                'accessKeySecret' => $_ENV['AWS_S3_SECRET'],
            ])
        )
    );

data_frame($config)
    ->read(from_csv(path('aws-s3://test.csv')))
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
```

`FileStatus` values returned from `list()` and `status()` carry `size` (from S3 `Size` / `ContentLength`)
and `lastModifiedAt` (from `LastModified`) populated directly from the S3 response — no extra HEAD call
is issued when the CLI `flow:filesystem:ls --long` or `flow:filesystem:stat` prints them.
