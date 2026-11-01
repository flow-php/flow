---
package: flow-php/filesystem-async-aws-bridge
---

# Filesystem Async AWS

[PACKAGE_NAV]

With the Filesystem Async AWS Bridge, you can use S3 as a filesystem in your application. It uses the
[Async AWS SDK](https://github.com/async-aws/s3) for this.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/filesystem-async-aws-bridge.md).

```php
use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_filesystem;

$aws = aws_s3_filesystem($_ENV['AWS_S3_BUCKET'], [
    'region' => $_ENV['AWS_S3_REGION'],
    'accessKeyId' => $_ENV['AWS_S3_KEY'],
    'accessKeySecret' => $_ENV['AWS_S3_SECRET'],
]);

$fstab = fstab($aws);
```

The mount protocol is the URI scheme of the filesystem in the `FilesystemTable`. The default is `'aws-s3'`. To
override it, give a fourth argument, for example
`aws_s3_filesystem($bucket, $client, options: new Options(), protocol: 'warehouse')`. Use this to mount the same
bucket twice under different names, or to pick a scheme with more meaning for your application.

## Usage with Flow

To use the AWS S3 filesystem with Flow, give it to the source or sink that reads or writes the
`aws-s3://` path. Keep one instance, and give the same instance to both sides.

```php
$s3 = aws_s3_filesystem(
    $_ENV['AWS_S3_BUCKET'],
    aws_s3_client([
        'region' => $_ENV['AWS_S3_REGION'],
        'accessKeyId' => $_ENV['AWS_S3_KEY'],
        'accessKeySecret' => $_ENV['AWS_S3_SECRET'],
    ])
);

data_frame()
    ->read(from_csv(path('aws-s3://test.csv'), filesystem: $s3))
    ->write(to_parquet(path('aws-s3://test.parquet'), filesystem: $s3))
    ->run();
```

If the filesystem of a source or sink does not serve its path, the construction throws. The message names the
`filesystem:` argument.

The `FileStatus` values from `list()` and `status()` carry `size` (from S3 `Size` / `ContentLength`)
and `lastModifiedAt` (from `LastModified`). Both come directly from the S3 response. The CLI commands
`flow:filesystem:ls` and `flow:filesystem:stat` print them without an extra HEAD call.

`create()` commits the multipart upload with `If-None-Match: *`. For an existing key, it therefore throws
`FileExistsException`. A conflicting write can abort the upload. `create()` then throws `RuntimeException`, and you
call `create()` again. S3-compatible stores that ignore conditional writes do not enforce it.
