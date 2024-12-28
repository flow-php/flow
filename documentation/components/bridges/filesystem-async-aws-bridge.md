# Filesystem Async AWS

- [⬅️️ Back](../../introduction.md)

The Filesystem Azure Bridge is a bridge that allows you to use the S3 as a filesystem in your application 
through [Async AWS SDK](https://github.com/async-aws/s3).

```bash
composer require flow-php/filesystem-async-aws-bridge
```

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

## Usage with Flow

To use the AWS S3 filesystem with Flow, you need to mount the filesystem to the configuration.
This operation will mount the S3 filesystem to fstab instance available in the DataFrame runtime.

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