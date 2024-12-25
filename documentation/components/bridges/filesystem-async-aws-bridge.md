# Filesystem Async AWS

- [⬅️️ Back](../../introduction.md)

The Filesystem Azure Bridge is a bridge that allows you to use the S3 as a filesystem in your application 
through [Async AWS SDK](https://github.com/async-aws/s3).

```bash
composer require flow-php/filesystem-async-aws-bridge
```

```code
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