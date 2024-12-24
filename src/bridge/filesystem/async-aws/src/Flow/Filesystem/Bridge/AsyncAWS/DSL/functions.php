<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\DSL;

use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\{AsyncAWSS3Filesystem, Options};
use Flow\Filesystem\Path;

/**
 * @param array<string, mixed> $configuration - for details please see https://async-aws.com/clients/s3.html
 */
function aws_s3_client(array $configuration) : S3Client
{
    return new S3Client($configuration);
}

function aws_s3_path(string $bucket, string $path) : Path
{
    $bucket = \trim($bucket, DIRECTORY_SEPARATOR);
    $path = \ltrim($path, DIRECTORY_SEPARATOR);

    return \Flow\Filesystem\DSL\path("aws-s3://{$bucket}/{$path}");
}

function aws_s3_filesystem(S3Client $s3Client, Options $options = new Options()) : AsyncAWSS3Filesystem
{
    return new AsyncAWSS3Filesystem($s3Client, $options);
}
