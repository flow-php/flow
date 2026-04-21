<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\DSL;

use AsyncAws\S3\S3Client;
use Flow\ETL\Attribute\{DocumentationDSL, Module, Type};
use Flow\Filesystem\Bridge\AsyncAWS\{AsyncAWSS3Filesystem, Options};
use Flow\Filesystem\Mount;

/**
 * @param array<string, mixed> $configuration - for details please see https://async-aws.com/clients/s3.html
 */
#[DocumentationDSL(module: Module::S3_FILESYSTEM, type: Type::HELPER)]
function aws_s3_client(array $configuration) : S3Client
{
    /** @phpstan-ignore-next-line */
    return new S3Client($configuration);
}

#[DocumentationDSL(module: Module::S3_FILESYSTEM, type: Type::HELPER)]
function aws_s3_filesystem(string $bucket, S3Client $s3Client, Options $options = new Options(), string $protocol = 'aws-s3') : AsyncAWSS3Filesystem
{
    return new AsyncAWSS3Filesystem(new Mount($protocol), $bucket, $s3Client, $options);
}
