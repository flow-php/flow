<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\DSL;

use AsyncAws\Core\Configuration;
use AsyncAws\S3\S3Client;
use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3Filesystem;
use Flow\Filesystem\Bridge\AsyncAWS\Options;
use Flow\Filesystem\Mount;

/**
 * @param array<Configuration::OPTION_*, null|string> $configuration - for details please see https://async-aws.com/clients/s3.html
 */
#[DocumentationDSL(module: Module::S3_FILESYSTEM, type: Type::HELPER)]
function aws_s3_client(array $configuration): S3Client
{
    return new S3Client($configuration);
}

#[DocumentationDSL(module: Module::S3_FILESYSTEM, type: Type::HELPER)]
function aws_s3_filesystem(
    string $bucket,
    S3Client $s3Client,
    Options $options = new Options(),
    string $protocol = 'aws-s3',
): AsyncAWSS3Filesystem {
    return new AsyncAWSS3Filesystem(new Mount($protocol), $bucket, $s3Client, $options);
}
