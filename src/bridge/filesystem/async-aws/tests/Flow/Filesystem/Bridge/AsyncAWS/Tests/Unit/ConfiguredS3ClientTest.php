<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit;

use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\ConfiguredS3Client;
use PHPUnit\Framework\TestCase;

final class ConfiguredS3ClientTest extends TestCase
{
    public function test_the_configuration_is_kept_verbatim(): void
    {
        $configuration = [
            'region' => 'eu-west-1',
            'accessKeyId' => 'k',
            'accessKeySecret' => 's',
            'endpoint' => 'http://s3.test',
        ];

        static::assertSame($configuration, (new ConfiguredS3Client($configuration))->normalize());
    }

    public function test_the_client_is_built_once(): void
    {
        $source = new ConfiguredS3Client(['region' => 'eu-west-1', 'accessKeyId' => 'k', 'accessKeySecret' => 's']);

        static::assertInstanceOf(S3Client::class, $source->client());
        static::assertSame($source->client(), $source->client());
    }
}
