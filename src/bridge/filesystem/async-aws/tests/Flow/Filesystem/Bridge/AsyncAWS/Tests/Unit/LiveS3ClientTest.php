<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit;

use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\LiveS3Client;
use PHPUnit\Framework\TestCase;

final class LiveS3ClientTest extends TestCase
{
    public function test_a_live_client_has_no_configuration(): void
    {
        $client = $this->createStub(S3Client::class);
        $source = new LiveS3Client($client);

        static::assertSame($client, $source->client());
        static::assertNull($source->normalize());
    }
}
