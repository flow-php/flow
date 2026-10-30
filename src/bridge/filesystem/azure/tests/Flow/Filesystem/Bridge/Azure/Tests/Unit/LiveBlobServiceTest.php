<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Unit;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Filesystem\Bridge\Azure\LiveBlobService;
use PHPUnit\Framework\TestCase;

final class LiveBlobServiceTest extends TestCase
{
    public function test_a_live_service_has_no_configuration(): void
    {
        $service = $this->createStub(BlobServiceInterface::class);
        $source = new LiveBlobService($service);

        static::assertSame($service, $source->blobService());
        static::assertNull($source->normalize());
    }
}
