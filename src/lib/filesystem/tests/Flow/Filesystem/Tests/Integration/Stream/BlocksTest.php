<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\Stream;

use Flow\Filesystem\SizeUnits;
use Flow\Filesystem\Stream\Blocks;
use PHPUnit\Framework\TestCase;

final class BlocksTest extends TestCase
{
    public function test_moving_resource_to_blocks() : void
    {
        $blocks = new Blocks($blockSize = SizeUnits::kbToBytes(10));

        $file = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        $fileSize = \filesize(__DIR__ . '/Fixtures/orders.csv');

        $blocks->fromResource($file);

        self::assertSame($fileSize, $blocks->size());
        self::assertSame((int) ceil($fileSize / $blockSize), \count($blocks->all()));
    }

    public function test_moving_resource_to_existing_blocks() : void
    {
        $blocks = new Blocks($blockSize = SizeUnits::kbToBytes(10));

        $file = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        $fileSize = \filesize(__DIR__ . '/Fixtures/orders.csv');

        $blocks->append(\str_repeat('a', 100));
        $blocks->fromResource($file);

        self::assertSame($fileSize + 100, $blocks->size());
        self::assertCount((int) ceil($fileSize / $blockSize), $blocks->all());
    }
}
