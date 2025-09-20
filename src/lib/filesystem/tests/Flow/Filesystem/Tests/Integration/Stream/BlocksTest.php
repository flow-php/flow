<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\Stream;

use Flow\Filesystem\SizeUnits;
use Flow\Filesystem\Stream\Blocks;
use PHPUnit\Framework\TestCase;

final class BlocksTest extends TestCase
{
    public function test_basic_blocks_operations() : void
    {
        $blocks = new Blocks($blockSize = SizeUnits::kbToBytes(10));

        $testContent = 'Basic test content for blocks functionality';
        $blocks->append($testContent);

        self::assertSame(\strlen($testContent), $blocks->size());
        self::assertGreaterThan(0, \count($blocks->all()));
    }

    public function test_blocks_with_multiple_appends() : void
    {
        $blocks = new Blocks($blockSize = SizeUnits::kbToBytes(1));

        $content1 = \str_repeat('a', 500);
        $content2 = \str_repeat('b', 600);

        $blocks->append($content1);
        $blocks->append($content2);

        self::assertSame(\strlen($content1) + \strlen($content2), $blocks->size());
        self::assertGreaterThan(1, \count($blocks->all()));
    }
}
