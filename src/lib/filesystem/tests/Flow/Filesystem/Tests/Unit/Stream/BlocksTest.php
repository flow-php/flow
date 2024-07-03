<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Stream;

use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\{BlockLifecycle, Blocks};
use PHPUnit\Framework\TestCase;

final class BlocksTest extends TestCase
{
    public function test_writing_to_blocks() : void
    {
        $blockLifecycle = $this->createMock(BlockLifecycle::class);
        $blockLifecycle->expects(self::exactly(4))
            ->method('filled');

        $blocks = new Blocks(
            100,
            new NativeLocalFileBlocksFactory(),
            $blockLifecycle
        );

        $blocks->append(\str_repeat('a', 100)); // block 1
        $blocks->append(\str_repeat('a', 150)); // block 2 and 3
        $blocks->append(\str_repeat('a', 70)); // block 3 and 4
        $blocks->append(\str_repeat('a', 90)); // block 5

        self::assertSame(410, $blocks->size());
        self::assertSame(90, $blocks->block()->spaceLeft());
        self::assertSame(10, $blocks->block()->size());
        self::assertCount(5, $blocks->all());
    }
}
