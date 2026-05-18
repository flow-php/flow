<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Stream;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockLifecycle;
use Flow\Filesystem\Stream\Blocks;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function str_repeat;

final class BlocksTest extends TestCase
{
    /**
     * @return \Generator<string, array{int}>
     */
    public static function nonPositiveBlockSizeProvider(): Generator
    {
        yield 'zero' => [0];
        yield 'negative' => [-1];
    }

    #[DataProvider('nonPositiveBlockSizeProvider')]
    public function test_constructor_rejects_non_positive_block_size(int $blockSize): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Block size must be greater than 0');

        /** @mago-ignore analysis:possibly-invalid-argument */
        new Blocks($blockSize);
    }

    public function test_writing_to_blocks(): void
    {
        $blockLifecycle = $this->createMock(BlockLifecycle::class);
        $blockLifecycle->expects(self::exactly(4))->method('filled');

        $blocks = new Blocks(100, new NativeLocalFileBlocksFactory(), $blockLifecycle);

        $blocks->append(str_repeat('a', 100)); // block 1
        $blocks->append(str_repeat('a', 150)); // block 2 and 3
        $blocks->append(str_repeat('a', 70)); // block 3 and 4
        $blocks->append(str_repeat('a', 90)); // block 5

        static::assertSame(410, $blocks->size());
        static::assertSame(90, $blocks->block()->spaceLeft());
        static::assertSame(10, $blocks->block()->size());
        static::assertCount(5, $blocks->all());
    }
}
