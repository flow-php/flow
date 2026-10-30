<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Stream;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Stream\BlockCachedSourceStream;
use Flow\Filesystem\Stream\StringSourceStream;
use Flow\Filesystem\Tests\Double\ReadRecordingSourceStream;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class BlockCachedSourceStreamTest extends TestCase
{
    public function test_a_block_size_below_one_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Block size must be greater than 0, given: 0');

        new BlockCachedSourceStream(new StringSourceStream(path('memory://cache.bin'), 'abc'), 0);
    }

    public function test_a_negative_offset_bypasses_the_cache(): void
    {
        $inner = new ReadRecordingSourceStream(new StringSourceStream(path('memory://cache.bin'), 'abcdefgh'));

        static::assertSame('gh', (new BlockCachedSourceStream($inner, 4))->read(2, -2));
        static::assertSame([[2, -2]], $inner->reads);
    }

    public function test_forward_reads_inside_two_blocks_fetch_each_block_once(): void
    {
        $inner = new ReadRecordingSourceStream(new StringSourceStream(path('memory://cache.bin'), 'abcdefgh'));
        $cached = new BlockCachedSourceStream($inner, 4);

        static::assertSame('bcd', $cached->read(3, 1));
        static::assertSame('cdef', $cached->read(4, 2));
        static::assertSame('ef', $cached->read(2, 4));
        static::assertSame([[4, 0], [4, 4]], $inner->reads);
    }

    public function test_only_the_last_two_blocks_are_kept(): void
    {
        $inner = new ReadRecordingSourceStream(new StringSourceStream(path('memory://cache.bin'), 'abcdefghijkl'));
        $cached = new BlockCachedSourceStream($inner, 4);

        $cached->read(1, 0);
        $cached->read(1, 4);
        $cached->read(1, 8);
        $cached->read(1, 0);

        static::assertSame([[4, 0], [4, 4], [4, 8], [4, 0]], $inner->reads);
    }

    /**
     * @param int<1, max> $length
     */
    #[TestWith([0, 20, 'abcdefgh'])]
    #[TestWith([6, 20, 'gh'])]
    #[TestWith([8, 3, ''])]
    #[TestWith([20, 3, ''])]
    public function test_a_read_stops_at_the_end_of_the_stream(int $offset, int $length, string $expected): void
    {
        static::assertSame($expected, (new BlockCachedSourceStream(
            new StringSourceStream(path('memory://cache.bin'), 'abcdefgh'),
            3,
        ))->read($length, $offset));
    }

    public function test_everything_but_read_is_the_inner_stream(): void
    {
        $cached = new BlockCachedSourceStream(new StringSourceStream(path('memory://cache.bin'), "ab\ncd"), 2);

        static::assertSame("ab\ncd", $cached->content());
        static::assertSame(['ab', "\nc", 'd'], iterator_to_array($cached->iterate(2), false));
        static::assertSame(['ab', 'cd'], iterator_to_array($cached->readLines(), false));
        static::assertSame(5, $cached->size());
        static::assertSame('memory://cache.bin', $cached->path()->uri());
        static::assertTrue($cached->isOpen());
    }
}
