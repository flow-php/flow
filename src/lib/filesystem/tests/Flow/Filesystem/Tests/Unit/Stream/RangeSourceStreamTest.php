<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Stream;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Stream\RangeSourceStream;
use Flow\Filesystem\Stream\StringSourceStream;
use Flow\Filesystem\Tests\Context\ReadLinesContext;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function strlen;

final class RangeSourceStreamTest extends TestCase
{
    public function test_a_negative_start_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Range start must not be negative, given: -1');

        new RangeSourceStream(new StringSourceStream(path('memory://range.bin'), 'abc'), -1, 2);
    }

    public function test_an_end_before_the_start_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Range end must not precede its start, given: 2..1');

        new RangeSourceStream(new StringSourceStream(path('memory://range.bin'), 'abc'), 2, 1);
    }

    public function test_an_empty_window_reads_nothing(): void
    {
        $range = new RangeSourceStream(new StringSourceStream(path('memory://range.bin'), 'abcdef'), 3, 3);

        static::assertSame('', $range->content());
        static::assertSame(0, $range->size());
        static::assertSame([], iterator_to_array($range->iterate(2)));
        static::assertSame([], iterator_to_array($range->readLines()));
        static::assertSame('', $range->read(10, 0));
    }

    public function test_content_is_the_window(): void
    {
        static::assertSame(
            'cde',
            (new RangeSourceStream(new StringSourceStream(path('memory://range.bin'), 'abcdefg'), 2, 5))->content(),
        );
    }

    public function test_exposes_its_window_and_inner_stream(): void
    {
        $inner = new StringSourceStream(path('memory://range.bin'), 'abcdefg');
        $range = new RangeSourceStream($inner, 2, 5);

        static::assertSame($inner, $range->stream());
        static::assertSame(2, $range->start());
        static::assertSame(5, $range->end());
        static::assertSame($range->start() + $range->size(), $range->end());
        static::assertSame(3, (new RangeSourceStream($inner, 3, 3))->end());
    }

    public function test_it_delegates_path_and_open_state(): void
    {
        $range = new RangeSourceStream(new StringSourceStream(path('memory://range.bin'), 'abc'), 0, 3);

        static::assertSame('memory://range.bin', $range->path()->uri());
        static::assertTrue($range->isOpen());
    }

    public function test_iterate_stops_at_the_window_end(): void
    {
        static::assertSame(
            ['cd', 'ef', 'g'],
            iterator_to_array(
                (new RangeSourceStream(
                    new StringSourceStream(path('memory://range.bin'), 'abcdefghij'),
                    2,
                    7,
                ))->iterate(2),
                false,
            ),
        );
    }

    public function test_iterate_stops_when_the_inner_stream_ends_early(): void
    {
        static::assertSame(
            ['cd'],
            iterator_to_array(
                (new RangeSourceStream(new StringSourceStream(path('memory://range.bin'), 'abcd'), 2, 10))->iterate(4),
                false,
            ),
        );
    }

    /**
     * @param int<1, max> $length
     */
    #[TestWith([0, 2, 'cd'])]
    #[TestWith([2, 10, 'ef'])]
    #[TestWith([4, 2, ''])]
    #[TestWith([-2, 5, 'ef'])]
    #[TestWith([-10, 2, 'cd'])]
    public function test_read_is_relative_to_the_window(int $offset, int $length, string $expected): void
    {
        static::assertSame($expected, (new RangeSourceStream(
            new StringSourceStream(path('memory://range.bin'), 'abcdefgh'),
            2,
            6,
        ))->read($length, $offset));
    }

    /**
     * @param non-empty-string $separator
     * @param null|int<1, max> $length
     * @param list<string> $expected
     */
    #[DataProviderExternal(ReadLinesContext::class, 'cases')]
    public function test_read_lines_conforms_to_the_source_stream_contract(
        string $content,
        string $separator,
        ?int $length,
        array $expected,
    ): void {
        $padded = '>>' . $content . '<<';

        static::assertSame($expected, iterator_to_array(
            (new RangeSourceStream(
                new StringSourceStream(path('memory://range.bin'), $padded),
                2,
                2 + strlen($content),
            ))->readLines($separator, $length),
            false,
        ));
    }

    public function test_size_is_the_window_length(): void
    {
        static::assertSame(
            3,
            (new RangeSourceStream(new StringSourceStream(path('memory://range.bin'), 'abcdefg'), 2, 5))->size(),
        );
    }
}
