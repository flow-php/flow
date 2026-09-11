<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Stream;

use Flow\Filesystem\Stream\StringSourceStream;
use Flow\Filesystem\Tests\Context\ReadLinesContext;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class StringSourceStreamTest extends TestCase
{
    public function test_content_returns_the_whole_payload(): void
    {
        static::assertSame('foobar', (new StringSourceStream(path('memory://value.bin'), 'foobar'))->content());
    }

    public function test_empty_content_is_allowed(): void
    {
        $stream = new StringSourceStream(path('memory://empty.bin'), '');

        static::assertSame('', $stream->content());
        static::assertSame(0, $stream->size());
        static::assertSame([], iterator_to_array($stream->iterate(4)));
    }

    public function test_size_is_the_byte_length(): void
    {
        static::assertSame(6, (new StringSourceStream(path('memory://size.bin'), 'foobar'))->size());
    }

    public function test_read_returns_a_byte_range(): void
    {
        $stream = new StringSourceStream(path('memory://read.bin'), 'foobar');

        static::assertSame('foo', $stream->read(3, 0));
        static::assertSame('bar', $stream->read(3, 3));
    }

    public function test_iterate_yields_fixed_size_chunks(): void
    {
        $stream = new StringSourceStream(path('memory://iterate.bin'), 'foobar');

        static::assertSame(['foo', 'bar'], iterator_to_array($stream->iterate(3)));
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
        static::assertSame(
            $expected,
            iterator_to_array((new StringSourceStream(path('memory://lines.bin'), $content))->readLines(
                $separator,
                $length,
            )),
        );
    }

    public function test_is_open_is_always_true(): void
    {
        static::assertTrue((new StringSourceStream(path('memory://open.bin'), 'x'))->isOpen());
    }

    public function test_close_is_a_noop_and_keeps_content_readable(): void
    {
        $stream = new StringSourceStream(path('memory://close.bin'), 'kept');
        $stream->close();

        static::assertSame('kept', $stream->content());
    }

    public function test_path_returns_the_construction_path(): void
    {
        static::assertSame(
            'memory://value.bin',
            (new StringSourceStream(path('memory://value.bin'), 'x'))->path()->uri(),
        );
    }
}
