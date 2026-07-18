<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Stream;

use Flow\Filesystem\Stream\StringDestinationStream;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function fopen;
use function fwrite;
use function rewind;

final class StringDestinationStreamTest extends TestCase
{
    public function test_append_accumulates_bytes(): void
    {
        $stream = new StringDestinationStream(path('memory://value.bin'));

        $stream->append('foo')->append('bar');

        static::assertSame('foobar', $stream->content());
    }

    public function test_content_of_empty_stream_is_empty_string(): void
    {
        static::assertSame('', (new StringDestinationStream(path('memory://empty.bin')))->content());
    }

    public function test_from_resource_appends_resource_contents(): void
    {
        $resource = fopen('php://memory', 'r+b');
        fwrite($resource, 'from-resource');
        rewind($resource);

        $stream = new StringDestinationStream(path('memory://res.bin'));
        $stream->append('head-')->fromResource($resource);

        static::assertSame('head-from-resource', $stream->content());
    }

    public function test_is_open_is_always_true(): void
    {
        static::assertTrue((new StringDestinationStream(path('memory://open.bin')))->isOpen());
    }

    public function test_close_is_a_noop_and_keeps_content_readable(): void
    {
        $stream = new StringDestinationStream(path('memory://close.bin'));
        $stream->append('kept');
        $stream->close();

        static::assertSame('kept', $stream->content());
    }

    public function test_path_returns_the_construction_path(): void
    {
        static::assertSame(
            'memory://value.bin',
            (new StringDestinationStream(path('memory://value.bin')))->path()->uri(),
        );
    }
}
