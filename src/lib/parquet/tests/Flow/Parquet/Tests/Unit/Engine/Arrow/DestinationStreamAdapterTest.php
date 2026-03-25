<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine\Arrow;

use Flow\Filesystem\Local\Memory\MemoryStream;
use Flow\Filesystem\Path;
use Flow\Parquet\Engine\Arrow\DestinationStreamAdapter;
use PHPUnit\Framework\TestCase;

final class DestinationStreamAdapterTest extends TestCase
{
    public function test_append_delegates_to_destination_stream() : void
    {
        $handle = \fopen('php://memory', 'r+b');
        $stream = new MemoryStream($handle, Path::realpath('/tmp/test'));

        $adapter = new DestinationStreamAdapter($stream);
        $adapter->append('hello ');
        $adapter->append('world');

        self::assertSame('hello world', $stream->content());
    }

    public function test_append_returns_self() : void
    {
        $handle = \fopen('php://memory', 'r+b');
        $stream = new MemoryStream($handle, Path::realpath('/tmp/test'));

        $adapter = new DestinationStreamAdapter($stream);

        self::assertSame($adapter, $adapter->append('data'));
    }
}
