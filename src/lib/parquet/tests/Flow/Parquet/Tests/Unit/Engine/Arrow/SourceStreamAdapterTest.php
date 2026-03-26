<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine\Arrow;

use Flow\Filesystem\Local\Memory\MemoryStream;
use Flow\Filesystem\Path;
use Flow\Parquet\Engine\Arrow\SourceStreamAdapter;
use PHPUnit\Framework\TestCase;

final class SourceStreamAdapterTest extends TestCase
{
    public function test_read_delegates_to_source_stream() : void
    {
        $handle = \fopen('php://memory', 'r+b');
        \fwrite($handle, 'hello world');
        $stream = new MemoryStream($handle, Path::realpath('/tmp/test'));

        $adapter = new SourceStreamAdapter($stream);

        self::assertSame('hello', $adapter->read(5, 0));
        self::assertSame('world', $adapter->read(5, 6));
        self::assertSame(' ', $adapter->read(1, 5));
    }

    public function test_size_delegates_to_source_stream() : void
    {
        $handle = \fopen('php://memory', 'r+b');
        \fwrite($handle, 'hello world');
        $stream = new MemoryStream($handle, Path::realpath('/tmp/test'));

        $adapter = new SourceStreamAdapter($stream);

        self::assertSame(11, $adapter->size());
    }
}
