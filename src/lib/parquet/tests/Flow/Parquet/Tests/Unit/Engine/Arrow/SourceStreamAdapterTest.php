<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine\Arrow;

use Flow\Filesystem\Local\Memory\MemoryStream;
use Flow\Filesystem\Path;
use Flow\Parquet\Engine\Arrow\SourceStreamAdapter;
use PHPUnit\Framework\TestCase;

use function fopen;
use function fwrite;

final class SourceStreamAdapterTest extends TestCase
{
    public function test_read_delegates_to_source_stream(): void
    {
        $handle = fopen('php://memory', 'r+b');
        fwrite($handle, 'hello world');
        $stream = new MemoryStream($handle, Path::realpath('/tmp/test'));

        $adapter = new SourceStreamAdapter($stream);

        static::assertSame('hello', $adapter->read(5, 0));
        static::assertSame('world', $adapter->read(5, 6));
        static::assertSame(' ', $adapter->read(1, 5));
    }

    public function test_size_delegates_to_source_stream(): void
    {
        $handle = fopen('php://memory', 'r+b');
        fwrite($handle, 'hello world');
        $stream = new MemoryStream($handle, Path::realpath('/tmp/test'));

        $adapter = new SourceStreamAdapter($stream);

        static::assertSame(11, $adapter->size());
    }
}
