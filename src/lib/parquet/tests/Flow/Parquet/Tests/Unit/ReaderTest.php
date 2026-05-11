<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit;

use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path_real;

final class ReaderTest extends TestCase
{
    public function test_arrow_factory_creates_reader_with_arrow_engine(): void
    {
        if (!\extension_loaded('arrow')) {
            $this->expectException(RuntimeException::class);
            Reader::arrow();

            return;
        }

        $reader = Reader::arrow();

        $reflection = new \ReflectionClass($reader);
        $engine = $reflection->getProperty('engine')->getValue($reader);

        static::assertInstanceOf(ArrowParquetEngine::class, $engine);
    }

    public function test_php_factory_creates_reader_with_php_engine(): void
    {
        $reader = Reader::php();

        $reflection = new \ReflectionClass($reader);
        $engine = $reflection->getProperty('engine')->getValue($reader);

        static::assertInstanceOf(PhpParquetEngine::class, $engine);
    }

    public function test_read_returns_parquet_file(): void
    {
        $reader = Reader::php();

        $parquetFile = $reader->read(__DIR__ . '/../Integration/IO/Fixtures/primitives.parquet');

        static::assertGreaterThan(0, $parquetFile->metadata()->rowsNumber());
    }

    public function test_read_stream_returns_parquet_file(): void
    {
        $reader = Reader::php();
        $stream = NativeLocalSourceStream::open(path_real(__DIR__ . '/../Integration/IO/Fixtures/primitives.parquet'));

        $parquetFile = $reader->readStream($stream);

        static::assertGreaterThan(0, $parquetFile->metadata()->rowsNumber());
    }
}
