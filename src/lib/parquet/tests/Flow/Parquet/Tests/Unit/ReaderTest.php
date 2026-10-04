<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit;

use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

use function Flow\Filesystem\DSL\path_real;

final class ReaderTest extends TestCase
{
    public function test_the_default_engine_is_adaptive(): void
    {
        $reader = new Reader();

        static::assertInstanceOf(
            AdaptiveParquetEngine::class,
            (new ReflectionClass($reader))
                ->getProperty('engine')
                ->getValue($reader),
        );
    }

    public function test_php_factory_creates_reader_with_php_engine(): void
    {
        $reader = Reader::php();

        static::assertInstanceOf(
            PhpParquetEngine::class,
            (new ReflectionClass($reader))
                ->getProperty('engine')
                ->getValue($reader),
        );
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
