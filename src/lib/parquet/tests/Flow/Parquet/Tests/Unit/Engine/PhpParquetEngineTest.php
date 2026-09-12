<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\PhpParquetFileWriter;
use Flow\Parquet\Tests\Mother\ParquetFileWriterMother;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class PhpParquetEngineTest extends TestCase
{
    public function test_open_for_write_returns_a_new_writer_for_every_call(): void
    {
        $engine = new PhpParquetEngine();
        $memory = memory_filesystem();

        $first = ParquetFileWriterMother::open($engine, $memory->writeTo(path('memory://first.parquet')));
        $second = ParquetFileWriterMother::open($engine, $memory->writeTo(path('memory://second.parquet')));

        static::assertInstanceOf(PhpParquetFileWriter::class, $first);
        static::assertInstanceOf(PhpParquetFileWriter::class, $second);
        static::assertNotSame($first, $second);
    }
}
