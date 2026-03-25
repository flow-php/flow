<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;

final class PhpParquetEngineTest extends TestCase
{
    public function test_close_write_throws_when_writer_not_open() : void
    {
        $engine = new PhpParquetEngine();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $engine->closeWrite();
    }

    public function test_write_batch_throws_when_writer_not_open() : void
    {
        $engine = new PhpParquetEngine();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $engine->writeBatch([['col' => 'value']]);
    }

    public function test_write_row_throws_when_writer_not_open() : void
    {
        $engine = new PhpParquetEngine();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $engine->writeRow(['col' => 'value']);
    }
}
