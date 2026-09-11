<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\DataProvider;

use function iterator_to_array;
use function random_bytes;
use function str_repeat;

class FixedLenByteArrayReadingTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_reading_and_writing_fixed_len_byte_array_without_logical_type(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::fixedSizeByteArray('raw_bytes', 8, Repetition::REQUIRED));

        $writer = new Writer(engine: $engine);

        $bytes1 = str_repeat('A', 8);
        $bytes2 = str_repeat('B', 8);
        $bytes3 = str_repeat('C', 8);

        $inputData = [
            ['raw_bytes' => $bytes1],
            ['raw_bytes' => $bytes2],
            ['raw_bytes' => $bytes3],
        ];

        $writer->write($path, $schema, $inputData);

        $reader = new Reader(engine: $engine);
        $parquetFile = $reader->read($path);

        $rows = iterator_to_array($parquetFile->values());

        static::assertCount(3, $rows);
        static::assertIsString($rows[0]['raw_bytes']);
        static::assertIsString($rows[1]['raw_bytes']);
        static::assertIsString($rows[2]['raw_bytes']);
        static::assertSame($bytes1, $rows[0]['raw_bytes']);
        static::assertSame($bytes2, $rows[1]['raw_bytes']);
        static::assertSame($bytes3, $rows[2]['raw_bytes']);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_fixed_len_byte_array_returns_raw_string(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::fixedSizeByteArray('data', 16, Repetition::REQUIRED));

        $writer = new Writer(engine: $engine);

        $data = random_bytes(16);
        $inputData = [['data' => $data]];

        $writer->write($path, $schema, $inputData);

        $reader = new Reader(engine: $engine);
        $parquetFile = $reader->read($path);

        $rows = iterator_to_array($parquetFile->values());

        static::assertCount(1, $rows);
        static::assertIsString($rows[0]['data']);
        static::assertSame($data, $rows[0]['data']);
    }
}
