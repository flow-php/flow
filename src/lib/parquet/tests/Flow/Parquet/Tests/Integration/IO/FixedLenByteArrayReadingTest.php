<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Binary\Bytes;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, Repetition};
use Flow\Parquet\{Reader, Writer};
use PHPUnit\Framework\TestCase;

final class FixedLenByteArrayReadingTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    public function test_reading_and_writing_fixed_len_byte_array_without_logical_type() : void
    {
        $path = __DIR__ . '/var/parquet_fixed_len_byte_array_' . \uniqid() . '.parquet';

        $schema = Schema::with(
            FlatColumn::fixedSizeByteArray('raw_bytes', 8, Repetition::REQUIRED),
        );

        $writer = new Writer();

        $bytes1 = \str_repeat('A', 8);
        $bytes2 = \str_repeat('B', 8);
        $bytes3 = \str_repeat('C', 8);

        $inputData = [
            ['raw_bytes' => $bytes1],
            ['raw_bytes' => $bytes2],
            ['raw_bytes' => $bytes3],
        ];

        $writer->write($path, $schema, $inputData);

        $reader = new Reader();
        $parquetFile = $reader->read($path);

        $rows = \iterator_to_array($parquetFile->values());

        self::assertCount(3, $rows);
        self::assertInstanceOf(Bytes::class, $rows[0]['raw_bytes']);
        self::assertInstanceOf(Bytes::class, $rows[1]['raw_bytes']);
        self::assertInstanceOf(Bytes::class, $rows[2]['raw_bytes']);
        self::assertSame($bytes1, $rows[0]['raw_bytes']->toString());
        self::assertSame($bytes2, $rows[1]['raw_bytes']->toString());
        self::assertSame($bytes3, $rows[2]['raw_bytes']->toString());

        \unlink($path);
    }

    public function test_reading_fixed_len_byte_array_returns_bytes_object() : void
    {
        $path = __DIR__ . '/var/parquet_fixed_len_byte_array_bytes_' . \uniqid() . '.parquet';

        $schema = Schema::with(
            FlatColumn::fixedSizeByteArray('data', 16, Repetition::REQUIRED),
        );

        $writer = new Writer();

        $data = \random_bytes(16);
        $inputData = [['data' => $data]];

        $writer->write($path, $schema, $inputData);

        $reader = new Reader();
        $parquetFile = $reader->read($path);

        $rows = \iterator_to_array($parquetFile->values());

        self::assertCount(1, $rows);
        self::assertInstanceOf(Bytes::class, $rows[0]['data']);
        self::assertSame($data, $rows[0]['data']->toString());

        \unlink($path);
    }
}
