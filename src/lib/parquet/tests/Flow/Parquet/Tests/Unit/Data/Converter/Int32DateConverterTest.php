<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data\Converter;

use Flow\Parquet\Data\Converter\Int32DateConverter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\{ConvertedType, FlatColumn, PhysicalType};
use PHPUnit\Framework\TestCase;

final class Int32DateConverterTest extends TestCase
{
    public function test_converting_dates() : void
    {
        $date = new \DateTimeImmutable('2021-01-01 00:00:00 UTC');

        $converter = new Int32DateConverter();

        self::assertEquals(
            $date,
            $converter->fromParquetType($converter->toParquetType($date))
        );
    }

    public function test_converting_int32_with_deprecated_converted_type() : void
    {
        $converter = new Int32DateConverter();

        self::assertTrue(
            $converter->isFor(
                new FlatColumn(
                    'date',
                    PhysicalType::INT32,
                    ConvertedType::DATE,
                    null,
                ),
                Options::default()
            )
        );
    }
}
