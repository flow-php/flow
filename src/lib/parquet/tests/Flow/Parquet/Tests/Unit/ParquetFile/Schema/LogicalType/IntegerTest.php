<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema\LogicalType;

use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Integer;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ThriftModel\IntType;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class IntegerTest extends TestCase
{
    #[TestWith([ConvertedType::INT_8, 8, true])]
    #[TestWith([ConvertedType::INT_16, 16, true])]
    #[TestWith([ConvertedType::INT_32, 32, true])]
    #[TestWith([ConvertedType::INT_64, 64, true])]
    #[TestWith([ConvertedType::UINT_8, 8, false])]
    #[TestWith([ConvertedType::UINT_16, 16, false])]
    #[TestWith([ConvertedType::UINT_32, 32, false])]
    #[TestWith([ConvertedType::UINT_64, 64, false])]
    public function test_for_column_reads_the_converted_type(
        ConvertedType $convertedType,
        int $bitWidth,
        bool $isSigned,
    ): void {
        static::assertEquals(
            new Integer($bitWidth, $isSigned),
            Integer::forColumn(new FlatColumn('i', PhysicalType::INT64, $convertedType)),
        );
    }

    public function test_for_column_prefers_the_converted_type_over_the_logical_integer(): void
    {
        static::assertEquals(
            new Integer(32, false),
            Integer::forColumn(
                new FlatColumn('i', PhysicalType::INT32, ConvertedType::UINT_32, LogicalType::integer(32, true)),
            ),
        );
    }

    public function test_for_column_reads_a_logical_integer_without_a_converted_type(): void
    {
        static::assertEquals(
            new Integer(16, false),
            Integer::forColumn(new FlatColumn('i', PhysicalType::INT32, logicalType: LogicalType::integer(16, false))),
        );
    }

    public function test_for_column_without_an_integer_annotation(): void
    {
        static::assertNull(Integer::forColumn(new FlatColumn('i', PhysicalType::INT64)));
        static::assertNull(Integer::forColumn(FlatColumn::date('d')));
    }

    #[TestWith([8, true])]
    #[TestWith([16, true])]
    #[TestWith([32, true])]
    #[TestWith([64, true])]
    #[TestWith([8, false])]
    #[TestWith([16, false])]
    #[TestWith([32, false])]
    #[TestWith([64, false])]
    public function test_thrift_round_trip(int $bitWidth, bool $isSigned): void
    {
        $thrift = (new Integer($bitWidth, $isSigned))->toThrift();

        static::assertEquals(new IntType(['bitWidth' => $bitWidth, 'isSigned' => $isSigned]), $thrift);
        static::assertEquals(new Integer($bitWidth, $isSigned), Integer::fromThrift($thrift));
        static::assertSame($bitWidth, Integer::fromThrift($thrift)->bitWidth());
        static::assertSame($isSigned, Integer::fromThrift($thrift)->isSigned());
    }
}
