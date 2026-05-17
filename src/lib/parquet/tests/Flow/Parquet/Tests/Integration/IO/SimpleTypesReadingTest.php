<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Reader;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Types\DSL\type_array;

class SimpleTypesReadingTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_reading_bool_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool')->type());
        static::assertNull($file->metadata()->schema()->get('bool')->logicalType());

        $results = type_array()->assert(\array_merge_recursive(...\iterator_to_array($file->values(['bool'])))['bool']);
        static::assertCount(100, $results);
        static::assertContainsOnlyBool($results);
        static::assertSame($file->metadata()->rowsNumber(), \count($results));
    }

    #[DataProvider('engine_provider')]
    public function test_reading_bool_column_with_limit(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool')->type());
        static::assertNull($file->metadata()->schema()->get('bool')->logicalType());

        $results = type_array()->assert(
            \array_merge_recursive(...\iterator_to_array($file->values(['bool'], limit: 50)))['bool'],
        );
        static::assertCount(50, $results);
        static::assertContainsOnlyBool($results);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_bool_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool_nullable')->type());
        static::assertNull($file->metadata()->schema()->get('bool_nullable')->logicalType());

        $results = type_array()->assert(
            \array_merge_recursive(...\iterator_to_array($file->values(['bool_nullable'])))['bool_nullable'],
        );
        static::assertCount(100, $results);
        static::assertSame($file->metadata()->rowsNumber(), \count($results));
        static::assertCount(50, \array_filter($results, static fn($value) => $value === null));
        static::assertCount(50, \array_filter($results, static fn($value) => $value !== null));
    }

    #[DataProvider('engine_provider')]
    public function test_reading_bool_nullable_column_with_limit(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool_nullable')->type());
        static::assertNull($file->metadata()->schema()->get('bool_nullable')->logicalType());

        $results = type_array()->assert(
            \array_merge_recursive(...\iterator_to_array($file->values(
                ['bool_nullable'],
                $limit = 50,
            )))['bool_nullable'],
        );
        static::assertCount($limit, $results);
        static::assertCount($limit / 2, \array_filter($results, static fn($value) => $value === null));
        static::assertCount($limit / 2, \array_filter($results, static fn($value) => $value !== null));
    }

    #[DataProvider('engine_provider')]
    public function test_reading_date_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('date')->type());
        static::assertEquals('DATE', $file->metadata()->schema()->get('date')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['date']) as $row) {
            static::assertInstanceOf(\DateTimeImmutable::class, $row['date']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_date_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('date_nullable')->type());
        static::assertEquals('DATE', $file->metadata()->schema()->get('date_nullable')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['date_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertInstanceOf(\DateTimeImmutable::class, $row['date_nullable']);
            } else {
                static::assertNull($row['date_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_decimal_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::FIXED_LEN_BYTE_ARRAY, $file->metadata()->schema()->get('decimal')->type());

        $count = 0;

        foreach ($file->values(['decimal']) as $row) {
            static::assertIsFloat($row['decimal']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_decimal_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
            $file->metadata()->schema()->get('decimal_nullable')->type(),
        );

        $count = 0;

        foreach ($file->values(['decimal_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertIsFloat($row['decimal_nullable']);
            } else {
                static::assertNull($row['decimal_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_delta_binary_packed_encoded_integers(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/delta_binary_acked_encoded_integers.parquet');

        // Verify schema structure
        static::assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('int32_column')->type());
        static::assertNull($file->metadata()->schema()->get('int32_column')->logicalType());
        static::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('int64_column')->type());
        static::assertNull($file->metadata()->schema()->get('int64_column')->logicalType());
        static::assertEquals(PhysicalType::FLOAT, $file->metadata()->schema()->get('float_column')->type());
        static::assertNull($file->metadata()->schema()->get('float_column')->logicalType());

        // Verify file contains expected number of rows
        static::assertSame(1000, $file->metadata()->rowsNumber());

        // Test reading delta binary packed encoded integers
        $int32Values = [];
        $int64Values = [];
        $floatValues = [];
        $count = 0;

        foreach ($file->values(['int32_column', 'int64_column', 'float_column']) as $row) {
            $int32Values[] = $row['int32_column'];
            $int64Values[] = $row['int64_column'];
            $floatValues[] = $row['float_column'];
            $count++;
        }

        // Verify we read the expected number of rows
        static::assertSame(1000, $count);
        static::assertCount(1000, $int32Values);
        static::assertCount(1000, $int64Values);
        static::assertCount(1000, $floatValues);

        // Verify data types
        static::assertContainsOnlyInt($int32Values);
        static::assertContainsOnlyInt($int64Values);
        static::assertContainsOnlyFloat($floatValues);

        // Verify some sample values are reasonable
        static::assertGreaterThan(0, count(array_filter($int32Values, static fn($v) => $v !== 0)));
        static::assertGreaterThan(0, count(array_filter($int64Values, static fn($v) => $v !== 0)));
        static::assertGreaterThan(0, count(array_filter($floatValues, static fn($v) => $v !== 0.0)));
    }

    #[DataProvider('engine_provider')]
    public function test_reading_double_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::DOUBLE, $file->metadata()->schema()->get('double')->type());

        $count = 0;

        foreach ($file->values(['double']) as $row) {
            static::assertIsFloat($row['double']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_double_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::DOUBLE, $file->metadata()->schema()->get('double_nullable')->type());

        $count = 0;

        foreach ($file->values(['double_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertIsFloat($row['double_nullable']);
            } else {
                static::assertNull($row['double_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_enum_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('enum')->type());
        static::assertEquals('STRING', $file->metadata()->schema()->get('enum')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['enum']) as $row) {
            static::assertIsString($row['enum']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_float_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::FLOAT, $file->metadata()->schema()->get('float')->type());

        $count = 0;

        foreach ($file->values(['float']) as $row) {
            static::assertIsFloat($row['float']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_float_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::FLOAT, $file->metadata()->schema()->get('float_nullable')->type());

        $count = 0;

        foreach ($file->values(['float_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertIsFloat($row['float_nullable']);
            } else {
                static::assertNull($row['float_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_int32_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('int32')->type());
        static::assertNull($file->metadata()->schema()->get('int32')->logicalType());

        $count = 0;

        foreach ($file->values(['int32']) as $row) {
            static::assertIsInt($row['int32']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_int32_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('int32_nullable')->type());
        static::assertNull($file->metadata()->schema()->get('int32_nullable')->logicalType());

        $count = 0;

        foreach ($file->values(['int32_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertIsInt($row['int32_nullable']);
            } else {
                static::assertNull($row['int32_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_int64(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('int64')->type());
        static::assertNull($file->metadata()->schema()->get('int64')->logicalType());

        $count = 0;

        foreach ($file->values(['int64']) as $row) {
            static::assertIsInt($row['int64']);
            $count++;
        }

        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_int64_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('int64_nullable')->type());
        static::assertNull($file->metadata()->schema()->get('int64_nullable')->logicalType());

        $count = 0;

        foreach ($file->values(['int64_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertIsInt($row['int64_nullable']);
            } else {
                static::assertNull($row['int64_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_json_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('json')->type());
        static::assertEquals('STRING', $file->metadata()->schema()->get('json')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['json']) as $row) {
            static::assertIsString($row['json']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_json_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('json_nullable')->type());
        static::assertEquals('STRING', $file->metadata()->schema()->get('json_nullable')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['json_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertIsString($row['json_nullable']);
            } else {
                static::assertNull($row['json_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_string_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('string')->type());
        static::assertEquals('STRING', $file->metadata()->schema()->get('string')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['string']) as $row) {
            static::assertIsString($row['string']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_string_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('string_nullable')->type());
        static::assertEquals('STRING', $file->metadata()->schema()->get('string_nullable')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['string_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertIsString($row['string_nullable']);
            } else {
                static::assertNull($row['string_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_time_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('time')->type());
        static::assertEquals('TIME', $file->metadata()->schema()->get('time')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['time']) as $row) {
            static::assertInstanceOf(\DateInterval::class, $row['time']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_time_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('time_nullable')->type());
        static::assertEquals('TIME', $file->metadata()->schema()->get('time_nullable')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['time_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertInstanceOf(\DateInterval::class, $row['time_nullable']);
            } else {
                static::assertNull($row['time_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_timestamp_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('timestamp')->type());
        static::assertEquals('TIMESTAMP', $file->metadata()->schema()->get('timestamp')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['timestamp']) as $row) {
            static::assertInstanceOf(\DateTimeImmutable::class, $row['timestamp']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_timestamp_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('timestamp_nullable')->type());
        static::assertEquals(
            'TIMESTAMP',
            $file->metadata()->schema()->get('timestamp_nullable')->logicalType()?->name(),
        );

        $count = 0;

        foreach ($file->values(['timestamp_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertInstanceOf(\DateTimeImmutable::class, $row['timestamp_nullable']);
            } else {
                static::assertNull($row['timestamp_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_uuid_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('uuid')->type());
        static::assertEquals('STRING', $file->metadata()->schema()->get('uuid')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['uuid']) as $row) {
            static::assertIsString($row['uuid']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_uuid_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        static::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('uuid_nullable')->type());
        static::assertEquals('STRING', $file->metadata()->schema()->get('uuid_nullable')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['uuid_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertIsString($row['uuid_nullable']);
            } else {
                static::assertNull($row['uuid_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
