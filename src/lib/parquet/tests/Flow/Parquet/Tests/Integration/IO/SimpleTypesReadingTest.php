<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;

final class SimpleTypesReadingTest extends TestCase
{
    public function test_reading_bool_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool')->type());
        self::assertNull($file->metadata()->schema()->get('bool')->logicalType());

        $results = \array_merge_recursive(...\iterator_to_array($file->values(['bool'])))['bool'];
        self::assertCount(100, $results);
        self::assertContainsOnly('bool', $results);
        self::assertSame($file->metadata()->rowsNumber(), \count($results));
    }

    public function test_reading_bool_column_with_limit() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool')->type());
        self::assertNull($file->metadata()->schema()->get('bool')->logicalType());

        $results = \array_merge_recursive(...\iterator_to_array($file->values(['bool'], limit: 50)))['bool'];
        self::assertCount(50, $results);
        self::assertContainsOnly('bool', $results);
    }

    public function test_reading_bool_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool_nullable')->type());
        self::assertNull($file->metadata()->schema()->get('bool_nullable')->logicalType());

        $results = \array_merge_recursive(...\iterator_to_array($file->values(['bool_nullable'])))['bool_nullable'];
        self::assertCount(100, $results);
        self::assertSame($file->metadata()->rowsNumber(), \count($results));
        self::assertCount(50, \array_filter($results, fn ($value) => $value === null));
        self::assertCount(50, \array_filter($results, fn ($value) => $value !== null));
    }

    public function test_reading_bool_nullable_column_with_limit() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool_nullable')->type());
        self::assertNull($file->metadata()->schema()->get('bool_nullable')->logicalType());

        $results = \array_merge_recursive(...\iterator_to_array($file->values(['bool_nullable'], $limit = 50)))['bool_nullable'];
        self::assertCount($limit, $results);
        self::assertCount($limit / 2, \array_filter($results, fn ($value) => $value === null));
        self::assertCount($limit / 2, \array_filter($results, fn ($value) => $value !== null));
    }

    public function test_reading_date_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('date')->type());
        self::assertEquals('DATE', $file->metadata()->schema()->get('date')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['date']) as $row) {
            self::assertInstanceOf(\DateTimeImmutable::class, $row['date']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_date_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('date_nullable')->type());
        self::assertEquals('DATE', $file->metadata()->schema()->get('date_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['date_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertInstanceOf(\DateTimeImmutable::class, $row['date_nullable']);
            } else {
                self::assertNull($row['date_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_decimal_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::FIXED_LEN_BYTE_ARRAY, $file->metadata()->schema()->get('decimal')->type());

        $count = 0;

        foreach ($file->values(['decimal']) as $row) {
            self::assertIsFloat($row['decimal']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_decimal_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::FIXED_LEN_BYTE_ARRAY, $file->metadata()->schema()->get('decimal_nullable')->type());

        $count = 0;

        foreach ($file->values(['decimal_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertIsFloat($row['decimal_nullable']);
            } else {
                self::assertNull($row['decimal_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_double_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::DOUBLE, $file->metadata()->schema()->get('double')->type());

        $count = 0;

        foreach ($file->values(['double']) as $row) {
            self::assertIsFloat($row['double']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_double_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::DOUBLE, $file->metadata()->schema()->get('double_nullable')->type());

        $count = 0;

        foreach ($file->values(['double_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertIsFloat($row['double_nullable']);
            } else {
                self::assertNull($row['double_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_enum_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('enum')->type());
        self::assertEquals('STRING', $file->metadata()->schema()->get('enum')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['enum']) as $row) {
            self::assertIsString($row['enum']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_float_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::FLOAT, $file->metadata()->schema()->get('float')->type());

        $count = 0;

        foreach ($file->values(['float']) as $row) {
            self::assertIsFloat($row['float']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_float_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::FLOAT, $file->metadata()->schema()->get('float_nullable')->type());

        $count = 0;

        foreach ($file->values(['float_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertIsFloat($row['float_nullable']);
            } else {
                self::assertNull($row['float_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_int32_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('int32')->type());
        self::assertNull($file->metadata()->schema()->get('int32')->logicalType());

        $count = 0;

        foreach ($file->values(['int32']) as $row) {
            self::assertIsInt($row['int32']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_int32_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('int32_nullable')->type());
        self::assertNull($file->metadata()->schema()->get('int32_nullable')->logicalType());

        $count = 0;

        foreach ($file->values(['int32_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertIsInt($row['int32_nullable']);
            } else {
                self::assertNull($row['int32_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_int64() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('int64')->type());
        self::assertNull($file->metadata()->schema()->get('int64')->logicalType());

        $count = 0;

        foreach ($file->values(['int64']) as $row) {
            self::assertIsInt($row['int64']);
            $count++;
        }

        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_int64_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('int64_nullable')->type());
        self::assertNull($file->metadata()->schema()->get('int64_nullable')->logicalType());

        $count = 0;

        foreach ($file->values(['int64_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertIsInt($row['int64_nullable']);
            } else {
                self::assertNull($row['int64_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_json_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('json')->type());
        self::assertEquals('STRING', $file->metadata()->schema()->get('json')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['json']) as $row) {
            self::assertIsString($row['json']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_json_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('json_nullable')->type());
        self::assertEquals('STRING', $file->metadata()->schema()->get('json_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['json_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertIsString($row['json_nullable']);
            } else {
                self::assertNull($row['json_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_string_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('string')->type());
        self::assertEquals('STRING', $file->metadata()->schema()->get('string')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['string']) as $row) {
            self::assertIsString($row['string']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_string_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('string_nullable')->type());
        self::assertEquals('STRING', $file->metadata()->schema()->get('string_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['string_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertIsString($row['string_nullable']);
            } else {
                self::assertNull($row['string_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_time_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('time')->type());
        self::assertEquals('TIME', $file->metadata()->schema()->get('time')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['time']) as $row) {
            self::assertInstanceOf(\DateInterval::class, $row['time']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_time_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('time_nullable')->type());
        self::assertEquals('TIME', $file->metadata()->schema()->get('time_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['time_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertInstanceOf(\DateInterval::class, $row['time_nullable']);
            } else {
                self::assertNull($row['time_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_timestamp_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('timestamp')->type());
        self::assertEquals('TIMESTAMP', $file->metadata()->schema()->get('timestamp')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['timestamp']) as $row) {
            self::assertInstanceOf(\DateTimeImmutable::class, $row['timestamp']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_timestamp_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('timestamp_nullable')->type());
        self::assertEquals('TIMESTAMP', $file->metadata()->schema()->get('timestamp_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['timestamp_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertInstanceOf(\DateTimeImmutable::class, $row['timestamp_nullable']);
            } else {
                self::assertNull($row['timestamp_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_uuid_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('uuid')->type());
        self::assertEquals('STRING', $file->metadata()->schema()->get('uuid')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['uuid']) as $row) {
            self::assertIsString($row['uuid']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_uuid_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/primitives.parquet');

        self::assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('uuid_nullable')->type());
        self::assertEquals('STRING', $file->metadata()->schema()->get('uuid_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['uuid_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertIsString($row['uuid_nullable']);
            } else {
                self::assertNull($row['uuid_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
