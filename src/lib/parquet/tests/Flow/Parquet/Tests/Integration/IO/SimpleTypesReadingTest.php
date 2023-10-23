<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Reader;

final class SimpleTypesReadingTest extends ParquetIntegrationTestCase
{
    public function test_reading_bool_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool')->type());
        $this->assertNull($file->metadata()->schema()->get('bool')->logicalType());

        $results = \array_merge_recursive(...\iterator_to_array($file->values(['bool'])))['bool'];
        $this->assertCount(100, $results);
        $this->assertContainsOnly('bool', $results);
        $this->assertSame($file->metadata()->rowsNumber(), \count($results));
    }

    public function test_reading_bool_column_with_limit() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool')->type());
        $this->assertNull($file->metadata()->schema()->get('bool')->logicalType());

        $results = \array_merge_recursive(...\iterator_to_array($file->values(['bool'], limit: 50)))['bool'];
        $this->assertCount(50, $results);
        $this->assertContainsOnly('bool', $results);
    }

    public function test_reading_bool_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool_nullable')->type());
        $this->assertNull($file->metadata()->schema()->get('bool_nullable')->logicalType());

        $results = \array_merge_recursive(...\iterator_to_array($file->values(['bool_nullable'])))['bool_nullable'];
        $this->assertCount(100, $results);
        $this->assertSame($file->metadata()->rowsNumber(), \count($results));
        $this->assertCount(50, \array_filter($results, fn ($value) => $value === null));
        $this->assertCount(50, \array_filter($results, fn ($value) => $value !== null));
    }

    public function test_reading_bool_nullable_column_with_limit() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('bool_nullable')->type());
        $this->assertNull($file->metadata()->schema()->get('bool_nullable')->logicalType());

        $results = \array_merge_recursive(...\iterator_to_array($file->values(['bool_nullable'], $limit = 50)))['bool_nullable'];
        $this->assertCount($limit, $results);
        $this->assertCount($limit / 2, \array_filter($results, fn ($value) => $value === null));
        $this->assertCount($limit / 2, \array_filter($results, fn ($value) => $value !== null));
    }

    public function test_reading_date_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('date')->type());
        $this->assertEquals('DATE', $file->metadata()->schema()->get('date')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['date']) as $row) {
            $this->assertInstanceOf(\DateTimeImmutable::class, $row['date']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_date_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('date_nullable')->type());
        $this->assertEquals('DATE', $file->metadata()->schema()->get('date_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['date_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertInstanceOf(\DateTimeImmutable::class, $row['date_nullable']);
            } else {
                $this->assertNull($row['date_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_decimal_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::FIXED_LEN_BYTE_ARRAY, $file->metadata()->schema()->get('decimal')->type());

        $count = 0;

        foreach ($file->values(['decimal']) as $row) {
            $this->assertIsFloat($row['decimal']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_decimal_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::FIXED_LEN_BYTE_ARRAY, $file->metadata()->schema()->get('decimal_nullable')->type());

        $count = 0;

        foreach ($file->values(['decimal_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsFloat($row['decimal_nullable']);
            } else {
                $this->assertNull($row['decimal_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_double_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::DOUBLE, $file->metadata()->schema()->get('double')->type());

        $count = 0;

        foreach ($file->values(['double']) as $row) {
            $this->assertIsFloat($row['double']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_double_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::DOUBLE, $file->metadata()->schema()->get('double_nullable')->type());

        $count = 0;

        foreach ($file->values(['double_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsFloat($row['double_nullable']);
            } else {
                $this->assertNull($row['double_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_enum_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('enum')->type());
        $this->assertEquals('STRING', $file->metadata()->schema()->get('enum')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['enum']) as $row) {
            $this->assertIsString($row['enum']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_float_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::FLOAT, $file->metadata()->schema()->get('float')->type());

        $count = 0;

        foreach ($file->values(['float']) as $row) {
            $this->assertIsFloat($row['float']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_float_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::FLOAT, $file->metadata()->schema()->get('float_nullable')->type());

        $count = 0;

        foreach ($file->values(['float_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsFloat($row['float_nullable']);
            } else {
                $this->assertNull($row['float_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_int32_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('int32')->type());
        $this->assertNull($file->metadata()->schema()->get('int32')->logicalType());

        $count = 0;

        foreach ($file->values(['int32']) as $row) {
            $this->assertIsInt($row['int32']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_int32_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::INT32, $file->metadata()->schema()->get('int32_nullable')->type());
        $this->assertNull($file->metadata()->schema()->get('int32_nullable')->logicalType());

        $count = 0;

        foreach ($file->values(['int32_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsInt($row['int32_nullable']);
            } else {
                $this->assertNull($row['int32_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_int64() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('int64')->type());
        $this->assertNull($file->metadata()->schema()->get('int64')->logicalType());

        $count = 0;

        foreach ($file->values(['int64']) as $row) {
            $this->assertIsInt($row['int64']);
            $count++;
        }

        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_int64_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('int64_nullable')->type());
        $this->assertNull($file->metadata()->schema()->get('int64_nullable')->logicalType());

        $count = 0;

        foreach ($file->values(['int64_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsInt($row['int64_nullable']);
            } else {
                $this->assertNull($row['int64_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_json_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('json')->type());
        $this->assertEquals('STRING', $file->metadata()->schema()->get('json')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['json']) as $row) {
            $this->assertIsString($row['json']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_json_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('json_nullable')->type());
        $this->assertEquals('STRING', $file->metadata()->schema()->get('json_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['json_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsString($row['json_nullable']);
            } else {
                $this->assertNull($row['json_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_string_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('string')->type());
        $this->assertEquals('STRING', $file->metadata()->schema()->get('string')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['string']) as $row) {
            $this->assertIsString($row['string']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_string_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('string_nullable')->type());
        $this->assertEquals('STRING', $file->metadata()->schema()->get('string_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['string_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsString($row['string_nullable']);
            } else {
                $this->assertNull($row['string_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_time_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('time')->type());
        $this->assertEquals('TIME', $file->metadata()->schema()->get('time')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['time']) as $row) {
            $this->assertIsInt($row['time']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_time_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('time_nullable')->type());
        $this->assertEquals('TIME', $file->metadata()->schema()->get('time_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['time_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsInt($row['time_nullable']);
            } else {
                $this->assertNull($row['time_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_timestamp_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('timestamp')->type());
        $this->assertEquals('TIMESTAMP', $file->metadata()->schema()->get('timestamp')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['timestamp']) as $row) {
            $this->assertIsInt($row['timestamp']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_timestamp_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::INT64, $file->metadata()->schema()->get('timestamp_nullable')->type());
        $this->assertEquals('TIMESTAMP', $file->metadata()->schema()->get('timestamp_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['timestamp_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsInt($row['timestamp_nullable']);
            } else {
                $this->assertNull($row['timestamp_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_uuid_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('uuid')->type());
        $this->assertEquals('STRING', $file->metadata()->schema()->get('uuid')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['uuid']) as $row) {
            $this->assertIsString($row['uuid']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_uuid_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/primitives.parquet');

        $this->assertEquals(PhysicalType::BYTE_ARRAY, $file->metadata()->schema()->get('uuid_nullable')->type());
        $this->assertEquals('STRING', $file->metadata()->schema()->get('uuid_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['uuid_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsString($row['uuid_nullable']);
            } else {
                $this->assertNull($row['uuid_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }
}
