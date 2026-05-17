<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetEngine;
use Flow\Parquet\Reader;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Types\DSL\type_array;

class MapsReadingTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_reading_map_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        static::assertNull($file->metadata()->schema()->get('map')->type());
        static::assertEquals('MAP', $file->metadata()->schema()->get('map')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['map']) as $row) {
            $map = type_array()->assert($row['map']);
            $firstKey = \array_key_first($map);
            static::assertIsString($firstKey);
            static::assertIsInt($map[$firstKey]);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_map_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        static::assertNull($file->metadata()->schema()->get('map_nullable')->type());
        static::assertEquals('MAP', $file->metadata()->schema()->get('map_nullable')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['map_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                $map = type_array()->assert($row['map_nullable']);
                $firstKey = \array_key_first($map);
                static::assertIsString($firstKey);
                static::assertIsInt($map[$firstKey]);
            } else {
                static::assertNull($row['map_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_map_of_complex_lists(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        static::assertNull($file->metadata()->schema()->get('map_of_complex_lists')->type());
        static::assertEquals('MAP', $file->metadata()->schema()->get('map_of_complex_lists')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['map_of_complex_lists']) as $row) {
            $map = type_array()->assert($row['map_of_complex_lists']);
            $list = type_array()->assert($map['key_0']);
            $entry = type_array()->assert($list[0]);
            static::assertIsString($entry['string']);
            static::assertIsInt($entry['int']);
            static::assertIsBool($entry['bool']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_map_of_list_of_map_of_lists(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        static::assertNull($file->metadata()->schema()->get('map_of_list_of_map_of_lists')->type());
        static::assertEquals(
            'MAP',
            $file->metadata()->schema()->get('map_of_list_of_map_of_lists')->logicalType()?->name(),
        );

        $count = 0;

        foreach ($file->values(['map_of_list_of_map_of_lists']) as $row) {
            $map = type_array()->assert($row['map_of_list_of_map_of_lists']);
            $outerList = type_array()->assert($map['key_0']);
            $innerMap = type_array()->assert($outerList[0]);
            $innerList = type_array()->assert($innerMap['string_0_0_0']);
            static::assertIsList($innerList);
            static::assertIsInt($innerList[0]);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_map_of_lists(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        static::assertNull($file->metadata()->schema()->get('map_of_lists')->type());
        static::assertEquals('MAP', $file->metadata()->schema()->get('map_of_lists')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['map_of_lists']) as $row) {
            $map = type_array()->assert($row['map_of_lists']);
            $list = type_array()->assert($map['key_0']);
            static::assertIsList($list);
            static::assertIsInt($list[0]);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_map_of_maps_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        static::assertNull($file->metadata()->schema()->get('map_of_maps')->type());
        static::assertEquals('MAP', $file->metadata()->schema()->get('map_of_maps')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['map_of_maps']) as $row) {
            $outerMap = type_array()->assert($row['map_of_maps']);
            $innerMap = type_array()->assert($outerMap['outer_key_0']);
            static::assertIsInt($innerMap['inner_key_0']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_map_of_struct_of_structs_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        static::assertNull($file->metadata()->schema()->get('map_of_struct_of_structs')->type());
        static::assertEquals(
            'MAP',
            $file->metadata()->schema()->get('map_of_struct_of_structs')->logicalType()?->name(),
        );

        $count = 0;

        foreach ($file->values(['map_of_struct_of_structs']) as $row) {
            $map = type_array()->assert($row['map_of_struct_of_structs']);
            $entry = type_array()->assert($map['key_0']);
            $struct = type_array()->assert($entry['struct']);
            $nested = type_array()->assert($struct['nested_struct']);
            static::assertIsInt($nested['int']);
            static::assertIsString($nested['string']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_map_of_struct_of_structs_column_with_limit(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        static::assertNull($file->metadata()->schema()->get('map_of_struct_of_structs')->type());
        static::assertEquals(
            'MAP',
            $file->metadata()->schema()->get('map_of_struct_of_structs')->logicalType()?->name(),
        );

        $count = 0;

        foreach ($file->values(['map_of_struct_of_structs'], $limit = 50) as $row) {
            $map = type_array()->assert($row['map_of_struct_of_structs']);
            $entry = type_array()->assert($map['key_0']);
            $struct = type_array()->assert($entry['struct']);
            $nested = type_array()->assert($struct['nested_struct']);
            static::assertIsInt($nested['int']);
            static::assertIsString($nested['string']);
            $count++;
        }
        static::assertSame($limit, $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_map_of_structs_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        static::assertNull($file->metadata()->schema()->get('map_of_structs')->type());
        static::assertEquals('MAP', $file->metadata()->schema()->get('map_of_structs')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['map_of_structs']) as $row) {
            $map = type_array()->assert($row['map_of_structs']);
            $entry = type_array()->assert($map['key_0']);
            static::assertIsInt($entry['int_field']);
            static::assertIsString($entry['string_field']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
