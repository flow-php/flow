<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetEngine;
use Flow\Parquet\Reader;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Types\DSL\type_array;

class StructsReadingTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_reading_struct_deeply_nested_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->type());
        static::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_deeply_nested']) as $row) {
            $deep = type_array()->assert($row['struct_deeply_nested']);
            static::assertArrayHasKey('struct_0', $deep);
            $s0 = type_array()->assert($deep['struct_0']);
            static::assertArrayHasKey('int', $s0);
            static::assertArrayHasKey('struct_1', $s0);
            $s1 = type_array()->assert($s0['struct_1']);
            static::assertArrayHasKey('string', $s1);
            static::assertArrayHasKey('struct_2', $s1);
            $s2 = type_array()->assert($s1['struct_2']);
            static::assertArrayHasKey('bool', $s2);
            static::assertArrayHasKey('struct_3', $s2);
            $s3 = type_array()->assert($s2['struct_3']);
            static::assertArrayHasKey('float', $s3);
            static::assertArrayHasKey('struct_4', $s3);
            $s4 = type_array()->assert($s3['struct_4']);
            static::assertArrayHasKey('string', $s4);
            static::assertArrayHasKey('json', $s4);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_deeply_nested_column_with_limit(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->type());
        static::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_deeply_nested'], $limit = 50) as $row) {
            $deep = type_array()->assert($row['struct_deeply_nested']);
            static::assertArrayHasKey('struct_0', $deep);
            $s0 = type_array()->assert($deep['struct_0']);
            static::assertArrayHasKey('int', $s0);
            static::assertArrayHasKey('struct_1', $s0);
            $s1 = type_array()->assert($s0['struct_1']);
            static::assertArrayHasKey('string', $s1);
            static::assertArrayHasKey('struct_2', $s1);
            $s2 = type_array()->assert($s1['struct_2']);
            static::assertArrayHasKey('bool', $s2);
            static::assertArrayHasKey('struct_3', $s2);
            $s3 = type_array()->assert($s2['struct_3']);
            static::assertArrayHasKey('float', $s3);
            static::assertArrayHasKey('struct_4', $s3);
            $s4 = type_array()->assert($s3['struct_4']);
            static::assertArrayHasKey('string', $s4);
            static::assertArrayHasKey('json', $s4);
            $count++;
        }
        static::assertSame($limit, $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_flat_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_flat')->type());
        static::assertNull($file->metadata()->schema()->get('struct_flat')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_flat']) as $row) {
            $flat = type_array()->assert($row['struct_flat']);
            static::assertArrayHasKey('string', $flat);
            static::assertArrayHasKey('int', $flat);
            static::assertArrayHasKey('list_of_ints', $flat);
            static::assertArrayHasKey('map_of_string_int', $flat);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_flat_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_flat_nullable')->type());
        static::assertNull($file->metadata()->schema()->get('struct_flat_nullable')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_flat_nullable']) as $i => $row) {
            if (($i % 2) === 0) {
                $flat = type_array()->assert($row['struct_flat_nullable']);
                static::assertArrayHasKey('string', $flat);
                static::assertArrayHasKey('int', $flat);
                static::assertArrayHasKey('list_of_ints', $flat);
                static::assertArrayHasKey('map_of_string_int', $flat);
            } else {
                static::assertNull($row['struct_flat_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_nested')->type());
        static::assertNull($file->metadata()->schema()->get('struct_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested']) as $row) {
            $nested = type_array()->assert($row['struct_nested']);
            static::assertArrayHasKey('string', $nested);
            static::assertArrayHasKey('struct_flat', $nested);
            $flat = type_array()->assert($nested['struct_flat']);
            static::assertArrayHasKey('int', $flat);
            static::assertArrayHasKey('list_of_ints', $flat);
            static::assertArrayHasKey('map_of_string_int', $flat);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_with_list_of_lists_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_lists')->type());
        static::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_lists')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_list_of_lists']) as $row) {
            $outer = type_array()->assert($row['struct_nested_with_list_of_lists']);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            $struct = type_array()->assert($outer['struct']);
            static::assertArrayHasKey('int', $struct);
            static::assertArrayHasKey('list_of_list_of_ints', $struct);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_with_list_of_maps_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_maps')->type());
        static::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_maps')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_list_of_maps']) as $row) {
            $outer = type_array()->assert($row['struct_nested_with_list_of_maps']);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            $struct = type_array()->assert($outer['struct']);
            static::assertArrayHasKey('int', $struct);
            static::assertArrayHasKey('list_of_map_of_string_int', $struct);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_with_map_of_list_of_ints_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_list_of_ints')->type());
        static::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_list_of_ints')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_list_of_ints']) as $row) {
            $outer = type_array()->assert($row['struct_nested_with_map_of_list_of_ints']);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            $struct = type_array()->assert($outer['struct']);
            static::assertArrayHasKey('int', $struct);
            static::assertArrayHasKey('map_of_int_list_of_string', $struct);
            $mapList = type_array()->assert($struct['map_of_int_list_of_string']);
            static::assertIsList($mapList[0]);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_with_map_of_string_map_of_string_string_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull(
            $file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->type(),
        );
        static::assertNull(
            $file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->logicalType(),
        );

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_string_map_of_string_string']) as $row) {
            $outer = type_array()->assert($row['struct_nested_with_map_of_string_map_of_string_string']);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            $struct = type_array()->assert($outer['struct']);
            static::assertArrayHasKey('int', $struct);
            static::assertArrayHasKey('map_of_string_map_of_string_string', $struct);
            $outerMap = type_array()->assert($struct['map_of_string_map_of_string_string']);
            static::assertArrayHasKey('outer_key_0', $outerMap);
            $innerMap = type_array()->assert($outerMap['outer_key_0']);
            static::assertArrayHasKey('inner_key_0', $innerMap);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_with_map_of_string_map_of_string_string_column_with_limit(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull(
            $file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->type(),
        );
        static::assertNull(
            $file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->logicalType(),
        );

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_string_map_of_string_string'], $limit = 50) as $row) {
            $outer = type_array()->assert($row['struct_nested_with_map_of_string_map_of_string_string']);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            $struct = type_array()->assert($outer['struct']);
            static::assertArrayHasKey('int', $struct);
            static::assertArrayHasKey('map_of_string_map_of_string_string', $struct);
            $outerMap = type_array()->assert($struct['map_of_string_map_of_string_string']);
            static::assertArrayHasKey('outer_key_0', $outerMap);
            $innerMap = type_array()->assert($outerMap['outer_key_0']);
            static::assertArrayHasKey('inner_key_0', $innerMap);
            $count++;
        }
        static::assertSame($limit, $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_with_list_and_map_of_structs_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_with_list_and_map_of_structs')->type());
        static::assertNull($file->metadata()->schema()->get('struct_with_list_and_map_of_structs')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_with_list_and_map_of_structs']) as $row) {
            $outer = type_array()->assert($row['struct_with_list_and_map_of_structs']);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            $struct = type_array()->assert($outer['struct']);
            static::assertArrayHasKey('int', $struct);
            static::assertArrayHasKey('list_of_structs', $struct);
            static::assertArrayHasKey('map_of_string_structs', $struct);
            $map = type_array()->assert($struct['map_of_string_structs']);
            static::assertArrayHasKey('key_0', $map);
            $entry = type_array()->assert($map['key_0']);
            static::assertArrayHasKey('int', $entry);
            static::assertArrayHasKey('list', $entry);
            static::assertIsList($entry['list']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
