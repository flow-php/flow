<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetEngine;
use Flow\Parquet\Reader;
use PHPUnit\Framework\Attributes\DataProvider;

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
            /** @var array<array-key, mixed> $deep */
            $deep = $row['struct_deeply_nested'];
            static::assertIsArray($deep);
            static::assertArrayHasKey('struct_0', $deep);
            /** @var array<array-key, mixed> $s0 */
            $s0 = $deep['struct_0'];
            static::assertIsArray($s0);
            static::assertArrayHasKey('int', $s0);
            static::assertArrayHasKey('struct_1', $s0);
            /** @var array<array-key, mixed> $s1 */
            $s1 = $s0['struct_1'];
            static::assertIsArray($s1);
            static::assertArrayHasKey('string', $s1);
            static::assertArrayHasKey('struct_2', $s1);
            /** @var array<array-key, mixed> $s2 */
            $s2 = $s1['struct_2'];
            static::assertIsArray($s2);
            static::assertArrayHasKey('bool', $s2);
            static::assertArrayHasKey('struct_3', $s2);
            /** @var array<array-key, mixed> $s3 */
            $s3 = $s2['struct_3'];
            static::assertIsArray($s3);
            static::assertArrayHasKey('float', $s3);
            static::assertArrayHasKey('struct_4', $s3);
            /** @var array<array-key, mixed> $s4 */
            $s4 = $s3['struct_4'];
            static::assertIsArray($s4);
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
            /** @var array<array-key, mixed> $deep */
            $deep = $row['struct_deeply_nested'];
            static::assertIsArray($deep);
            static::assertArrayHasKey('struct_0', $deep);
            /** @var array<array-key, mixed> $s0 */
            $s0 = $deep['struct_0'];
            static::assertIsArray($s0);
            static::assertArrayHasKey('int', $s0);
            static::assertArrayHasKey('struct_1', $s0);
            /** @var array<array-key, mixed> $s1 */
            $s1 = $s0['struct_1'];
            static::assertIsArray($s1);
            static::assertArrayHasKey('string', $s1);
            static::assertArrayHasKey('struct_2', $s1);
            /** @var array<array-key, mixed> $s2 */
            $s2 = $s1['struct_2'];
            static::assertIsArray($s2);
            static::assertArrayHasKey('bool', $s2);
            static::assertArrayHasKey('struct_3', $s2);
            /** @var array<array-key, mixed> $s3 */
            $s3 = $s2['struct_3'];
            static::assertIsArray($s3);
            static::assertArrayHasKey('float', $s3);
            static::assertArrayHasKey('struct_4', $s3);
            /** @var array<array-key, mixed> $s4 */
            $s4 = $s3['struct_4'];
            static::assertIsArray($s4);
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
            /** @var array<array-key, mixed> $flat */
            $flat = $row['struct_flat'];
            static::assertIsArray($flat);
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
                /** @var array<array-key, mixed> $flat */
                $flat = $row['struct_flat_nullable'];
                static::assertIsArray($flat);
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
            /** @var array<array-key, mixed> $nested */
            $nested = $row['struct_nested'];
            static::assertIsArray($nested);
            static::assertArrayHasKey('string', $nested);
            static::assertArrayHasKey('struct_flat', $nested);
            /** @var array<array-key, mixed> $flat */
            $flat = $nested['struct_flat'];
            static::assertIsArray($flat);
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
            /** @var array<array-key, mixed> $outer */
            $outer = $row['struct_nested_with_list_of_lists'];
            static::assertIsArray($outer);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            /** @var array<array-key, mixed> $struct */
            $struct = $outer['struct'];
            static::assertIsArray($struct);
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
            /** @var array<array-key, mixed> $outer */
            $outer = $row['struct_nested_with_list_of_maps'];
            static::assertIsArray($outer);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            /** @var array<array-key, mixed> $struct */
            $struct = $outer['struct'];
            static::assertIsArray($struct);
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
            /** @var array<array-key, mixed> $outer */
            $outer = $row['struct_nested_with_map_of_list_of_ints'];
            static::assertIsArray($outer);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            /** @var array<array-key, mixed> $struct */
            $struct = $outer['struct'];
            static::assertIsArray($struct);
            static::assertArrayHasKey('int', $struct);
            static::assertArrayHasKey('map_of_int_list_of_string', $struct);
            /** @var array<array-key, mixed> $mapList */
            $mapList = $struct['map_of_int_list_of_string'];
            static::assertIsArray($mapList);
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
            /** @var array<array-key, mixed> $outer */
            $outer = $row['struct_nested_with_map_of_string_map_of_string_string'];
            static::assertIsArray($outer);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            /** @var array<array-key, mixed> $struct */
            $struct = $outer['struct'];
            static::assertIsArray($struct);
            static::assertArrayHasKey('int', $struct);
            static::assertArrayHasKey('map_of_string_map_of_string_string', $struct);
            /** @var array<array-key, mixed> $outerMap */
            $outerMap = $struct['map_of_string_map_of_string_string'];
            static::assertIsArray($outerMap);
            static::assertArrayHasKey('outer_key_0', $outerMap);
            /** @var array<array-key, mixed> $innerMap */
            $innerMap = $outerMap['outer_key_0'];
            static::assertIsArray($innerMap);
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
            /** @var array<array-key, mixed> $outer */
            $outer = $row['struct_nested_with_map_of_string_map_of_string_string'];
            static::assertIsArray($outer);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            /** @var array<array-key, mixed> $struct */
            $struct = $outer['struct'];
            static::assertIsArray($struct);
            static::assertArrayHasKey('int', $struct);
            static::assertArrayHasKey('map_of_string_map_of_string_string', $struct);
            /** @var array<array-key, mixed> $outerMap */
            $outerMap = $struct['map_of_string_map_of_string_string'];
            static::assertIsArray($outerMap);
            static::assertArrayHasKey('outer_key_0', $outerMap);
            /** @var array<array-key, mixed> $innerMap */
            $innerMap = $outerMap['outer_key_0'];
            static::assertIsArray($innerMap);
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
            /** @var array<array-key, mixed> $outer */
            $outer = $row['struct_with_list_and_map_of_structs'];
            static::assertIsArray($outer);
            static::assertArrayHasKey('string', $outer);
            static::assertArrayHasKey('struct', $outer);
            /** @var array<array-key, mixed> $struct */
            $struct = $outer['struct'];
            static::assertIsArray($struct);
            static::assertArrayHasKey('int', $struct);
            static::assertArrayHasKey('list_of_structs', $struct);
            static::assertArrayHasKey('map_of_string_structs', $struct);
            /** @var array<array-key, mixed> $map */
            $map = $struct['map_of_string_structs'];
            static::assertIsArray($map);
            static::assertArrayHasKey('key_0', $map);
            /** @var array<array-key, mixed> $entry */
            $entry = $map['key_0'];
            static::assertIsArray($entry);
            static::assertArrayHasKey('int', $entry);
            static::assertArrayHasKey('list', $entry);
            static::assertIsList($entry['list']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
