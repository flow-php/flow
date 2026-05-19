<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetEngine;
use Flow\Parquet\Reader;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_key_first;

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
            /** @var array<array-key, mixed> $map */
            $map = $row['map'];
            static::assertIsArray($map);
            $firstKey = array_key_first($map);
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
                /** @var array<array-key, mixed> $map */
                $map = $row['map_nullable'];
                static::assertIsArray($map);
                $firstKey = array_key_first($map);
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
            /** @var array<array-key, mixed> $map */
            $map = $row['map_of_complex_lists'];
            static::assertIsArray($map);
            /** @var array<array-key, mixed> $list */
            $list = $map['key_0'];
            static::assertIsArray($list);
            /** @var array<array-key, mixed> $entry */
            $entry = $list[0];
            static::assertIsArray($entry);
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
            /** @var array<array-key, mixed> $map */
            $map = $row['map_of_list_of_map_of_lists'];
            static::assertIsArray($map);
            /** @var array<array-key, mixed> $outerList */
            $outerList = $map['key_0'];
            static::assertIsArray($outerList);
            /** @var array<array-key, mixed> $innerMap */
            $innerMap = $outerList[0];
            static::assertIsArray($innerMap);
            /** @var array<array-key, mixed> $innerList */
            $innerList = $innerMap['string_0_0_0'];
            static::assertIsArray($innerList);
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
            /** @var array<array-key, mixed> $map */
            $map = $row['map_of_lists'];
            static::assertIsArray($map);
            /** @var array<array-key, mixed> $list */
            $list = $map['key_0'];
            static::assertIsArray($list);
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
            /** @var array<array-key, mixed> $outerMap */
            $outerMap = $row['map_of_maps'];
            static::assertIsArray($outerMap);
            /** @var array<array-key, mixed> $innerMap */
            $innerMap = $outerMap['outer_key_0'];
            static::assertIsArray($innerMap);
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
            /** @var array<array-key, mixed> $map */
            $map = $row['map_of_struct_of_structs'];
            static::assertIsArray($map);
            /** @var array<array-key, mixed> $entry */
            $entry = $map['key_0'];
            static::assertIsArray($entry);
            /** @var array<array-key, mixed> $struct */
            $struct = $entry['struct'];
            static::assertIsArray($struct);
            /** @var array<array-key, mixed> $nested */
            $nested = $struct['nested_struct'];
            static::assertIsArray($nested);
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
            /** @var array<array-key, mixed> $map */
            $map = $row['map_of_struct_of_structs'];
            static::assertIsArray($map);
            /** @var array<array-key, mixed> $entry */
            $entry = $map['key_0'];
            static::assertIsArray($entry);
            /** @var array<array-key, mixed> $struct */
            $struct = $entry['struct'];
            static::assertIsArray($struct);
            /** @var array<array-key, mixed> $nested */
            $nested = $struct['nested_struct'];
            static::assertIsArray($nested);
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
            /** @var array<array-key, mixed> $map */
            $map = $row['map_of_structs'];
            static::assertIsArray($map);
            /** @var array<array-key, mixed> $entry */
            $entry = $map['key_0'];
            static::assertIsArray($entry);
            static::assertIsInt($entry['int_field']);
            static::assertIsString($entry['string_field']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
