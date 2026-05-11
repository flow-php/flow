<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetEngine;
use Flow\Parquet\Reader;
use PHPUnit\Framework\Attributes\DataProvider;

class MapsReadingTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_reading_map_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        static::assertNull($file->metadata()->schema()->get('map')->type());
        static::assertEquals('MAP', $file->metadata()->schema()->get('map')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map']) as $row) {
            static::assertIsString(\array_key_first($row['map']));
            static::assertIsInt($row['map'][\array_key_first($row['map'])]);
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
        static::assertEquals('MAP', $file->metadata()->schema()->get('map_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertIsString(\array_key_first($row['map_nullable']));
                static::assertIsInt($row['map_nullable'][\array_key_first($row['map_nullable'])]);
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
        static::assertEquals('MAP', $file->metadata()->schema()->get('map_of_complex_lists')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_complex_lists']) as $row) {
            static::assertIsArray($row['map_of_complex_lists']);
            static::assertIsArray($row['map_of_complex_lists']['key_0']);
            static::assertIsString($row['map_of_complex_lists']['key_0'][0]['string']);
            static::assertIsInt($row['map_of_complex_lists']['key_0'][0]['int']);
            static::assertIsBool($row['map_of_complex_lists']['key_0'][0]['bool']);
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
            $file->metadata()->schema()->get('map_of_list_of_map_of_lists')->logicalType()->name(),
        );

        $count = 0;

        foreach ($file->values(['map_of_list_of_map_of_lists']) as $row) {
            static::assertIsArray($row['map_of_list_of_map_of_lists']);
            static::assertIsArray($row['map_of_list_of_map_of_lists']['key_0']);
            static::assertIsArray($row['map_of_list_of_map_of_lists']['key_0'][0]);
            static::assertIsArray($row['map_of_list_of_map_of_lists']['key_0'][0]['string_0_0_0']);
            static::assertIsList($row['map_of_list_of_map_of_lists']['key_0'][0]['string_0_0_0']);
            static::assertIsInt($row['map_of_list_of_map_of_lists']['key_0'][0]['string_0_0_0'][0]);
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
        static::assertEquals('MAP', $file->metadata()->schema()->get('map_of_lists')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_lists']) as $row) {
            static::assertIsArray($row['map_of_lists']);
            static::assertIsArray($row['map_of_lists']['key_0']);
            static::assertIsList($row['map_of_lists']['key_0']);
            static::assertIsInt($row['map_of_lists']['key_0'][0]);
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
        static::assertEquals('MAP', $file->metadata()->schema()->get('map_of_maps')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_maps']) as $row) {
            static::assertIsArray($row['map_of_maps']);
            static::assertIsArray($row['map_of_maps']['outer_key_0']);
            static::assertIsInt($row['map_of_maps']['outer_key_0']['inner_key_0']);
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
            $file->metadata()->schema()->get('map_of_struct_of_structs')->logicalType()->name(),
        );

        $count = 0;

        foreach ($file->values(['map_of_struct_of_structs']) as $row) {
            static::assertIsArray($row['map_of_struct_of_structs']);
            static::assertIsArray($row['map_of_struct_of_structs']['key_0']);
            static::assertIsInt($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['int']);
            static::assertIsString($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['string']);
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
            $file->metadata()->schema()->get('map_of_struct_of_structs')->logicalType()->name(),
        );

        $count = 0;

        foreach ($file->values(['map_of_struct_of_structs'], $limit = 50) as $row) {
            static::assertIsArray($row['map_of_struct_of_structs']);
            static::assertIsArray($row['map_of_struct_of_structs']['key_0']);
            static::assertIsInt($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['int']);
            static::assertIsString($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['string']);
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
        static::assertEquals('MAP', $file->metadata()->schema()->get('map_of_structs')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_structs']) as $row) {
            static::assertIsArray($row['map_of_structs']);
            static::assertIsArray($row['map_of_structs']['key_0']);
            static::assertIsInt($row['map_of_structs']['key_0']['int_field']);
            static::assertIsString($row['map_of_structs']['key_0']['string_field']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
