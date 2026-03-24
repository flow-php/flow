<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\{ParquetEngine, Reader};
use PHPUnit\Framework\Attributes\DataProvider;

class StructsReadingTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_reading_struct_deeply_nested_column(ParquetEngine $engine) : void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->type());
        static::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_deeply_nested']) as $row) {
            static::assertArrayHasKey('struct_0', $row['struct_deeply_nested']);
            static::assertArrayHasKey('int', $row['struct_deeply_nested']['struct_0']);
            static::assertArrayHasKey('struct_1', $row['struct_deeply_nested']['struct_0']);
            static::assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']);
            static::assertArrayHasKey('struct_2', $row['struct_deeply_nested']['struct_0']['struct_1']);
            static::assertArrayHasKey('bool', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            static::assertArrayHasKey('struct_3', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            static::assertArrayHasKey('float', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            static::assertArrayHasKey('struct_4', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            static::assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            static::assertArrayHasKey('json', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_deeply_nested_column_with_limit(ParquetEngine $engine) : void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->type());
        static::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_deeply_nested'], $limit = 50) as $row) {
            static::assertArrayHasKey('struct_0', $row['struct_deeply_nested']);
            static::assertArrayHasKey('int', $row['struct_deeply_nested']['struct_0']);
            static::assertArrayHasKey('struct_1', $row['struct_deeply_nested']['struct_0']);
            static::assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']);
            static::assertArrayHasKey('struct_2', $row['struct_deeply_nested']['struct_0']['struct_1']);
            static::assertArrayHasKey('bool', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            static::assertArrayHasKey('struct_3', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            static::assertArrayHasKey('float', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            static::assertArrayHasKey('struct_4', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            static::assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            static::assertArrayHasKey('json', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            $count++;
        }
        static::assertSame($limit, $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_flat_column(ParquetEngine $engine) : void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_flat')->type());
        static::assertNull($file->metadata()->schema()->get('struct_flat')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_flat']) as $row) {
            static::assertArrayHasKey('string', $row['struct_flat']);
            static::assertArrayHasKey('int', $row['struct_flat']);
            static::assertArrayHasKey('list_of_ints', $row['struct_flat']);
            static::assertArrayHasKey('map_of_string_int', $row['struct_flat']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_flat_nullable_column(ParquetEngine $engine) : void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_flat_nullable')->type());
        static::assertNull($file->metadata()->schema()->get('struct_flat_nullable')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_flat_nullable']) as $i => $row) {
            if ($i % 2 === 0) {
                static::assertArrayHasKey('string', $row['struct_flat_nullable']);
                static::assertArrayHasKey('int', $row['struct_flat_nullable']);
                static::assertArrayHasKey('list_of_ints', $row['struct_flat_nullable']);
                static::assertArrayHasKey('map_of_string_int', $row['struct_flat_nullable']);
            } else {
                static::assertNull($row['struct_flat_nullable']);
            }
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_column(ParquetEngine $engine) : void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_nested')->type());
        static::assertNull($file->metadata()->schema()->get('struct_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested']) as $row) {
            static::assertArrayHasKey('string', $row['struct_nested']);
            static::assertArrayHasKey('struct_flat', $row['struct_nested']);
            static::assertArrayHasKey('int', $row['struct_nested']['struct_flat']);
            static::assertArrayHasKey('list_of_ints', $row['struct_nested']['struct_flat']);
            static::assertArrayHasKey('map_of_string_int', $row['struct_nested']['struct_flat']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_with_list_of_lists_column(ParquetEngine $engine) : void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_lists')->type());
        static::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_lists')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_list_of_lists']) as $row) {
            static::assertArrayHasKey('string', $row['struct_nested_with_list_of_lists']);
            static::assertArrayHasKey('struct', $row['struct_nested_with_list_of_lists']);
            static::assertArrayHasKey('int', $row['struct_nested_with_list_of_lists']['struct']);
            static::assertArrayHasKey('list_of_list_of_ints', $row['struct_nested_with_list_of_lists']['struct']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_with_list_of_maps_column(ParquetEngine $engine) : void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_maps')->type());
        static::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_maps')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_list_of_maps']) as $row) {
            static::assertArrayHasKey('string', $row['struct_nested_with_list_of_maps']);
            static::assertArrayHasKey('struct', $row['struct_nested_with_list_of_maps']);
            static::assertArrayHasKey('int', $row['struct_nested_with_list_of_maps']['struct']);
            static::assertArrayHasKey('list_of_map_of_string_int', $row['struct_nested_with_list_of_maps']['struct']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_with_map_of_list_of_ints_column(ParquetEngine $engine) : void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_list_of_ints')->type());
        static::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_list_of_ints')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_list_of_ints']) as $row) {
            static::assertArrayHasKey('string', $row['struct_nested_with_map_of_list_of_ints']);
            static::assertArrayHasKey('struct', $row['struct_nested_with_map_of_list_of_ints']);
            static::assertArrayHasKey('int', $row['struct_nested_with_map_of_list_of_ints']['struct']);
            static::assertArrayHasKey('map_of_int_list_of_string', $row['struct_nested_with_map_of_list_of_ints']['struct']);
            static::assertIsList($row['struct_nested_with_map_of_list_of_ints']['struct']['map_of_int_list_of_string'][0]);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_with_map_of_string_map_of_string_string_column(ParquetEngine $engine) : void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->type());
        static::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_string_map_of_string_string']) as $row) {
            static::assertArrayHasKey('string', $row['struct_nested_with_map_of_string_map_of_string_string']);
            static::assertArrayHasKey('struct', $row['struct_nested_with_map_of_string_map_of_string_string']);
            static::assertArrayHasKey('int', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            static::assertArrayHasKey('map_of_string_map_of_string_string', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            static::assertArrayHasKey('outer_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']);
            static::assertArrayHasKey('inner_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']['outer_key_0']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_nested_with_map_of_string_map_of_string_string_column_with_limit(ParquetEngine $engine) : void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->type());
        static::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_string_map_of_string_string'], $limit = 50) as $row) {
            static::assertArrayHasKey('string', $row['struct_nested_with_map_of_string_map_of_string_string']);
            static::assertArrayHasKey('struct', $row['struct_nested_with_map_of_string_map_of_string_string']);
            static::assertArrayHasKey('int', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            static::assertArrayHasKey('map_of_string_map_of_string_string', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            static::assertArrayHasKey('outer_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']);
            static::assertArrayHasKey('inner_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']['outer_key_0']);
            $count++;
        }
        static::assertSame($limit, $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_struct_with_list_and_map_of_structs_column(ParquetEngine $engine) : void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        static::assertNull($file->metadata()->schema()->get('struct_with_list_and_map_of_structs')->type());
        static::assertNull($file->metadata()->schema()->get('struct_with_list_and_map_of_structs')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_with_list_and_map_of_structs']) as $row) {
            static::assertArrayHasKey('string', $row['struct_with_list_and_map_of_structs']);
            static::assertArrayHasKey('struct', $row['struct_with_list_and_map_of_structs']);
            static::assertArrayHasKey('int', $row['struct_with_list_and_map_of_structs']['struct']);
            static::assertArrayHasKey('list_of_structs', $row['struct_with_list_and_map_of_structs']['struct']);
            static::assertArrayHasKey('map_of_string_structs', $row['struct_with_list_and_map_of_structs']['struct']);
            static::assertArrayHasKey('key_0', $row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']);
            static::assertArrayHasKey('int', $row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']['key_0']);
            static::assertArrayHasKey('list', $row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']['key_0']);
            static::assertIsList($row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']['key_0']['list']);
            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
