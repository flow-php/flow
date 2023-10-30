<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Reader;

final class StructsReadingTest extends ParquetIntegrationTestCase
{
    public function test_reading_struct_deeply_nested_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/structs.parquet');

        $this->assertNull($file->metadata()->schema()->get('struct_deeply_nested')->type());
        $this->assertNull($file->metadata()->schema()->get('struct_deeply_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_deeply_nested']) as $row) {
            $this->assertArrayHasKey('struct_0', $row['struct_deeply_nested']);
            $this->assertArrayHasKey('int', $row['struct_deeply_nested']['struct_0']);
            $this->assertArrayHasKey('struct_1', $row['struct_deeply_nested']['struct_0']);
            $this->assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']);
            $this->assertArrayHasKey('struct_2', $row['struct_deeply_nested']['struct_0']['struct_1']);
            $this->assertArrayHasKey('bool', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            $this->assertArrayHasKey('struct_3', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            $this->assertArrayHasKey('float', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            $this->assertArrayHasKey('struct_4', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            $this->assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            $this->assertArrayHasKey('json', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_deeply_nested_column_with_limit() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/structs.parquet');

        $this->assertNull($file->metadata()->schema()->get('struct_deeply_nested')->type());
        $this->assertNull($file->metadata()->schema()->get('struct_deeply_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_deeply_nested'], $limit = 50) as $row) {
            $this->assertArrayHasKey('struct_0', $row['struct_deeply_nested']);
            $this->assertArrayHasKey('int', $row['struct_deeply_nested']['struct_0']);
            $this->assertArrayHasKey('struct_1', $row['struct_deeply_nested']['struct_0']);
            $this->assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']);
            $this->assertArrayHasKey('struct_2', $row['struct_deeply_nested']['struct_0']['struct_1']);
            $this->assertArrayHasKey('bool', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            $this->assertArrayHasKey('struct_3', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            $this->assertArrayHasKey('float', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            $this->assertArrayHasKey('struct_4', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            $this->assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            $this->assertArrayHasKey('json', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            $count++;
        }
        $this->assertSame($limit, $count);
    }

    public function test_reading_struct_flat_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/structs.parquet');

        $this->assertNull($file->metadata()->schema()->get('struct_flat')->type());
        $this->assertNull($file->metadata()->schema()->get('struct_flat')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_flat']) as $row) {
            $this->assertArrayHasKey('string', $row['struct_flat']);
            $this->assertArrayHasKey('int', $row['struct_flat']);
            $this->assertArrayHasKey('list_of_ints', $row['struct_flat']);
            $this->assertArrayHasKey('map_of_string_int', $row['struct_flat']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_flat_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/structs.parquet');

        $this->assertNull($file->metadata()->schema()->get('struct_flat_nullable')->type());
        $this->assertNull($file->metadata()->schema()->get('struct_flat_nullable')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_flat_nullable']) as $i => $row) {
            if ($i % 2 === 0) {
                $this->assertArrayHasKey('string', $row['struct_flat_nullable']);
                $this->assertArrayHasKey('int', $row['struct_flat_nullable']);
                $this->assertArrayHasKey('list_of_ints', $row['struct_flat_nullable']);
                $this->assertArrayHasKey('map_of_string_int', $row['struct_flat_nullable']);
            } else {
                $this->assertNull($row['struct_flat_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/structs.parquet');

        $this->assertNull($file->metadata()->schema()->get('struct_nested')->type());
        $this->assertNull($file->metadata()->schema()->get('struct_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested']) as $row) {
            $this->assertArrayHasKey('string', $row['struct_nested']);
            $this->assertArrayHasKey('struct_flat', $row['struct_nested']);
            $this->assertArrayHasKey('int', $row['struct_nested']['struct_flat']);
            $this->assertArrayHasKey('list_of_ints', $row['struct_nested']['struct_flat']);
            $this->assertArrayHasKey('map_of_string_int', $row['struct_nested']['struct_flat']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_with_list_of_lists_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/structs.parquet');

        $this->assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_lists')->type());
        $this->assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_lists')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_list_of_lists']) as $row) {
            $this->assertArrayHasKey('string', $row['struct_nested_with_list_of_lists']);
            $this->assertArrayHasKey('struct', $row['struct_nested_with_list_of_lists']);
            $this->assertArrayHasKey('int', $row['struct_nested_with_list_of_lists']['struct']);
            $this->assertArrayHasKey('list_of_list_of_ints', $row['struct_nested_with_list_of_lists']['struct']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_with_list_of_maps_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/structs.parquet');

        $this->assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_maps')->type());
        $this->assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_maps')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_list_of_maps']) as $row) {
            $this->assertArrayHasKey('string', $row['struct_nested_with_list_of_maps']);
            $this->assertArrayHasKey('struct', $row['struct_nested_with_list_of_maps']);
            $this->assertArrayHasKey('int', $row['struct_nested_with_list_of_maps']['struct']);
            $this->assertArrayHasKey('list_of_map_of_string_int', $row['struct_nested_with_list_of_maps']['struct']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_with_map_of_list_of_ints_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/structs.parquet');

        $this->assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_list_of_ints')->type());
        $this->assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_list_of_ints')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_list_of_ints']) as $row) {
            $this->assertArrayHasKey('string', $row['struct_nested_with_map_of_list_of_ints']);
            $this->assertArrayHasKey('struct', $row['struct_nested_with_map_of_list_of_ints']);
            $this->assertArrayHasKey('int', $row['struct_nested_with_map_of_list_of_ints']['struct']);
            $this->assertArrayHasKey('map_of_int_list_of_string', $row['struct_nested_with_map_of_list_of_ints']['struct']);
            $this->assertIsList($row['struct_nested_with_map_of_list_of_ints']['struct']['map_of_int_list_of_string'][0]);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_with_map_of_string_map_of_string_string_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/structs.parquet');

        $this->assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->type());
        $this->assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_string_map_of_string_string']) as $row) {
            $this->assertArrayHasKey('string', $row['struct_nested_with_map_of_string_map_of_string_string']);
            $this->assertArrayHasKey('struct', $row['struct_nested_with_map_of_string_map_of_string_string']);
            $this->assertArrayHasKey('int', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            $this->assertArrayHasKey('map_of_string_map_of_string_string', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            $this->assertArrayHasKey('outer_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']);
            $this->assertArrayHasKey('inner_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']['outer_key_0']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_with_map_of_string_map_of_string_string_column_with_limit() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/structs.parquet');

        $this->assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->type());
        $this->assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_string_map_of_string_string'], $limit = 50) as $row) {
            $this->assertArrayHasKey('string', $row['struct_nested_with_map_of_string_map_of_string_string']);
            $this->assertArrayHasKey('struct', $row['struct_nested_with_map_of_string_map_of_string_string']);
            $this->assertArrayHasKey('int', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            $this->assertArrayHasKey('map_of_string_map_of_string_string', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            $this->assertArrayHasKey('outer_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']);
            $this->assertArrayHasKey('inner_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']['outer_key_0']);
            $count++;
        }
        $this->assertSame($limit, $count);
    }

    public function test_reading_struct_with_list_and_map_of_structs_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/structs.parquet');

        $this->assertNull($file->metadata()->schema()->get('struct_with_list_and_map_of_structs')->type());
        $this->assertNull($file->metadata()->schema()->get('struct_with_list_and_map_of_structs')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_with_list_and_map_of_structs']) as $row) {
            $this->assertArrayHasKey('string', $row['struct_with_list_and_map_of_structs']);
            $this->assertArrayHasKey('struct', $row['struct_with_list_and_map_of_structs']);
            $this->assertArrayHasKey('int', $row['struct_with_list_and_map_of_structs']['struct']);
            $this->assertArrayHasKey('list_of_structs', $row['struct_with_list_and_map_of_structs']['struct']);
            $this->assertArrayHasKey('map_of_string_structs', $row['struct_with_list_and_map_of_structs']['struct']);
            $this->assertArrayHasKey('key_0', $row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']);
            $this->assertArrayHasKey('int', $row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']['key_0']);
            $this->assertArrayHasKey('list', $row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']['key_0']);
            $this->assertIsList($row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']['key_0']['list']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }
}
