<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;

final class StructsReadingTest extends TestCase
{
    public function test_reading_struct_deeply_nested_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        self::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->type());
        self::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_deeply_nested']) as $row) {
            self::assertArrayHasKey('struct_0', $row['struct_deeply_nested']);
            self::assertArrayHasKey('int', $row['struct_deeply_nested']['struct_0']);
            self::assertArrayHasKey('struct_1', $row['struct_deeply_nested']['struct_0']);
            self::assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']);
            self::assertArrayHasKey('struct_2', $row['struct_deeply_nested']['struct_0']['struct_1']);
            self::assertArrayHasKey('bool', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            self::assertArrayHasKey('struct_3', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            self::assertArrayHasKey('float', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            self::assertArrayHasKey('struct_4', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            self::assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            self::assertArrayHasKey('json', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_deeply_nested_column_with_limit() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        self::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->type());
        self::assertNull($file->metadata()->schema()->get('struct_deeply_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_deeply_nested'], $limit = 50) as $row) {
            self::assertArrayHasKey('struct_0', $row['struct_deeply_nested']);
            self::assertArrayHasKey('int', $row['struct_deeply_nested']['struct_0']);
            self::assertArrayHasKey('struct_1', $row['struct_deeply_nested']['struct_0']);
            self::assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']);
            self::assertArrayHasKey('struct_2', $row['struct_deeply_nested']['struct_0']['struct_1']);
            self::assertArrayHasKey('bool', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            self::assertArrayHasKey('struct_3', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']);
            self::assertArrayHasKey('float', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            self::assertArrayHasKey('struct_4', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']);
            self::assertArrayHasKey('string', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            self::assertArrayHasKey('json', $row['struct_deeply_nested']['struct_0']['struct_1']['struct_2']['struct_3']['struct_4']);
            $count++;
        }
        self::assertSame($limit, $count);
    }

    public function test_reading_struct_flat_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        self::assertNull($file->metadata()->schema()->get('struct_flat')->type());
        self::assertNull($file->metadata()->schema()->get('struct_flat')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_flat']) as $row) {
            self::assertArrayHasKey('string', $row['struct_flat']);
            self::assertArrayHasKey('int', $row['struct_flat']);
            self::assertArrayHasKey('list_of_ints', $row['struct_flat']);
            self::assertArrayHasKey('map_of_string_int', $row['struct_flat']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_flat_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        self::assertNull($file->metadata()->schema()->get('struct_flat_nullable')->type());
        self::assertNull($file->metadata()->schema()->get('struct_flat_nullable')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_flat_nullable']) as $i => $row) {
            if ($i % 2 === 0) {
                self::assertArrayHasKey('string', $row['struct_flat_nullable']);
                self::assertArrayHasKey('int', $row['struct_flat_nullable']);
                self::assertArrayHasKey('list_of_ints', $row['struct_flat_nullable']);
                self::assertArrayHasKey('map_of_string_int', $row['struct_flat_nullable']);
            } else {
                self::assertNull($row['struct_flat_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        self::assertNull($file->metadata()->schema()->get('struct_nested')->type());
        self::assertNull($file->metadata()->schema()->get('struct_nested')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested']) as $row) {
            self::assertArrayHasKey('string', $row['struct_nested']);
            self::assertArrayHasKey('struct_flat', $row['struct_nested']);
            self::assertArrayHasKey('int', $row['struct_nested']['struct_flat']);
            self::assertArrayHasKey('list_of_ints', $row['struct_nested']['struct_flat']);
            self::assertArrayHasKey('map_of_string_int', $row['struct_nested']['struct_flat']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_with_list_of_lists_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        self::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_lists')->type());
        self::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_lists')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_list_of_lists']) as $row) {
            self::assertArrayHasKey('string', $row['struct_nested_with_list_of_lists']);
            self::assertArrayHasKey('struct', $row['struct_nested_with_list_of_lists']);
            self::assertArrayHasKey('int', $row['struct_nested_with_list_of_lists']['struct']);
            self::assertArrayHasKey('list_of_list_of_ints', $row['struct_nested_with_list_of_lists']['struct']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_with_list_of_maps_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        self::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_maps')->type());
        self::assertNull($file->metadata()->schema()->get('struct_nested_with_list_of_maps')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_list_of_maps']) as $row) {
            self::assertArrayHasKey('string', $row['struct_nested_with_list_of_maps']);
            self::assertArrayHasKey('struct', $row['struct_nested_with_list_of_maps']);
            self::assertArrayHasKey('int', $row['struct_nested_with_list_of_maps']['struct']);
            self::assertArrayHasKey('list_of_map_of_string_int', $row['struct_nested_with_list_of_maps']['struct']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_with_map_of_list_of_ints_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        self::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_list_of_ints')->type());
        self::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_list_of_ints')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_list_of_ints']) as $row) {
            self::assertArrayHasKey('string', $row['struct_nested_with_map_of_list_of_ints']);
            self::assertArrayHasKey('struct', $row['struct_nested_with_map_of_list_of_ints']);
            self::assertArrayHasKey('int', $row['struct_nested_with_map_of_list_of_ints']['struct']);
            self::assertArrayHasKey('map_of_int_list_of_string', $row['struct_nested_with_map_of_list_of_ints']['struct']);
            self::assertIsList($row['struct_nested_with_map_of_list_of_ints']['struct']['map_of_int_list_of_string'][0]);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_with_map_of_string_map_of_string_string_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        self::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->type());
        self::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_string_map_of_string_string']) as $row) {
            self::assertArrayHasKey('string', $row['struct_nested_with_map_of_string_map_of_string_string']);
            self::assertArrayHasKey('struct', $row['struct_nested_with_map_of_string_map_of_string_string']);
            self::assertArrayHasKey('int', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            self::assertArrayHasKey('map_of_string_map_of_string_string', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            self::assertArrayHasKey('outer_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']);
            self::assertArrayHasKey('inner_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']['outer_key_0']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_struct_nested_with_map_of_string_map_of_string_string_column_with_limit() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        self::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->type());
        self::assertNull($file->metadata()->schema()->get('struct_nested_with_map_of_string_map_of_string_string')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_nested_with_map_of_string_map_of_string_string'], $limit = 50) as $row) {
            self::assertArrayHasKey('string', $row['struct_nested_with_map_of_string_map_of_string_string']);
            self::assertArrayHasKey('struct', $row['struct_nested_with_map_of_string_map_of_string_string']);
            self::assertArrayHasKey('int', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            self::assertArrayHasKey('map_of_string_map_of_string_string', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']);
            self::assertArrayHasKey('outer_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']);
            self::assertArrayHasKey('inner_key_0', $row['struct_nested_with_map_of_string_map_of_string_string']['struct']['map_of_string_map_of_string_string']['outer_key_0']);
            $count++;
        }
        self::assertSame($limit, $count);
    }

    public function test_reading_struct_with_list_and_map_of_structs_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/structs.parquet');

        self::assertNull($file->metadata()->schema()->get('struct_with_list_and_map_of_structs')->type());
        self::assertNull($file->metadata()->schema()->get('struct_with_list_and_map_of_structs')->logicalType());

        $count = 0;

        foreach ($file->values(['struct_with_list_and_map_of_structs']) as $row) {
            self::assertArrayHasKey('string', $row['struct_with_list_and_map_of_structs']);
            self::assertArrayHasKey('struct', $row['struct_with_list_and_map_of_structs']);
            self::assertArrayHasKey('int', $row['struct_with_list_and_map_of_structs']['struct']);
            self::assertArrayHasKey('list_of_structs', $row['struct_with_list_and_map_of_structs']['struct']);
            self::assertArrayHasKey('map_of_string_structs', $row['struct_with_list_and_map_of_structs']['struct']);
            self::assertArrayHasKey('key_0', $row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']);
            self::assertArrayHasKey('int', $row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']['key_0']);
            self::assertArrayHasKey('list', $row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']['key_0']);
            self::assertIsList($row['struct_with_list_and_map_of_structs']['struct']['map_of_string_structs']['key_0']['list']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
