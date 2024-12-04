<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;

final class MapsReadingTest extends TestCase
{
    public function test_reading_map_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        self::assertNull($file->metadata()->schema()->get('map')->type());
        self::assertEquals('MAP', $file->metadata()->schema()->get('map')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map']) as $row) {
            self::assertIsString(\array_key_first($row['map']));
            self::assertIsInt($row['map'][\array_key_first($row['map'])]);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        self::assertNull($file->metadata()->schema()->get('map_nullable')->type());
        self::assertEquals('MAP', $file->metadata()->schema()->get('map_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertIsString(\array_key_first($row['map_nullable']));
                self::assertIsInt($row['map_nullable'][\array_key_first($row['map_nullable'])]);
            } else {
                self::assertNull($row['map_nullable']);
            }
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_complex_lists() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        self::assertNull($file->metadata()->schema()->get('map_of_complex_lists')->type());
        self::assertEquals('MAP', $file->metadata()->schema()->get('map_of_complex_lists')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_complex_lists']) as $row) {
            self::assertIsArray($row['map_of_complex_lists']);
            self::assertIsArray($row['map_of_complex_lists']['key_0']);
            self::assertIsString($row['map_of_complex_lists']['key_0'][0]['string']);
            self::assertIsInt($row['map_of_complex_lists']['key_0'][0]['int']);
            self::assertIsBool($row['map_of_complex_lists']['key_0'][0]['bool']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_list_of_map_of_lists() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        self::assertNull($file->metadata()->schema()->get('map_of_list_of_map_of_lists')->type());
        self::assertEquals('MAP', $file->metadata()->schema()->get('map_of_list_of_map_of_lists')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_list_of_map_of_lists']) as $row) {
            self::assertIsArray($row['map_of_list_of_map_of_lists']);
            self::assertIsArray($row['map_of_list_of_map_of_lists']['key_0']);
            self::assertIsArray($row['map_of_list_of_map_of_lists']['key_0'][0]);
            self::assertIsArray($row['map_of_list_of_map_of_lists']['key_0'][0]['string_0_0_0']);
            self::assertIsList($row['map_of_list_of_map_of_lists']['key_0'][0]['string_0_0_0']);
            self::assertIsInt($row['map_of_list_of_map_of_lists']['key_0'][0]['string_0_0_0'][0]);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_lists() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        self::assertNull($file->metadata()->schema()->get('map_of_lists')->type());
        self::assertEquals('MAP', $file->metadata()->schema()->get('map_of_lists')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_lists']) as $row) {
            self::assertIsArray($row['map_of_lists']);
            self::assertIsArray($row['map_of_lists']['key_0']);
            self::assertIsList($row['map_of_lists']['key_0']);
            self::assertIsInt($row['map_of_lists']['key_0'][0]);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_maps_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        self::assertNull($file->metadata()->schema()->get('map_of_maps')->type());
        self::assertEquals('MAP', $file->metadata()->schema()->get('map_of_maps')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_maps']) as $row) {
            self::assertIsArray($row['map_of_maps']);
            self::assertIsArray($row['map_of_maps']['outer_key_0']);
            self::assertIsInt($row['map_of_maps']['outer_key_0']['inner_key_0']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_struct_of_structs_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        self::assertNull($file->metadata()->schema()->get('map_of_struct_of_structs')->type());
        self::assertEquals('MAP', $file->metadata()->schema()->get('map_of_struct_of_structs')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_struct_of_structs']) as $row) {
            self::assertIsArray($row['map_of_struct_of_structs']);
            self::assertIsArray($row['map_of_struct_of_structs']['key_0']);
            self::assertIsInt($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['int']);
            self::assertIsString($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['string']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_struct_of_structs_column_with_limit() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        self::assertNull($file->metadata()->schema()->get('map_of_struct_of_structs')->type());
        self::assertEquals('MAP', $file->metadata()->schema()->get('map_of_struct_of_structs')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_struct_of_structs'], $limit = 50) as $row) {
            self::assertIsArray($row['map_of_struct_of_structs']);
            self::assertIsArray($row['map_of_struct_of_structs']['key_0']);
            self::assertIsInt($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['int']);
            self::assertIsString($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['string']);
            $count++;
        }
        self::assertSame($limit, $count);
    }

    public function test_reading_map_of_structs_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/maps.parquet');

        self::assertNull($file->metadata()->schema()->get('map_of_structs')->type());
        self::assertEquals('MAP', $file->metadata()->schema()->get('map_of_structs')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_structs']) as $row) {
            self::assertIsArray($row['map_of_structs']);
            self::assertIsArray($row['map_of_structs']['key_0']);
            self::assertIsInt($row['map_of_structs']['key_0']['int_field']);
            self::assertIsString($row['map_of_structs']['key_0']['string_field']);
            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
