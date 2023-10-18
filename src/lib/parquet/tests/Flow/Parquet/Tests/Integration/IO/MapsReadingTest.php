<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Reader;

final class MapsReadingTest extends ParquetFunctionalTestCase
{
    public function test_reading_map_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/maps.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('map')->type());
        $this->assertEquals('MAP', $file->metadata()->schema()->get('map')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map']) as $row) {
            $this->assertIsString(\array_key_first($row['map']));
            $this->assertIsInt($row['map'][\array_key_first($row['map'])]);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_nullable_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/maps.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('map_nullable')->type());
        $this->assertEquals('MAP', $file->metadata()->schema()->get('map_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsString(\array_key_first($row['map_nullable']));
                $this->assertIsInt($row['map_nullable'][\array_key_first($row['map_nullable'])]);
            } else {
                $this->assertNull($row['map_nullable']);
            }
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_complex_lists() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/maps.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('map_of_complex_lists')->type());
        $this->assertEquals('MAP', $file->metadata()->schema()->get('map_of_complex_lists')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_complex_lists']) as $row) {
            $this->assertIsArray($row['map_of_complex_lists']);
            $this->assertIsArray($row['map_of_complex_lists']['key_0']);
            $this->assertIsString($row['map_of_complex_lists']['key_0']['string'][0]);
            $this->assertIsInt($row['map_of_complex_lists']['key_0']['int'][0]);
            $this->assertIsBool($row['map_of_complex_lists']['key_0']['bool'][0]);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_list_of_map_of_lists() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/maps.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('map_of_list_of_map_of_lists')->type());
        $this->assertEquals('MAP', $file->metadata()->schema()->get('map_of_list_of_map_of_lists')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_list_of_map_of_lists']) as $row) {
            $this->assertIsArray($row['map_of_list_of_map_of_lists']);
            $this->assertIsArray($row['map_of_list_of_map_of_lists']['key_0']);
            $this->assertIsArray($row['map_of_list_of_map_of_lists']['key_0'][0]);
            $this->assertIsArray($row['map_of_list_of_map_of_lists']['key_0'][0]['string_0_0_0']);
            $this->assertIsList($row['map_of_list_of_map_of_lists']['key_0'][0]['string_0_0_0']);
            $this->assertIsInt($row['map_of_list_of_map_of_lists']['key_0'][0]['string_0_0_0'][0]);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_lists() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/maps.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('map_of_lists')->type());
        $this->assertEquals('MAP', $file->metadata()->schema()->get('map_of_lists')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_lists']) as $row) {
            $this->assertIsArray($row['map_of_lists']);
            $this->assertIsArray($row['map_of_lists']['key_0']);
            $this->assertIsList($row['map_of_lists']['key_0']);
            $this->assertIsInt($row['map_of_lists']['key_0'][0]);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_maps_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/maps.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('map_of_maps')->type());
        $this->assertEquals('MAP', $file->metadata()->schema()->get('map_of_maps')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_maps']) as $row) {
            $this->assertIsArray($row['map_of_maps']);
            $this->assertIsArray($row['map_of_maps']['outer_key_0']);
            $this->assertIsInt($row['map_of_maps']['outer_key_0']['inner_key_0']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_struct_of_structs_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/maps.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('map_of_struct_of_structs')->type());
        $this->assertEquals('MAP', $file->metadata()->schema()->get('map_of_struct_of_structs')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_struct_of_structs']) as $row) {
            $this->assertIsArray($row['map_of_struct_of_structs']);
            $this->assertIsArray($row['map_of_struct_of_structs']['key_0']);
            $this->assertIsInt($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['int']);
            $this->assertIsString($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['string']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_map_of_struct_of_structs_column_with_limit() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/maps.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('map_of_struct_of_structs')->type());
        $this->assertEquals('MAP', $file->metadata()->schema()->get('map_of_struct_of_structs')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_struct_of_structs'], $limit = 50) as $row) {
            $this->assertIsArray($row['map_of_struct_of_structs']);
            $this->assertIsArray($row['map_of_struct_of_structs']['key_0']);
            $this->assertIsInt($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['int']);
            $this->assertIsString($row['map_of_struct_of_structs']['key_0']['struct']['nested_struct']['string']);
            $count++;
        }
        $this->assertSame($limit, $count);
    }

    public function test_reading_map_of_structs_column() : void
    {
        $reader = new Reader(logger: $this->getLogger());
        $file = $reader->read(__DIR__ . '/../../Fixtures/maps.parquet');

        $this->assertEquals(PhysicalType::BOOLEAN, $file->metadata()->schema()->get('map_of_structs')->type());
        $this->assertEquals('MAP', $file->metadata()->schema()->get('map_of_structs')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['map_of_structs']) as $row) {
            $this->assertIsArray($row['map_of_structs']);
            $this->assertIsArray($row['map_of_structs']['key_0']);
            $this->assertIsInt($row['map_of_structs']['key_0']['int_field']);
            $this->assertIsString($row['map_of_structs']['key_0']['string_field']);
            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }
}
