<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\ParquetFile\Schema\{FlatColumn, MapKey, MapValue, NestedColumn};
use PHPUnit\Framework\TestCase;

final class FlatColumnTest extends TestCase
{
    public function test_is_map_element_on_a_column_without_parent() : void
    {
        self::assertFalse(FlatColumn::int32('int32')->isMapElement());
    }

    public function test_is_map_element_on_a_map_column() : void
    {
        self::assertFalse(NestedColumn::map('map', MapKey::int32(), MapValue::string())->isMapElement());
    }

    public function test_is_map_element_on_map_key_and_value() : void
    {
        self::assertTrue(NestedColumn::map('map', MapKey::int32(), MapValue::string())->getMapKeyColumn()->isMapElement());
        self::assertTrue(NestedColumn::map('map', MapKey::int32(), MapValue::string())->getMapValueColumn()->isMapElement());
    }

    public function test_is_map_on_a_map_column() : void
    {
        self::assertTrue(NestedColumn::map('map', MapKey::int32(), MapValue::string())->isMap());
    }

    public function test_is_map_on_a_non_map_column() : void
    {
        self::assertFalse(FlatColumn::int32('int32')->isMap());
    }
}
