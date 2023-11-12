<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Schema\Definition;
use PHPUnit\Framework\TestCase;

final class MapEntryTest extends TestCase
{
    public function test_create_with_empty_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        new MapEntry('', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string()));
    }

    public function test_creating_boolean_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of boolean got different types');

        new MapEntry('map', ['string', false], new MapType(MapKey::integer(), MapValue::boolean()));
    }

    public function test_creating_datetime_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of object<DateTimeInterface> got different types.');

        new MapEntry('map', ['string', new \DateTimeImmutable()], new MapType(MapKey::integer(), MapValue::object(\DateTimeInterface::class)));
    }

    public function test_creating_float_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of float got different types');

        new MapEntry('map', ['string', 1.3], new MapType(MapKey::integer(), MapValue::float()));
    }

    public function test_creating_integer_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of integer got different types');

        new MapEntry('map', ['string', 1], new MapType(MapKey::integer(), MapValue::integer()));
    }

    public function test_creating_map_from_not_map_array() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of integer got different types');

        new MapEntry('map', ['a' => 1, 'b' => 2], new MapType(MapKey::integer(), MapValue::integer()));
    }

    public function test_creating_string_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of string got different types');

        new MapEntry('map', ['string', 1], new MapType(MapKey::integer(), MapValue::string()));
    }

    public function test_definition() : void
    {
        $this->assertEquals(
            Definition::map('strings', new MapType(MapKey::integer(), MapValue::string())),
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->definition()
        );
    }

    public function test_is_equal() : void
    {
        $this->assertTrue(
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))
                ->isEqual((new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string()))))
        );
        $this->assertFalse(
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))
                ->isEqual(new MapEntry('strings', [1, 2, 3], new MapType(MapKey::integer(), MapValue::integer())))
        );
        $this->assertTrue(
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))
                ->isEqual((new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string()))))
        );
    }

    public function test_map() : void
    {
        $this->assertEquals(
            (new MapEntry('strings', ['one, two, three'], new MapType(MapKey::integer(), MapValue::string()))),
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->map(fn (array $value) => [\implode(', ', $value)])
        );
    }

    public function test_rename() : void
    {
        $this->assertEquals(
            (new MapEntry('new_name', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string()))),
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->rename('new_name')
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            '["one","two","three"]',
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->toString()
        );
    }

    public function test_type() : void
    {
        $this->assertEquals(
            new MapType(MapKey::integer(), MapValue::string()),
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->type()
        );
    }

    public function test_value() : void
    {
        $this->assertSame(
            ['one', 'two', 'three'],
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->value()
        );
        $this->assertSame(
            ['one' => 'two'],
            (new MapEntry('strings', ['one' => 'two'], new MapType(MapKey::string(), MapValue::string())))->value()
        );
    }
}
