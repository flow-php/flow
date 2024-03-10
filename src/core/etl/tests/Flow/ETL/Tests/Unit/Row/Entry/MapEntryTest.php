<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\Map\{MapKey, MapValue};
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
        $this->expectExceptionMessage('Expected map<integer, boolean> got different types: array<mixed>');

        new MapEntry('map', ['string', false], new MapType(MapKey::integer(), MapValue::boolean()));
    }

    public function test_creating_datetime_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map<integer, datetime> got different types: array<mixed>');

        new MapEntry('map', ['string', new \DateTimeImmutable()], new MapType(MapKey::integer(), MapValue::datetime()));
    }

    public function test_creating_float_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map<integer, float> got different types: array<mixed>');

        new MapEntry('map', ['string', 1.3], new MapType(MapKey::integer(), MapValue::float()));
    }

    public function test_creating_integer_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map<integer, integer> got different types: array<mixed>');

        new MapEntry('map', ['string', 1], new MapType(MapKey::integer(), MapValue::integer()));
    }

    public function test_creating_map_from_not_map_array() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map<integer, integer> got different types: structure{a: integer, b: integer}');

        new MapEntry('map', ['a' => 1, 'b' => 2], new MapType(MapKey::integer(), MapValue::integer()));
    }

    public function test_creating_string_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map<integer, string> got different types: array<mixed>');

        new MapEntry('map', ['string', 1], new MapType(MapKey::integer(), MapValue::string()));
    }

    public function test_definition() : void
    {
        self::assertEquals(
            Definition::map('strings', new MapType(MapKey::integer(), MapValue::string())),
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->definition()
        );
    }

    public function test_is_equal() : void
    {
        self::assertTrue(
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))
                ->isEqual((new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string()))))
        );
        self::assertFalse(
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))
                ->isEqual(new MapEntry('strings', [1, 2, 3], new MapType(MapKey::integer(), MapValue::integer())))
        );
        self::assertTrue(
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))
                ->isEqual((new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string()))))
        );
    }

    public function test_map() : void
    {
        self::assertEquals(
            (new MapEntry('strings', ['one, two, three'], new MapType(MapKey::integer(), MapValue::string()))),
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->map(fn (array $value) => [\implode(', ', $value)])
        );
    }

    public function test_rename() : void
    {
        self::assertEquals(
            (new MapEntry('new_name', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string()))),
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->rename('new_name')
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            '["one","two","three"]',
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->toString()
        );
    }

    public function test_type() : void
    {
        self::assertEquals(
            new MapType(MapKey::integer(), MapValue::string()),
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->type()
        );
    }

    public function test_value() : void
    {
        self::assertSame(
            ['one', 'two', 'three'],
            (new MapEntry('strings', ['one', 'two', 'three'], new MapType(MapKey::integer(), MapValue::string())))->value()
        );
        self::assertSame(
            ['one' => 'two'],
            (new MapEntry('strings', ['one' => 'two'], new MapType(MapKey::string(), MapValue::string())))->value()
        );
    }
}
