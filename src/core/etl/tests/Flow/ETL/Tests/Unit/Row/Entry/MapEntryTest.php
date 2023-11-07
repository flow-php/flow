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

        new MapEntry('', MapKey::integer(), MapValue::string(), ['one', 'two', 'three']);
    }

    public function test_creating_boolean_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of boolean got different types');

        new MapEntry('map', MapKey::integer(), MapValue::boolean(), ['string', false]);
    }

    public function test_creating_datetime_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of object<DateTimeInterface> got different types.');

        new MapEntry('map', MapKey::integer(), MapValue::object(\DateTimeInterface::class), ['string', new \DateTimeImmutable()]);
    }

    public function test_creating_float_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of float got different types');

        new MapEntry('map', MapKey::integer(), MapValue::float(), ['string', 1.3]);
    }

    public function test_creating_integer_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of integer got different types');

        new MapEntry('map', MapKey::integer(), MapValue::integer(), ['string', 1]);
    }

    public function test_creating_map_from_not_map_array() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of integer got different types');

        new MapEntry('map', MapKey::integer(), MapValue::integer(), ['a' => 1, 'b' => 2]);
    }

    public function test_creating_string_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map of integer with value of string got different types');

        new MapEntry('map', MapKey::integer(), MapValue::string(), ['string', 1]);
    }

    public function test_definition() : void
    {
        $this->assertEquals(
            Definition::map('strings', MapKey::integer(), MapValue::string()),
            (new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one', 'two', 'three']))->definition()
        );
    }

    public function test_is_equal() : void
    {
        $this->assertTrue(
            (new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one', 'two', 'three']))
                ->isEqual((new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one', 'two', 'three'])))
        );
        $this->assertFalse(
            (new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one', 'two', 'three']))
                ->isEqual(new MapEntry('strings', MapKey::integer(), MapValue::integer(), [1, 2, 3]))
        );
        $this->assertTrue(
            (new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one', 'two', 'three']))
                ->isEqual((new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one', 'two', 'three'])))
        );
    }

    public function test_map() : void
    {
        $this->assertEquals(
            (new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one, two, three'])),
            (new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one', 'two', 'three']))->map(fn (array $value) => [\implode(', ', $value)])
        );
    }

    public function test_rename() : void
    {
        $this->assertEquals(
            (new MapEntry('new_name', MapKey::integer(), MapValue::string(), ['one', 'two', 'three'])),
            (new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one', 'two', 'three']))->rename('new_name')
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            '["one","two","three"]',
            (new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one', 'two', 'three']))->toString()
        );
    }

    public function test_type() : void
    {
        $this->assertEquals(
            new MapType(MapKey::integer(), MapValue::string()),
            (new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one', 'two', 'three']))->type()
        );
    }

    public function test_value() : void
    {
        $this->assertSame(
            ['one', 'two', 'three'],
            (new MapEntry('strings', MapKey::integer(), MapValue::string(), ['one', 'two', 'three']))->value()
        );
        $this->assertSame(
            ['one' => 'two'],
            (new MapEntry('strings', MapKey::string(), MapValue::string(), ['one' => 'two']))->value()
        );
    }
}
