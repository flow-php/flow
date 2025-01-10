<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\{map_entry, map_schema, type_boolean, type_datetime, type_float, type_integer, type_map, type_string};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class MapEntryTest extends FlowTestCase
{
    public function test_create_with_empty_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        map_entry('', ['one', 'two', 'three'], type_map(type_integer(), type_string()));
    }

    public function test_creating_boolean_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map<integer, boolean> got different types: array<mixed>');

        map_entry('map', ['string', false], type_map(type_integer(), type_boolean()));
    }

    public function test_creating_datetime_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map<integer, datetime> got different types: array<mixed>');

        map_entry('map', ['string', new \DateTimeImmutable()], type_map(type_integer(), type_datetime()));
    }

    public function test_creating_float_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map<integer, float> got different types: array<mixed>');

        map_entry('map', ['string', 1.3], type_map(type_integer(), type_float()));
    }

    public function test_creating_integer_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map<integer, integer> got different types: array<mixed>');

        map_entry('map', ['string', 1], type_map(type_integer(), type_integer()));
    }

    public function test_creating_map_from_not_map_array() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map<integer, integer> got different types: map<string, integer>');

        map_entry('map', ['a' => 1, 'b' => 2], type_map(type_integer(), type_integer()));
    }

    public function test_creating_string_map_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected map<integer, string> got different types: array<mixed>');

        map_entry('map', ['string', 1], type_map(type_integer(), type_string()));
    }

    public function test_definition() : void
    {
        self::assertEquals(
            map_schema('strings', type_map(type_integer(), type_string())),
            (map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string())))->definition()
        );
    }

    public function test_is_equal() : void
    {
        self::assertTrue(
            (map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string())))
                ->isEqual((map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string()))))
        );
        self::assertFalse(
            (map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string())))
                ->isEqual(map_entry('strings', [1, 2, 3], type_map(type_integer(), type_integer())))
        );
        self::assertTrue(
            (map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string())))
                ->isEqual((map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string()))))
        );
    }

    public function test_map() : void
    {
        self::assertEquals(
            (map_entry('strings', ['one, two, three'], type_map(type_integer(), type_string()))),
            (map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string())))->map(fn (array $value) : array => [\implode(', ', $value)])
        );
    }

    public function test_rename() : void
    {
        self::assertEquals(
            (map_entry('new_name', ['one', 'two', 'three'], type_map(type_integer(), type_string()))),
            (map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string())))->rename('new_name')
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            '["one","two","three"]',
            (map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string())))->toString()
        );
    }

    public function test_type() : void
    {
        self::assertEquals(
            type_map(type_integer(), type_string()),
            (map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string())))->type()
        );
    }

    public function test_value() : void
    {
        self::assertSame(
            ['one', 'two', 'three'],
            (map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string())))->value()
        );
        self::assertSame(
            ['one' => 'two'],
            (map_entry('strings', ['one' => 'two'], type_map(type_string(), type_string())))->value()
        );
    }
}
