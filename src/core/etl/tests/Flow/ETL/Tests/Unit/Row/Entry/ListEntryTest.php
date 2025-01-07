<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\{list_entry, type_boolean, type_datetime, type_int, type_list, type_string};
use function Flow\ETL\DSL\{list_schema, type_float, type_integer, type_object};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class ListEntryTest extends FlowTestCase
{
    public function test_create_with_empty_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        list_entry('', ['one', 'two', 'three'], type_list(type_string()));
    }

    public function test_creating_boolean_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<boolean> got different types: array<mixed>');

        list_entry('list', ['string', false], type_list(type_boolean()));
    }

    public function test_creating_datetime_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<object<DateTimeInterface>> got different types: array<mixed>');

        list_entry('list', ['string', new \DateTimeImmutable()], type_list(type_object(\DateTimeInterface::class)));
    }

    public function test_creating_float_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<float> got different types: array<mixed>');

        list_entry('list', ['string', 1.3], type_list(type_float()));
    }

    public function test_creating_integer_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<integer> got different types: array<mixed>');

        list_entry('list', ['string', 1], type_list(type_integer()));
    }

    public function test_creating_list_from_not_list_array() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<integer> got different types: map<string, integer>');

        list_entry('list', ['a' => 1, 'b' => 2], type_list(type_integer()));
    }

    public function test_creating_string_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<string> got different types: array<mixed>');

        list_entry('list', ['string', 1], type_list(type_string()));
    }

    public function test_definition() : void
    {
        self::assertEquals(
            list_schema('strings', type_list(type_string())),
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->definition()
        );
    }

    public function test_is_equal() : void
    {
        self::assertTrue(
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))
                ->isEqual(list_entry('strings', ['one', 'two', 'three'], type_list(type_string())))
        );
        self::assertFalse(
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))
                ->isEqual(list_entry('strings', [1, 2, 3], type_list(type_int())))
        );
        self::assertTrue(
            list_entry('strings', ['two', 'one', 'three'], type_list(type_string()))
                ->isEqual(list_entry('strings', ['one', 'two', 'three'], type_list(type_string())))
        );
    }

    public function test_map() : void
    {
        self::assertEquals(
            list_entry('strings', ['one, two, three'], type_list(type_string())),
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->map(fn (array $value) => [\implode(', ', $value)])
        );
    }

    public function test_rename() : void
    {
        self::assertEquals(
            list_entry('new_name', ['one', 'two', 'three'], type_list(type_string())),
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->rename('new_name')
        );
    }

    public function test_to_string() : void
    {
        self::assertEquals(
            '["one","two","three"]',
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->toString()
        );
    }

    public function test_to_string_date_time() : void
    {
        self::assertEquals(
            '[{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}]',
            list_entry('strings', [new \DateTimeImmutable('2021-01-01 00:00:00')], type_list(type_datetime()))->toString()
        );
    }

    public function test_type() : void
    {
        self::assertEquals(
            type_list(type_string()),
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->type()
        );
    }

    public function test_value() : void
    {
        self::assertEquals(
            ['one', 'two', 'three'],
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->value()
        );
    }
}
