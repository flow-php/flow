<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Schema\Definition;
use PHPUnit\Framework\TestCase;

final class ListEntryTest extends TestCase
{
    public function test_create_with_empty_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        Entry::list_of_string('', ['one', 'two', 'three']);
    }

    public function test_creating_boolean_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<boolean> got different types: array<mixed>');

        new ListEntry('list', ['string', false], new ListType(ListElement::boolean()));
    }

    public function test_creating_datetime_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<object<DateTimeInterface>> got different types: array<mixed>');

        new ListEntry('list', ['string', new \DateTimeImmutable()], new ListType(ListElement::object(\DateTimeInterface::class)));
    }

    public function test_creating_float_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<float> got different types: array<mixed>');

        new ListEntry('list', ['string', 1.3], new ListType(ListElement::float()));
    }

    public function test_creating_integer_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<integer> got different types: array<mixed>');

        new ListEntry('list', ['string', 1], new ListType(ListElement::integer()));
    }

    public function test_creating_list_from_not_list_array() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<integer> got different types: structure{a: integer, b: integer}');

        new ListEntry('list', ['a' => 1, 'b' => 2], new ListType(ListElement::integer()));
    }

    public function test_creating_string_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<string> got different types: array<mixed>');

        new ListEntry('list', ['string', 1], new ListType(ListElement::string()));
    }

    public function test_definition() : void
    {
        $this->assertEquals(
            Definition::list('strings', new ListType(ListElement::string())),
            Entry::list_of_string('strings', ['one', 'two', 'three'])->definition()
        );
    }

    public function test_is_equal() : void
    {
        $this->assertTrue(
            Entry::list_of_string('strings', ['one', 'two', 'three'])
                ->isEqual(Entry::list_of_string('strings', ['one', 'two', 'three']))
        );
        $this->assertFalse(
            Entry::list_of_string('strings', ['one', 'two', 'three'])
                ->isEqual(Entry::list_of_int('strings', [1, 2, 3]))
        );
        $this->assertTrue(
            Entry::list_of_string('strings', ['two', 'one', 'three'])
                ->isEqual(Entry::list_of_string('strings', ['one', 'two', 'three']))
        );
    }

    public function test_map() : void
    {
        $this->assertEquals(
            Entry::list_of_string('strings', ['one, two, three']),
            Entry::list_of_string('strings', ['one', 'two', 'three'])->map(fn (array $value) => [\implode(', ', $value)])
        );
    }

    public function test_rename() : void
    {
        $this->assertEquals(
            Entry::list_of_string('new_name', ['one', 'two', 'three']),
            Entry::list_of_string('strings', ['one', 'two', 'three'])->rename('new_name')
        );
    }

    public function test_to_string() : void
    {
        $this->assertEquals(
            '["one","two","three"]',
            Entry::list_of_string('strings', ['one', 'two', 'three'])->toString()
        );
    }

    public function test_to_string_date_time() : void
    {
        $this->assertEquals(
            '[{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}]',
            Entry::list_of_datetime('strings', [new \DateTimeImmutable('2021-01-01 00:00:00')])->toString()
        );
    }

    public function test_type() : void
    {
        $this->assertEquals(
            new ListType(ListElement::string()),
            Entry::list_of_string('strings', ['one', 'two', 'three'])->type()
        );
    }

    public function test_value() : void
    {
        $this->assertEquals(
            ['one', 'two', 'three'],
            Entry::list_of_string('strings', ['one', 'two', 'three'])->value()
        );
    }
}
