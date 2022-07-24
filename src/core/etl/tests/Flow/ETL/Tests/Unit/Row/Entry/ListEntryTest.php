<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\TypedCollection\ObjectType;
use Flow\ETL\Row\Entry\TypedCollection\ScalarType;
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
        $this->expectExceptionMessage('Expected list of boolean got different types');

        new ListEntry('list', ScalarType::boolean, ['string', false]);
    }

    public function test_creating_datetime_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list of object<DateTimeInterface> got different types.');

        new ListEntry('list', new ObjectType(\DateTimeInterface::class), ['string', new \DateTimeImmutable()]);
    }

    public function test_creating_float_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list of float got different types');

        new ListEntry('list', ScalarType::float, ['string', 1.3]);
    }

    public function test_creating_integer_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list of integer got different types');

        new ListEntry('list', ScalarType::integer, ['string', 1]);
    }

    public function test_creating_list_from_not_list_array() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list of integer got array with not sequential integer indexes');

        new ListEntry('list', ScalarType::integer, ['a' => 1, 'b' => 2]);
    }

    public function test_creating_string_list_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list of string got different types');

        new ListEntry('list', ScalarType::string, ['string', 1]);
    }

    public function test_definition() : void
    {
        $this->assertEquals(
            Definition::list('strings', ScalarType::string, false),
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
            ScalarType::string,
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
