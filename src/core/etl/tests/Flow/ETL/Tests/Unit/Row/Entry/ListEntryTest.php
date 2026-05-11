<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\list_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class ListEntryTest extends FlowTestCase
{
    public function test_create_with_empty_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        list_entry('', ['one', 'two', 'three'], type_list(type_string()));
    }

    public function test_creating_boolean_list_from_wrong_value_types(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<boolean> got different types: array<mixed>');

        list_entry('list', ['string', false], type_list(type_boolean()));
    }

    public function test_creating_datetime_list_from_wrong_value_types(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<object<DateTimeInterface>> got different types: array<mixed>');

        list_entry(
            'list',
            ['string', new \DateTimeImmutable()],
            type_list(type_instance_of(\DateTimeInterface::class)),
        );
    }

    public function test_creating_float_list_from_wrong_value_types(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<float> got different types: array<mixed>');

        list_entry('list', ['string', 1.3], type_list(type_float()));
    }

    public function test_creating_integer_list_from_wrong_value_types(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<integer> got different types: array<mixed>');

        list_entry('list', ['string', 1], type_list(type_integer()));
    }

    public function test_creating_list_from_not_list_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<integer> got different types: map<string, integer>');

        /** @phpstan-ignore-next-line */
        list_entry('list', ['a' => 1, 'b' => 2], type_list(type_integer()));
    }

    public function test_creating_string_list_from_wrong_value_types(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected list<string> got different types: array<mixed>');

        list_entry('list', ['string', 1], type_list(type_string()));
    }

    public function test_definition(): void
    {
        static::assertEquals(
            list_schema('strings', type_list(type_string())),
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->definition(),
        );
    }

    public function test_duplicating_entry(): void
    {
        $entry = list_entry('strings', ['one', 'two', 'three'], type_list(type_string()));
        $duplicated = $entry->duplicate();

        static::assertNotSame($entry, $duplicated);
        static::assertEquals($entry, $duplicated);
    }

    public function test_is_equal(): void
    {
        static::assertTrue(list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->isEqual(list_entry(
            'strings',
            ['one', 'two', 'three'],
            type_list(type_string()),
        )));
        static::assertFalse(list_entry(
            'strings',
            ['one', 'two', 'three'],
            type_list(type_string()),
        )->isEqual(list_entry('strings', [1, 2, 3], type_list(type_integer()))));
        static::assertTrue(list_entry('strings', ['two', 'one', 'three'], type_list(type_string()))->isEqual(list_entry(
            'strings',
            ['one', 'two', 'three'],
            type_list(type_string()),
        )));
    }

    public function test_map(): void
    {
        static::assertEquals(
            list_entry('strings', ['one, two, three'], type_list(type_string())),
            list_entry(
                'strings',
                ['one', 'two', 'three'],
                type_list(type_string()),
            )->map(static fn(array $value): array => [\implode(', ', $value)]),
        );
    }

    public function test_rename(): void
    {
        static::assertEquals(
            list_entry('new_name', ['one', 'two', 'three'], type_list(type_string())),
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->rename('new_name'),
        );
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = list_entry('old_name', ['one', 'two', 'three'], type_list(type_string()), $metadata);

        $renamedEntry = $entry->rename('new_name');

        static::assertSame('new_name', $renamedEntry->name());
        static::assertEquals(['one', 'two', 'three'], $renamedEntry->value());
        static::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_to_string(): void
    {
        static::assertEquals(
            '["one","two","three"]',
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->toString(),
        );
    }

    public function test_to_string_date_time(): void
    {
        static::assertEquals(
            '[{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}]',
            list_entry(
                'strings',
                [new \DateTimeImmutable('2021-01-01 00:00:00')],
                type_list(type_datetime()),
            )->toString(),
        );
    }

    public function test_type(): void
    {
        static::assertEquals(
            type_list(type_string()),
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->type(),
        );
    }

    public function test_value(): void
    {
        static::assertEquals(
            ['one', 'two', 'three'],
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->value(),
        );
    }
}
