<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\list_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ListEntryTest extends FlowTestCase
{
    public function test_create_with_empty_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');
        list_entry('', ['one', 'two', 'three'], type_list(type_string()));
    }

    public function test_definition(): void
    {
        static::assertEquals(
            list_schema('strings', type_list(type_string())),
            list_entry('strings', ['one', 'two', 'three'], type_list(type_string()))->definition(),
        );
    }

    public function test_is_equal_follows_structure_field_order_in_the_element_type(): void
    {
        static::assertFalse(list_entry(
            'items',
            [['a' => 1, 'b' => 'x']],
            type_list(type_structure(['a' => type_integer(), 'b' => type_string()])),
        )->isEqual(list_entry(
            'items',
            [['b' => 'x', 'a' => 1]],
            type_list(type_structure(['b' => type_string(), 'a' => type_integer()])),
        )));
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
                [new DateTimeImmutable('2021-01-01 00:00:00')],
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
