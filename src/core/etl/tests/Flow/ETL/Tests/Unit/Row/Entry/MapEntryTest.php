<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\map_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class MapEntryTest extends FlowTestCase
{
    public function test_create_with_empty_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        map_entry('', ['one', 'two', 'three'], type_map(type_integer(), type_string()));
    }

    public function test_definition(): void
    {
        static::assertEquals(
            map_schema('strings', type_map(type_integer(), type_string())),
            map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string()))->definition(),
        );
    }

    public function test_is_equal(): void
    {
        static::assertTrue(map_entry(
            'strings',
            ['one', 'two', 'three'],
            type_map(type_integer(), type_string()),
        )->isEqual(map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string()))));
        static::assertFalse(map_entry(
            'strings',
            ['one', 'two', 'three'],
            type_map(type_integer(), type_string()),
        )->isEqual(map_entry('strings', [1, 2, 3], type_map(type_integer(), type_integer()))));
        static::assertTrue(map_entry(
            'strings',
            ['one', 'two', 'three'],
            type_map(type_integer(), type_string()),
        )->isEqual(map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string()))));
    }

    public function test_rename(): void
    {
        static::assertEquals(
            map_entry('new_name', ['one', 'two', 'three'], type_map(type_integer(), type_string())),
            map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string()))->rename('new_name'),
        );
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = map_entry('old_name', ['one', 'two', 'three'], type_map(type_integer(), type_string()), $metadata);

        $renamedEntry = $entry->rename('new_name');

        static::assertSame('new_name', $renamedEntry->name());
        static::assertEquals(['one', 'two', 'three'], $renamedEntry->value());
        static::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_to_string(): void
    {
        static::assertSame(
            '["one","two","three"]',
            map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string()))->toString(),
        );
    }

    public function test_type(): void
    {
        static::assertEquals(
            type_map(type_integer(), type_string()),
            map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string()))->type(),
        );
    }

    public function test_value(): void
    {
        static::assertSame(
            ['one', 'two', 'three'],
            map_entry('strings', ['one', 'two', 'three'], type_map(type_integer(), type_string()))->value(),
        );
        static::assertSame(
            ['one' => 'two'],
            map_entry('strings', ['one' => 'two'], type_map(type_string(), type_string()))->value(),
        );
    }
}
