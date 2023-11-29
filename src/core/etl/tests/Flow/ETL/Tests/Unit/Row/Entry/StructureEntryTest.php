<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_array;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Schema\Definition;
use PHPUnit\Framework\TestCase;

final class StructureEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and equal simple same integer entries' => [
            true,
            new StructureEntry(
                'name',
                ['1' => 1, '2' => 2, '3' => 3],
                struct_type(struct_element('1', type_int()), struct_element('2', type_int()), struct_element('3', type_int()))
            ),
            new StructureEntry(
                'name',
                ['1' => 1, '2' => 2, '3' => 3],
                struct_type(struct_element('1', type_int()), struct_element('2', type_int()), struct_element('3', type_int()))
            ),
        ];
        yield 'equal names and equal simple same integer entries with different number of entries' => [
            false,
            new StructureEntry(
                'name',
                ['1' => 1, '2' => 2, '3' => 3],
                struct_type(struct_element('1', type_int()), struct_element('2', type_string()), struct_element('3', type_string()))
            ),
            new StructureEntry(
                'name',
                ['1' => 1, '2' => 2],
                struct_type(struct_element('1', type_int()), struct_element('2', type_string()))
            ),
        ];
        yield 'equal names and equal simple same integer entries with different number of entries reversed' => [
            false,
            new StructureEntry(
                'name',
                ['1' => 1, '2' => 2],
                struct_type(struct_element('1', type_int()), struct_element('2', type_string()))
            ),
            new StructureEntry(
                'name',
                ['1' => 1, '2' => 2, '3' => 3],
                struct_type(struct_element('1', type_int()), struct_element('2', type_string()), struct_element('3', type_string()))
            ),
        ];
        yield 'equal names and equal simple same array entries' => [
            true,
            new StructureEntry(
                'name',
                ['json' => ['foo' => ['bar' => 'baz']]],
                struct_type(struct_element('json', new MapType(MapKey::string(), MapValue::map(new MapType(MapKey::string(), MapValue::string())))))
            ),
            new StructureEntry(
                'name',
                ['json' => ['foo' => ['bar' => 'baz']]],
                struct_type(struct_element('json', new MapType(MapKey::string(), MapValue::map(new MapType(MapKey::string(), MapValue::string())))))
            ),
        ];
        yield 'equal names and equal simple same collection entries' => [
            true,
            new StructureEntry(
                'name',
                ['json' => ['1' => 1, '2' => 2, '3' => 3]],
                struct_type(struct_element('json', type_array()))
            ),
            new StructureEntry(
                'name',
                ['json' => ['1' => 1, '2' => 2, '3' => 3]],
                struct_type(struct_element('json', type_array()))
            ),
        ];
        yield 'equal names and equal simple different collection entries' => [
            false,
            new StructureEntry(
                'name',
                ['json' => ['5' => 5, '2' => 2, '1' => 1]],
                struct_type(struct_element('json', type_array()))
            ),
            new StructureEntry(
                'name',
                ['json' => ['1' => 1, '2' => 2, '3' => 3]],
                struct_type(struct_element('json', type_array()))
            ),
        ];
    }

    public function test_creating_string_structure_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected structure{id: integer, name: string} got different types: list<integer>');

        new StructureEntry(
            'test',
            [1, 2, 3],
            struct_type(struct_element('id', type_int()), struct_element('name', type_string()))
        );
    }

    public function test_definition() : void
    {
        $entry = struct_entry(
            'items',
            [
                'id' => 1,
                'name' => 'one',
                'address' => [
                    'street' => 'foo',
                    'city' => 'bar',
                ],
            ],
            struct_type(
                struct_element('id', type_int()),
                struct_element('name', type_string()),
                struct_element(
                    'address',
                    struct_type(
                        struct_element('street', type_string()),
                        struct_element('city', type_string()),
                    )
                ),
            ),
        );

        $this->assertEquals(
            Definition::structure(
                'items',
                struct_type(
                    struct_element('id', type_int()),
                    struct_element('name', type_string()),
                    struct_element(
                        'address',
                        struct_type(
                            struct_element('street', type_string()),
                            struct_element('city', type_string()),
                        )
                    )
                ),
            ),
            $entry->definition()
        );
    }

    public function test_entry_name_can_be_zero() : void
    {
        $this->assertSame(
            '0',
            (
                new StructureEntry(
                    '0',
                    ['id' => 1, 'name' => 'one'],
                    struct_type(struct_element('id', type_int()), struct_element('name', type_string()))
                )
            )->name()
        );
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, StructureEntry $entry, StructureEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = new StructureEntry(
            'entry-name',
            ['id' => 1234],
            struct_type(struct_element('id', type_int()))
        );

        $this->assertEquals(
            $entry,
            $entry->map(fn (array $entries) => $entries)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        new StructureEntry(
            '',
            ['id' => 1, 'name' => 'one'],
            struct_type(struct_element('id', type_int()), struct_element('name', type_string()))
        );
    }

    public function test_renames_entry() : void
    {
        $entry = new StructureEntry(
            'entry-name',
            ['id' => 1234],
            struct_type(struct_element('id', type_int()))
        );
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertEquals($entry->value(), $newEntry->value());
    }

    public function test_returns_array_as_value() : void
    {
        $entry = new StructureEntry(
            'items',
            ['item-id' => 1, 'name' => 'one'],
            struct_type(struct_element('id', type_int()), struct_element('name', type_string()))
        );

        $this->assertEquals(
            [
                'item-id' => 1,
                'name' => 'one',
            ],
            $entry->value()
        );
    }

    public function test_serialization() : void
    {
        $string = new StructureEntry(
            'name',
            ['json' => ['5' => 5, '2' => 2, '3' => 3]],
            struct_type(struct_element('json', type_array()))
        );

        $serialized = \serialize($string);
        /** @var StructureEntry $unserialized */
        $unserialized = \unserialize($serialized);

        $this->assertTrue($string->isEqual($unserialized));
    }
}
