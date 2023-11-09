<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\DSL\Entry;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ScalarType;
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
                new StructureType(new StructureElement('1', ScalarType::integer()), new StructureElement('2', ScalarType::string()), new StructureElement('3', ScalarType::string()))
            ),
            new StructureEntry(
                'name',
                ['1' => 1, '2' => 2, '3' => 3],
                new StructureType(new StructureElement('1', ScalarType::integer()), new StructureElement('2', ScalarType::string()), new StructureElement('3', ScalarType::string()))
            ),
        ];
        yield 'equal names and equal simple same integer entries with different number of entries' => [
            false,
            new StructureEntry(
                'name',
                ['1' => 1, '2' => 2, '3' => 3],
                new StructureType(new StructureElement('1', ScalarType::integer()), new StructureElement('2', ScalarType::string()), new StructureElement('3', ScalarType::string()))
            ),
            new StructureEntry(
                'name',
                ['1' => 1, '2' => 2],
                new StructureType(new StructureElement('1', ScalarType::integer()), new StructureElement('2', ScalarType::string()))
            ),
        ];
        yield 'equal names and equal simple same integer entries with different number of entries reversed' => [
            false,
            new StructureEntry(
                'name',
                ['1' => 1, '2' => 2],
                new StructureType(new StructureElement('1', ScalarType::integer()), new StructureElement('2', ScalarType::string()))
            ),
            new StructureEntry(
                'name',
                ['1' => 1, '2' => 2, '3' => 3],
                new StructureType(new StructureElement('1', ScalarType::integer()), new StructureElement('2', ScalarType::string()), new StructureElement('3', ScalarType::string()))
            ),
        ];
        yield 'equal names and equal simple same array entries' => [
            true,
            new StructureEntry(
                'name',
                ['json', ['foo' => ['bar' => 'baz']]],
                new StructureType(new StructureElement('json', new ListType(ListElement::map(new MapType(MapKey::string(), MapValue::string())))))
            ),
            new StructureEntry(
                'name',
                ['json', ['foo' => ['bar' => 'baz']]],
                new StructureType(new StructureElement('json', new ListType(ListElement::map(new MapType(MapKey::string(), MapValue::string())))))
            ),
        ];
        yield 'equal names and equal simple same collection entries' => [
            true,
            new StructureEntry(
                'name',
                ['json', ['1' => 1, '2' => 2, '3' => 3]],
                new StructureType(new StructureElement('json', new ListType(ListElement::map(new MapType(MapKey::string(), MapValue::string())))))
            ),
            new StructureEntry(
                'name',
                ['json', ['1' => 1, '2' => 2, '3' => 3]],
                new StructureType(new StructureElement('json', new ListType(ListElement::map(new MapType(MapKey::string(), MapValue::string())))))
            ),
        ];
        yield 'equal names and equal simple different collection entries' => [
            false,
            new StructureEntry(
                'name',
                ['json', ['5' => 5, '2' => 2, '1' => 1]],
                new StructureType(new StructureElement('json', new ListType(ListElement::map(new MapType(MapKey::string(), MapValue::string())))))
            ),
            new StructureEntry(
                'name',
                ['json', ['1' => 1, '2' => 2, '3' => 3]],
                new StructureType(new StructureElement('json', new ListType(ListElement::map(new MapType(MapKey::string(), MapValue::string())))))
            ),
        ];
    }

    public function test_definition() : void
    {
        $entry = Entry::structure(
            'items',
            Entry::integer('id', 1),
            Entry::string('name', 'one'),
            Entry::structure('address', Entry::string('street', 'foo'), Entry::string('city', 'bar'))
        );

        $this->assertEquals(
            Definition::structure(
                'items',
                [
                    'id' => Definition::integer('id', false),
                    'name' => Definition::string('name', false),
                    'address' => [
                        'street' => Definition::string('street', false),
                        'city' => Definition::string('city', false),
                    ],
                ],
                false
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
                    new StructureType(new StructureElement('id', ScalarType::integer()), new StructureElement('name', ScalarType::string()))
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
            new StructureType(new StructureElement('id', ScalarType::integer()))
        );

        $this->assertEquals(
            $entry,
            $entry->map(fn (array $entries) => $entries)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new StructureEntry(
            '',
            ['id' => 1, 'name' => 'one'],
            new StructureType(new StructureElement('id', ScalarType::integer()), new StructureElement('name', ScalarType::string()))
        );
    }

    public function test_renames_entry() : void
    {
        $entry = new StructureEntry(
            'entry-name',
            ['id' => 1234],
            new StructureType(new StructureElement('id', ScalarType::integer()))
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
            new StructureType(new StructureElement('id', ScalarType::integer()), new StructureElement('name', ScalarType::string()))
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
            ['json', ['5' => 5, '2' => 2, '3' => 3]],
            new StructureType(new StructureElement('json', new ListType(ListElement::map(new MapType(MapKey::string(), MapValue::integer())))))
        );

        $serialized = \serialize($string);
        /** @var StructureEntry $unserialized */
        $unserialized = \unserialize($serialized);

        $this->assertTrue($string->isEqual($unserialized));
    }
}
