<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\object_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_map;
use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use PHPUnit\Framework\TestCase;

final class RowTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal simple same integer entries' => [
            true,
            new Row(new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3))),
            new Row(new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3))),
        ];
        yield 'same integer entries with different number of entries' => [
            false,
            new Row(new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3))),
            new Row(new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2))),
        ];
        yield 'simple same integer entries with different number of entries reversed' => [
            false,
            new Row(new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2))),
            new Row(new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3))),
        ];
        yield 'simple same array entries' => [
            true,
            new Row(new Entries(new ArrayEntry('json', ['foo' => ['bar' => 'baz']]))),
            new Row(new Entries(new ArrayEntry('json', ['foo' => ['bar' => 'baz']]))),
        ];
        yield 'simple same collection entries' => [
            true,
            new Row(
                new Entries(
                    new StructureEntry(
                        'json',
                        ['json' => [1, 2, 3]],
                        new StructureType(new StructureElement('json', new ListType(ListElement::integer())))
                    )
                )
            ),
            new Row(
                new Entries(
                    new StructureEntry(
                        'json',
                        ['json' => [1, 2, 3]],
                        new StructureType(new StructureElement('json', new ListType(ListElement::integer())))
                    )
                )
            ),
        ];
        yield 'simple different collection entries' => [
            false,
            new Row(
                new Entries(
                    new StructureEntry(
                        'json',
                        ['json' => ['5', '2', '1']],
                        new StructureType(new StructureElement('json', new ListType(ListElement::string())))
                    )
                )
            ),
            new Row(
                new Entries(
                    new StructureEntry(
                        'json',
                        ['json' => ['1', '2', '3']],
                        new StructureType(new StructureElement('json', new ListType(ListElement::string())))
                    )
                )
            ),
        ];
    }

    public function test_getting_schema_from_row() : void
    {
        $row = Row::create(
            int_entry('id', \random_int(100, 100000)),
            float_entry('price', \random_int(100, 100000) / 100),
            bool_entry('deleted', false),
            datetime_entry('created-at', new \DateTimeImmutable('now')),
            null_entry('phase'),
            array_entry(
                'array',
                [
                    ['id' => 1, 'status' => 'NEW'],
                    ['id' => 2, 'status' => 'PENDING'],
                ]
            ),
            struct_entry(
                'items',
                ['item-id' => 1, 'name' => 'one'],
                struct_type(
                    struct_element('item-id', type_int()),
                    struct_element('name', type_string())
                )
            ),
            list_entry('list', [1, 2, 3], type_list(type_int())),
            map_entry(
                'statuses',
                ['NEW', 'PENDING'],
                type_map(type_int(), type_string())
            ),
            object_entry('object', new \ArrayIterator([1, 2, 3]))
        );

        $this->assertEquals(
            new Row\Schema(
                Row\Schema\Definition::integer('id'),
                Row\Schema\Definition::float('price'),
                Row\Schema\Definition::boolean('deleted'),
                Row\Schema\Definition::dateTime('created-at'),
                Row\Schema\Definition::null('phase'),
                Row\Schema\Definition::array('array'),
                Row\Schema\Definition::structure(
                    'items',
                    new StructureType(
                        new StructureElement('item-id', type_int()),
                        new StructureElement('name', type_string())
                    )
                ),
                Row\Schema\Definition::map(
                    'statuses',
                    new MapType(MapKey::integer(), MapValue::string())
                ),
                Row\Schema\Definition::list('list', new ListType(ListElement::integer())),
                Row\Schema\Definition::object('object', type_object(\ArrayIterator::class)),
            ),
            $row->schema()
        );
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, Row $row, Row $nextRow) : void
    {
        $this->assertSame($equals, $row->isEqual($nextRow));
    }

    public function test_keep() : void
    {
        $row = new Row(new Entries(
            int_entry('id', 1),
            str_entry('name', 'test'),
            bool_entry('active', true)
        ));

        $this->assertEquals(
            new Row(new Entries(
                int_entry('id', 1),
                bool_entry('active', true)
            )),
            $row->keep('id', 'active')
        );
    }

    public function test_keep_non_existing_entry() : void
    {
        $this->expectExceptionMessage('Entry "something" does not exist.');

        $row = new Row(new Entries(
            int_entry('id', 1),
            str_entry('name', 'test'),
            bool_entry('active', true)
        ));

        $this->assertEquals(
            new Row(new Entries()),
            $row->keep('something')
        );
    }

    public function test_remove() : void
    {
        $row = new Row(new Entries(
            int_entry('id', 1),
            str_entry('name', 'test'),
            bool_entry('active', true)
        ));

        $this->assertEquals(
            new Row(new Entries(
                int_entry('id', 1),
                str_entry('name', 'test')
            )),
            $row->remove('active')
        );
    }

    public function test_remove_non_existing_entry() : void
    {
        $row = new Row(new Entries(
            int_entry('id', 1),
            str_entry('name', 'test'),
            bool_entry('active', true)
        ));

        $this->assertEquals(
            new Row(new Entries(
                int_entry('id', 1),
                str_entry('name', 'test'),
                bool_entry('active', true)
            )),
            $row->remove('something')
        );
    }

    public function test_renames_entry() : void
    {
        $row = Row::create(
            new StringEntry('name', 'just a string'),
            new BooleanEntry('active', true)
        );
        $newRow = $row->rename('name', 'new-name');

        $this->assertEquals(
            Row::create(
                new BooleanEntry('active', true),
                new StringEntry('new-name', 'just a string')
            ),
            $newRow
        );
    }

    public function test_transforms_row_to_array() : void
    {
        $row = Row::create(
            new IntegerEntry('id', 1234),
            new BooleanEntry('deleted', false),
            new DateTimeEntry('created-at', $createdAt = new \DateTimeImmutable('2020-07-13 15:00')),
            new NullEntry('phase'),
            new StructureEntry(
                'items',
                ['item-id' => 1, 'name' => 'one'],
                new StructureType(new StructureElement('id', type_int()), new StructureElement('name', type_string()))
            ),
            new Row\Entry\MapEntry(
                'statuses',
                ['NEW', 'PENDING'],
                new MapType(MapKey::integer(), MapValue::string())
            )
        );

        $this->assertEquals(
            [
                'id' => 1234,
                'deleted' => false,
                'created-at' => $createdAt,
                'phase' => null,
                'items' => [
                    'item-id' => 1,
                    'name' => 'one',
                ],
                'statuses' => ['NEW', 'PENDING'],
            ],
            $row->toArray(),
        );
    }
}
