<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\{array_entry, array_to_rows, bool_entry, bool_schema, datetime_entry, int_entry, int_schema, list_entry, partition, partitions, ref, row, rows, rows_partitioned, str_entry, str_schema, type_int, type_list, type_string};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\Row\Comparator\{NativeComparator, WeakObjectComparator};
use Flow\ETL\Row\Entry\{BooleanEntry, DateTimeEntry, ObjectEntry, StringEntry};
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\{Comparator, Schema};
use Flow\ETL\{Row, Rows};
use PHPUnit\Framework\TestCase;

final class RowsTest extends TestCase
{
    public static function rows_diff_left_provider() : \Generator
    {
        yield 'one entry identical row' => [
            $expected = rows(),
            $left = rows(row(int_entry('number', 1))),
            $right = rows(row(int_entry('number', 1))),
        ];

        yield 'one entry right different - missing entry' => [
            $expected = rows(row(int_entry('number', 1))),
            $left = rows(row(int_entry('number', 1))),
            $right = rows(),
        ];

        yield 'one entry left different - missing entry' => [
            $expected = rows(),
            $left = rows(),
            $right = rows(row(int_entry('number', 1))),
        ];

        yield 'one entry right different - different entry' => [
            $expected = rows(row(int_entry('number', 1))),
            $left = rows(row(int_entry('number', 1))),
            $right = rows(row(int_entry('number', 2))),
        ];

        yield 'one entry left different - different entry' => [
            $expected = rows(row(int_entry('number', 2))),
            $left = rows(row(int_entry('number', 2))),
            $right = rows(row(int_entry('number', 1))),
        ];
    }

    public static function rows_diff_right_provider() : \Generator
    {
        yield 'one entry identical row' => [
            $expected = rows(),
            $left = rows(row(int_entry('number', 1))),
            $right = rows(row(int_entry('number', 1))),
        ];

        yield 'one entry right different - missing entry' => [
            $expected = rows(),
            $left = rows(row(int_entry('number', 1))),
            $right = rows(),
        ];

        yield 'one entry left different - missing entry' => [
            $expected = rows(row(int_entry('number', 1))),
            $left = rows(),
            $right = rows(row(int_entry('number', 1))),
        ];

        yield 'one entry right different - different entry' => [
            $expected = rows(row(int_entry('number', 2))),
            $left = rows(row(int_entry('number', 1))),
            $right = rows(row(int_entry('number', 2))),
        ];

        yield 'one entry left different - different entry' => [
            $expected = rows(row(int_entry('number', 1))),
            $left = rows(row(int_entry('number', 2))),
            $right = rows(row(int_entry('number', 1))),
        ];
    }

    public static function unique_rows_provider() : \Generator
    {
        yield 'simple identical rows' => [
            $expected = rows(row(int_entry('number', 1))),
            $notUnique = rows(
                row(int_entry('number', 1)),
                row(int_entry('number', 1))
            ),
            $comparator = new NativeComparator(),
        ];

        yield 'simple identical rows with objects' => [
            $expected = rows(row(new ObjectEntry('object', new \stdClass()))),
            $notUnique = rows(
                row(new ObjectEntry('object', $object = new \stdClass())),
                row(new ObjectEntry('object', $object = new \stdClass()))
            ),
            $comparator = new WeakObjectComparator(),
        ];
    }

    public function test_adding_multiple_rows() : void
    {
        $one = row(int_entry('number', 1), new StringEntry('name', 'one'));
        $two = row(int_entry('number', 2), new StringEntry('name', 'two'));
        $rows = (rows())->add($one, $two);

        self::assertEquals(rows($one, $two), $rows);
    }

    public function test_array_access_exists() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        self::assertTrue(isset($rows[0]));
        self::assertFalse(isset($rows[3]));
    }

    public function test_array_access_get() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        self::assertSame(1, $rows[0]->valueOf('id'));
        self::assertSame(2, $rows[1]->valueOf('id'));
        self::assertSame(3, $rows[2]->valueOf('id'));
    }

    public function test_array_access_set() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('In order to add new rows use Rows::add(Row $row) : self');
        $rows = rows();
        $rows[0] = row(int_entry('id', 1));
    }

    public function test_array_access_unset() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('In order to remove rows use Rows::remove(int $offset) : self');
        $rows = rows(row(int_entry('id', 1)));
        unset($rows[0]);
    }

    public function test_building_row_from_array_with_schema_and_additional_fields_not_covered_by_schema() : void
    {
        $rows = array_to_rows(
            ['id' => 1234, 'deleted' => false, 'phase' => null],
            schema: new Schema(
                int_schema('id'),
                bool_schema('deleted'),
            )
        );

        self::assertEquals(
            rows(
                row(
                    int_entry('id', 1234),
                    bool_entry('deleted', false),
                ),
            ),
            $rows
        );
    }

    public function test_building_row_from_array_with_schema_but_entries_not_available_in_rows() : void
    {
        $rows = array_to_rows(
            ['id' => 1234, 'deleted' => false],
            schema: new Schema(
                int_schema('id'),
                bool_schema('deleted'),
                str_schema('phase', true),
            )
        );

        self::assertEquals(
            rows(
                row(
                    int_entry('id', 1234),
                    bool_entry('deleted', false),
                    str_entry('phase', null)
                ),
            ),
            $rows
        );
    }

    public function test_building_rows_from_array() : void
    {
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ]
        );

        self::assertEquals(
            rows(
                row(
                    int_entry('id', 1234),
                    bool_entry('deleted', false),
                    str_entry('phase', null),
                ),
                row(
                    int_entry('id', 4321),
                    bool_entry('deleted', true),
                    str_entry('phase', 'launch'),
                )
            ),
            $rows
        );
    }

    public function test_building_rows_from_array_with_schema_and_additional_fields_not_covered_by_schema() : void
    {
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            schema: new Schema(
                int_schema('id'),
                bool_schema('deleted'),
            )
        );

        self::assertEquals(
            rows(
                row(
                    int_entry('id', 1234),
                    bool_entry('deleted', false),
                ),
                row(
                    int_entry('id', 4321),
                    bool_entry('deleted', true),
                )
            ),
            $rows
        );
    }

    public function test_building_rows_from_array_with_schema_but_entries_not_available_in_rows() : void
    {
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false],
                ['id' => 4321, 'deleted' => true],
            ],
            schema: new Schema(
                int_schema('id'),
                bool_schema('deleted'),
                str_schema('phase', true),
            )
        );

        self::assertEquals(
            rows(
                row(
                    int_entry('id', 1234),
                    bool_entry('deleted', false),
                    str_entry('phase', null)
                ),
                row(
                    int_entry('id', 4321),
                    bool_entry('deleted', true),
                    str_entry('phase', null)
                )
            ),
            $rows
        );
    }

    public function test_chunks_with_less() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
            row(int_entry('id', 4)),
            row(int_entry('id', 5)),
            row(int_entry('id', 6)),
            row(int_entry('id', 7)),
        );

        $chunk = \iterator_to_array($rows->chunks(10));

        self::assertCount(1, $chunk);
        self::assertSame([1, 2, 3, 4, 5, 6, 7], $chunk[0]->reduceToArray('id'));
    }

    public function test_chunks_with_more_than_expected_in_chunk_rows() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
            row(int_entry('id', 4)),
            row(int_entry('id', 5)),
            row(int_entry('id', 6)),
            row(int_entry('id', 7)),
            row(int_entry('id', 8)),
            row(int_entry('id', 9)),
            row(int_entry('id', 10)),
        );

        $chunk = \iterator_to_array($rows->chunks(5));

        self::assertCount(2, $chunk);
        self::assertSame([1, 2, 3, 4, 5], $chunk[0]->reduceToArray('id'));
        self::assertSame([6, 7, 8, 9, 10], $chunk[1]->reduceToArray('id'));
    }

    public function test_drop() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->drop(1);

        self::assertCount(2, $rows);
        self::assertSame(2, $rows[0]->valueOf('id'));
        self::assertSame(3, $rows[1]->valueOf('id'));
    }

    public function test_drop_all() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->drop(3);

        self::assertCount(0, $rows);
    }

    public function test_drop_more_than_exists() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->drop(4);

        self::assertCount(0, $rows);
    }

    public function test_drop_right() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->dropRight(1);

        self::assertCount(2, $rows);
        self::assertSame(1, $rows[0]->valueOf('id'));
        self::assertSame(2, $rows[1]->valueOf('id'));
    }

    public function test_drop_right_all() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->dropRight(3);

        self::assertCount(0, $rows);
    }

    public function test_drop_right_more_than_available() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->dropRight(5);

        self::assertCount(0, $rows);
    }

    public function test_drop_right_more_than_exists() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->dropRight(4);

        self::assertCount(0, $rows);
    }

    public function test_empty_rows() : void
    {
        self::assertTrue((rows())->empty());
        self::assertFalse((rows(row(int_entry('id', 1))))->empty());
    }

    public function test_filters_out_rows() : void
    {
        $rows = rows(
            $one = row(int_entry('number', 1), new StringEntry('name', 'one')),
            $two = row(int_entry('number', 2), new StringEntry('name', 'two')),
            $three = row(int_entry('number', 3), new StringEntry('name', 'three')),
            $four = row(int_entry('number', 4), new StringEntry('name', 'four')),
            $five = row(int_entry('number', 5), new StringEntry('name', 'five'))
        );

        $evenRows = fn (Row $row) : bool => $row->get('number')->value() % 2 === 0;
        $oddRows = fn (Row $row) : bool => $row->get('number')->value() % 2 === 1;

        self::assertEquals(rows($two, $four), $rows->filter($evenRows));
        self::assertEquals(rows($one, $three, $five), $rows->filter($oddRows));
    }

    public function test_find() : void
    {
        $rows = rows(
            $one = row(int_entry('number', 1), new StringEntry('name', 'one')),
            $two = row(int_entry('number', 2), new StringEntry('name', 'two')),
            $three = row(int_entry('number', 3), new StringEntry('name', 'one')),
            $four = row(int_entry('number', 4), new StringEntry('name', 'four')),
            $three1 = row(int_entry('number', 3), new StringEntry('name', 'three')),
        );

        self::assertEquals(
            rows(
                $one,
                $three
            ),
            $rows->find(fn (Row $row) : bool => $row->valueOf('name') === 'one')
        );
    }

    public function test_find_on_empty_rows() : void
    {
        self::assertEquals(rows(), (rows())->find(fn (Row $row) => false));
    }

    public function test_find_one() : void
    {
        $rows = rows(
            $one = row(int_entry('number', 1), new StringEntry('name', 'one')),
            $two = row(int_entry('number', 2), new StringEntry('name', 'two')),
            $three = row(int_entry('number', 3), new StringEntry('name', 'three')),
            $four = row(int_entry('number', 4), new StringEntry('name', 'four')),
            $three1 = row(int_entry('number', 3), new StringEntry('name', 'three')),
        );

        self::assertSame($three, $rows->findOne(fn (Row $row) : bool => $row->valueOf('number') === 3));
        self::assertNotSame($three1, $rows->findOne(fn (Row $row) : bool => $row->valueOf('number') === 3));
    }

    public function test_find_one_on_empty_rows() : void
    {
        self::assertNull((rows())->findOne(fn (Row $row) => false));
    }

    public function test_find_without_results() : void
    {
        $rows = rows(
            $one = row(int_entry('number', 1), new StringEntry('name', 'one')),
            $two = row(int_entry('number', 2), new StringEntry('name', 'two')),
            $three = row(int_entry('number', 3), new StringEntry('name', 'three')),
            $four = row(int_entry('number', 4), new StringEntry('name', 'four')),
            $three1 = row(int_entry('number', 3), new StringEntry('name', 'three')),
        );

        self::assertNull($rows->findOne(fn (Row $row) : bool => $row->valueOf('number') === 5));
    }

    public function test_first_on_empty_rows() : void
    {
        $this->expectException(RuntimeException::class);

        (rows())->first();
    }

    public function test_flat_map() : void
    {
        $rows = rows(
            row(
                int_entry('id', 1234),
            ),
            row(
                int_entry('id', 4567),
            )
        );

        $rows = $rows->flatMap(fn (Row $row) : array => [
            $row->add(new StringEntry('name', $row->valueOf('id') . '-name-01')),
            $row->add(new StringEntry('name', $row->valueOf('id') . '-name-02')),
        ]);

        self::assertSame(
            [
                ['id' => 1234, 'name' => '1234-name-01'],
                ['id' => 1234, 'name' => '1234-name-02'],
                ['id' => 4567, 'name' => '4567-name-01'],
                ['id' => 4567, 'name' => '4567-name-02'],
            ],
            $rows->toArray()
        );
    }

    public function test_hash() : void
    {
        $rows = rows(
            row(int_entry('id', 1), bool_entry('bool', false)),
            row(int_entry('id', 2), bool_entry('bool', false)),
            row(int_entry('id', 3), bool_entry('bool', false)),
            row(int_entry('id', 4), bool_entry('bool', false))
        );

        self::assertSame(
            $rows->hash(),
            rows(
                row(bool_entry('bool', false), int_entry('id', 1)),
                row(bool_entry('bool', false), int_entry('id', 2)),
                row(bool_entry('bool', false), int_entry('id', 3)),
                row(bool_entry('bool', false), int_entry('id', 4))
            )->hash()
        );
    }

    public function test_hash_empty_rows() : void
    {
        self::assertSame(
            rows()->hash(),
            rows()->hash(),
        );
    }

    public function test_hash_rows_with_different_columns() : void
    {
        $rows = rows(
            row(int_entry('id', 1), bool_entry('bool', false)),
            row(int_entry('id', 3), bool_entry('bool', false)),
            row(int_entry('id', 2), bool_entry('bool', false)),
            row(int_entry('id', 4), bool_entry('bool', false))
        );

        self::assertNotSame(
            $rows->hash(),
            rows(
                row(bool_entry('bool', false)),
                row(bool_entry('bool', false)),
                row(bool_entry('bool', false)),
                row(bool_entry('bool', false))
            )->hash()
        );
    }

    public function test_hash_rows_with_different_order() : void
    {
        $rows = rows(
            row(int_entry('id', 1), bool_entry('bool', false)),
            row(int_entry('id', 3), bool_entry('bool', false)),
            row(int_entry('id', 2), bool_entry('bool', false)),
            row(int_entry('id', 4), bool_entry('bool', false))
        );

        self::assertNotSame(
            $rows->hash(),
            rows(
                row(bool_entry('bool', false), int_entry('id', 1)),
                row(bool_entry('bool', false), int_entry('id', 2)),
                row(bool_entry('bool', false), int_entry('id', 3)),
                row(bool_entry('bool', false), int_entry('id', 4))
            )->hash()
        );
    }

    public function test_merge_empty_rows_with_partitioned_rows() : void
    {
        $rows1 = rows(row(int_entry('id', 1), str_entry('group', 'a')))->partitionBy(ref('group'))[0];
        $rows2 = rows();

        self::assertEquals(
            partitions(partition('group', 'a')),
            $rows1->merge($rows2)->partitions()
        );
        self::assertCount(1, $rows1->merge($rows2));
    }

    public function test_merge_row_with_another_row_that_has_duplicated_entries() : void
    {
        $this->expectExceptionMessage('Merged entries names must be unique, given: [id] + [id]');
        $this->expectException(InvalidArgumentException::class);

        row(int_entry('id', 1))
            ->merge(row(int_entry('id', 2)), $prefix = '');
    }

    public function test_merge_rows_from_different_partition() : void
    {
        $rows1 = rows(row(int_entry('id', 1), str_entry('group', 'a')))->partitionBy(ref('group'))[0];
        $rows2 = rows(row(int_entry('id', 2), str_entry('group', 'b')))->partitionBy(ref('group'))[0];

        self::assertEquals(
            partitions(),
            $rows1->merge($rows2)->partitions()
        );
        self::assertCount(2, $rows1->merge($rows2));
    }

    public function test_merge_rows_from_same_partition() : void
    {
        $rows1 = rows(row(int_entry('id', 1), str_entry('group', 'a')))->partitionBy(ref('group'))[0];
        $rows2 = rows(row(int_entry('id', 2), str_entry('group', 'a')))->partitionBy(ref('group'))[0];

        self::assertEquals(
            partitions(partition('group', 'a')),
            $rows1->merge($rows2)->partitions()
        );
        self::assertCount(2, $rows1->merge($rows2));
    }

    public function test_merge_rows_from_same_partitions() : void
    {
        $rows1 = rows(row(int_entry('id', 1), str_entry('group', 'a'), str_entry('sub_group', '1')))
            ->partitionBy(ref('group'), ref('sub_group'))[0];

        $rows2 = rows(row(int_entry('id', 2), str_entry('group', 'a'), str_entry('sub_group', '1')))
            ->partitionBy(ref('sub_group'), ref('group'))[0];

        self::assertEquals(
            partitions(partition('group', 'a'), partition('sub_group', '1')),
            $rows1->merge($rows2)->partitions()
        );
        self::assertCount(2, $rows1->merge($rows2));
    }

    public function test_merges_collection_together() : void
    {
        $rowsOne = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
        );
        $rowsTwo = rows(
            row(int_entry('id', 3)),
            row(int_entry('id', 4)),
            row(int_entry('id', 5))
        );

        $rowsThree = rows(
            row(int_entry('id', 6)),
            row(int_entry('id', 7)),
        );

        $merged = $rowsOne->merge($rowsTwo)->merge($rowsThree);

        self::assertEquals(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3)),
                row(int_entry('id', 4)),
                row(int_entry('id', 5)),
                row(int_entry('id', 6)),
                row(int_entry('id', 7))
            ),
            $merged
        );
    }

    public function test_offset_exists_with_non_int_offset() : void
    {
        $this->expectException(InvalidArgumentException::class);

        (rows())->offsetExists('a');
    }

    public function test_offset_get_on_empty_rows() : void
    {
        $this->expectException(InvalidArgumentException::class);

        (rows())[5];
    }

    public function test_partition_rows_by_multiple_duplicated_entries() : void
    {
        self::assertEquals(
            [
                rows_partitioned(
                    [
                        row(int_entry('num', 1), str_entry('cat', 'a')),
                        row(int_entry('num', 1), str_entry('cat', 'a')),
                    ],
                    [
                        partition('num', '1'),
                        partition('cat', 'a'),
                    ]
                ),
                rows_partitioned(
                    [row(int_entry('num', 1), str_entry('cat', 'b'))],
                    [
                        partition('num', '1'),
                        partition('cat', 'b'),
                    ]
                ),
                rows_partitioned(
                    [row(int_entry('num', 3), str_entry('cat', 'a'))],
                    [
                        partition('num', '3'),
                        partition('cat', 'a'),
                    ]
                ),
                rows_partitioned(
                    [row(int_entry('num', 2), str_entry('cat', 'b'))],
                    [
                        partition('num', '2'),
                        partition('cat', 'b'),
                    ]
                ),
            ],
            (rows(
                row(int_entry('num', 1), str_entry('cat', 'a')),
                row(int_entry('num', 3), str_entry('cat', 'a')),
                row(int_entry('num', 1), str_entry('cat', 'b')),
                row(int_entry('num', 2), str_entry('cat', 'b')),
                row(int_entry('num', 1), str_entry('cat', 'a')),
            ))->partitionBy('num', 'num', 'cat')
        );
    }

    public function test_partition_rows_by_multiple_entries() : void
    {
        self::assertEquals(
            [
                rows_partitioned(
                    [
                        row(int_entry('num', 1), str_entry('cat', 'a')),
                        row(int_entry('num', 1), str_entry('cat', 'a')),
                    ],
                    [
                        partition('num', '1'),
                        partition('cat', 'a'),
                    ]
                ),
                rows_partitioned(
                    [row(int_entry('num', 1), str_entry('cat', 'b'))],
                    [
                        partition('num', '1'),
                        partition('cat', 'b'),
                    ]
                ),
                rows_partitioned(
                    [row(int_entry('num', 3), str_entry('cat', 'a'))],
                    [
                        partition('num', '3'),
                        partition('cat', 'a'),
                    ]
                ),
                rows_partitioned(
                    [row(int_entry('num', 2), str_entry('cat', 'b'))],
                    [
                        partition('num', '2'),
                        partition('cat', 'b'),
                    ]
                ),
            ],
            (rows(
                row(int_entry('num', 1), str_entry('cat', 'a')),
                row(int_entry('num', 3), str_entry('cat', 'a')),
                row(int_entry('num', 1), str_entry('cat', 'b')),
                row(int_entry('num', 2), str_entry('cat', 'b')),
                row(int_entry('num', 1), str_entry('cat', 'a')),
            ))->partitionBy('num', 'cat')
        );
    }

    public function test_partition_rows_by_non_existing_entry() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "test" does not exist');

        (rows(
            row(int_entry('number', 1)),
            row(int_entry('number', 1)),
            row(int_entry('number', 3)),
            row(int_entry('number', 2)),
            row(int_entry('number', 4)),
        ))->partitionBy('test');
    }

    public function test_partition_rows_by_single_entry() : void
    {
        self::assertEquals(
            [
                rows_partitioned(
                    [row(int_entry('number', 1)), row(int_entry('number', 1))],
                    [partition('number', '1')]
                ),
                rows_partitioned([row(int_entry('number', 3))], [partition('number', '3')]),
                rows_partitioned([row(int_entry('number', 2))], [partition('number', '2')]),
                rows_partitioned([row(int_entry('number', 4))], [partition('number', '4')]),
            ],
            (rows(
                row(int_entry('number', 1)),
                row(int_entry('number', 1)),
                row(int_entry('number', 3)),
                row(int_entry('number', 2)),
                row(int_entry('number', 4)),
            ))->partitionBy('number')
        );
    }

    public function test_partition_rows_date_entry() : void
    {
        self::assertEquals(
            [
                rows_partitioned(
                    [row(datetime_entry('date', '2023-01-01 00:00:00 UTC'))],
                    partitions(
                        partition('date', '2023-01-01')
                    )
                ),
                rows_partitioned(
                    [
                        row(datetime_entry('date', '2023-01-02 00:00:00 UTC')),
                        row(datetime_entry('date', '2023-01-02 00:00:00 UTC')),
                    ],
                    partitions(
                        partition('date', '2023-01-02')
                    )
                ),
            ],
            rows(
                row(datetime_entry('date', '2023-01-01 00:00:00 UTC')),
                row(datetime_entry('date', '2023-01-02 00:00:00 UTC')),
                row(datetime_entry('date', '2023-01-02 00:00:00 UTC'))
            )->partitionBy(ref('date'))
        );
    }

    public function test_partitions() : void
    {
        $rows = (rows(
            row(int_entry('number', 1), str_entry('group', 'a')),
            row(int_entry('number', 2), str_entry('group', 'a')),
            row(int_entry('number', 3), str_entry('group', 'a')),
            row(int_entry('number', 4), str_entry('group', 'a')),
        ))->partitionBy('group');

        self::assertEquals(
            partitions(partition('group', 'a')),
            $rows[0]->partitions()
        );
    }

    public function test_remove() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->remove(1);

        self::assertCount(2, $rows);
        self::assertSame(1, $rows[0]->valueOf('id'));
        self::assertSame(3, $rows[1]->valueOf('id'));
    }

    public function test_remove_on_empty_rows() : void
    {
        $this->expectException(InvalidArgumentException::class);

        (rows())->remove(1);
    }

    public function test_returns_first_row() : void
    {
        $rows = rows(
            $first = row(int_entry('number', 3), new StringEntry('name', 'three')),
            row(int_entry('number', 1), new StringEntry('name', 'one')),
            row(int_entry('number', 2), new StringEntry('name', 'two')),
        );

        self::assertEquals($first, $rows->first());
    }

    public function test_reverse() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->reverse();

        self::assertCount(3, $rows);
        self::assertSame(3, $rows[0]->valueOf('id'));
        self::assertSame(2, $rows[1]->valueOf('id'));
        self::assertSame(1, $rows[2]->valueOf('id'));
    }

    /**
     * @dataProvider rows_diff_left_provider
     */
    public function test_rows_diff_left(Rows $expected, Rows $left, Rows $right) : void
    {
        self::assertEquals($expected->toArray(), $left->diffLeft($right)->toArray());
    }

    /**
     * @dataProvider rows_diff_right_provider
     */
    public function test_rows_diff_right(Rows $expected, Rows $left, Rows $right) : void
    {
        self::assertEquals($expected->toArray(), $left->diffRight($right)->toArray());
    }

    public function test_rows_schema() : void
    {
        $rows = rows(
            row(int_entry('id', 1), str_entry('name', 'foo')),
            row(int_entry('id', 1), str_entry('name', null), list_entry('list', [1, 2], type_list(type_int()))),
            row(int_entry('id', 1), str_entry('name', 'bar'), array_entry('tags', ['a', 'b'])),
            row(int_entry('id', 1), int_entry('name', 25)),
        );

        self::assertEquals(
            new Schema(
                Definition::integer('id'),
                Definition::string('name', true),
                Definition::array('tags', false, true),
                Definition::list('list', new ListType(ListElement::integer(), true))
            ),
            $rows->schema()
        );
    }

    public function test_rows_schema_when_rows_have_different_list_types() : void
    {
        $rows = rows(
            row(list_entry('list', ['one', 'two'], type_list(type_string()))),
            row(list_entry('list', [1, 2], type_list(type_int()))),
        );

        self::assertEquals(
            new Schema(Definition::array('list')),
            $rows->schema()
        );
    }

    public function test_rows_serialization() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $serialized = \serialize($rows);

        /** @var Rows $unserialized */
        $unserialized = \unserialize($serialized);

        self::assertTrue($unserialized[0]->isEqual($rows[0]));
        self::assertTrue($unserialized[1]->isEqual($rows[1]));
        self::assertTrue($unserialized[2]->isEqual($rows[2]));
    }

    /**
     * @dataProvider unique_rows_provider
     */
    public function test_rows_unique(Rows $expected, Rows $notUnique, Comparator $comparator = new NativeComparator()) : void
    {
        self::assertEquals($expected, $notUnique->unique($comparator));
    }

    public function test_sort() : void
    {
        $rows = rows(
            $three = row(int_entry('number', 3), new StringEntry('name', 'three')),
            $one = row(int_entry('number', 1), new StringEntry('name', 'one')),
            $five = row(int_entry('number', 5), new StringEntry('name', 'five')),
            $two = row(int_entry('number', 2), new StringEntry('name', 'two')),
            $four = row(int_entry('number', 4), new StringEntry('name', 'four')),
        );

        $sort = $rows->sort(fn (Row $row, Row $nextRow) : int => $row->valueOf('number') <=> $nextRow->valueOf('number'));

        self::assertEquals(rows($one, $two, $three, $four, $five), $sort);
        self::assertNotEquals($sort, $rows);
    }

    public function test_sort_rows_by_not_existing_column() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "c" does not exist');

        $rows = rows(
            row(int_entry('a', 3), int_entry('b', 2)),
            row(int_entry('a', 1), int_entry('b', 5)),
            row(int_entry('a', 1), int_entry('b', 4)),
            row(int_entry('a', 2), int_entry('b', 7)),
            row(int_entry('a', 3), int_entry('b', 10)),
            row(int_entry('a', 2), int_entry('b', 4)),
        );

        $rows->sortBy(ref('c'), ref('b')->desc());
    }

    public function test_sort_rows_by_two_columns() : void
    {
        $rows = rows(
            row(int_entry('a', 3), int_entry('b', 2)),
            row(int_entry('a', 1), int_entry('b', 5)),
            row(int_entry('a', 1), int_entry('b', 4)),
            row(int_entry('a', 2), int_entry('b', 7)),
            row(int_entry('a', 3), int_entry('b', 10)),
            row(int_entry('a', 2), int_entry('b', 4)),
        );

        $ascending = $rows->sortBy(ref('a'), ref('b')->desc());
        $descending = $rows->sortBy(ref('a')->desc(), ref('b'));

        self::assertSame(
            [
                ['a' => 1, 'b' => 5], ['a' => 1, 'b' => 4], ['a' => 2, 'b' => 7], ['a' => 2, 'b' => 4], ['a' => 3, 'b' => 10], ['a' => 3, 'b' => 2],
            ],
            $ascending->toArray()
        );
        self::assertSame(
            [
                ['a' => 3, 'b' => 2], ['a' => 3, 'b' => 10], ['a' => 2, 'b' => 4], ['a' => 2, 'b' => 7], ['a' => 1, 'b' => 4], ['a' => 1, 'b' => 5],
            ],
            $descending->toArray()
        );
    }

    public function test_sort_rows_without_changing_original_collection() : void
    {
        $rows = rows(
            $three = row(int_entry('number', 3), new StringEntry('name', 'three')),
            $one = row(int_entry('number', 1), new StringEntry('name', 'one')),
            $five = row(int_entry('number', 5), new StringEntry('name', 'five')),
            $two = row(int_entry('number', 2), new StringEntry('name', 'two')),
            $four = row(int_entry('number', 4), new StringEntry('name', 'four')),
        );

        $ascending = $rows->sortAscending(ref('number'));
        $descending = $rows->sortDescending(ref('number'));

        self::assertEquals(rows($one, $two, $three, $four, $five), $ascending);
        self::assertEquals(rows($five, $four, $three, $two, $one), $descending);
        self::assertNotEquals($ascending, $rows);
        self::assertNotEquals($descending, $rows);
    }

    public function test_sorts_entries_in_all_rows() : void
    {
        $rows = rows(
            row(
                $rowOneId = int_entry('id', 1),
                $rowOneDeleted = new BooleanEntry('deleted', true),
                $rowOnePhase = new StringEntry('phase', null),
                $rowOneCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
            ),
            row(
                $rowTwoDeleted = new BooleanEntry('deleted', true),
                $rowTwoCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
                $rowTwoId = int_entry('id', 1),
                $rowTwoPhase = new StringEntry('phase', null),
            ),
        );

        $sorted = $rows->sortEntries();

        self::assertEquals(
            rows(
                row(
                    $rowOneCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
                    $rowOneDeleted = new BooleanEntry('deleted', true),
                    $rowOneId = int_entry('id', 1),
                    $rowOnePhase = new StringEntry('phase', null),
                ),
                row(
                    $rowTwoCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
                    $rowTwoDeleted = new BooleanEntry('deleted', true),
                    $rowTwoId = int_entry('id', 1),
                    $rowTwoPhase = new StringEntry('phase', null),
                )
            ),
            $sorted
        );
    }

    public function test_take() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->take(1);

        self::assertCount(1, $rows);
        self::assertSame(1, $rows[0]->valueOf('id'));
    }

    public function test_take_all() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->take(3);

        self::assertCount(3, $rows);
        self::assertSame(1, $rows[0]->valueOf('id'));
        self::assertSame(2, $rows[1]->valueOf('id'));
        self::assertSame(3, $rows[2]->valueOf('id'));
    }

    public function test_take_more_than_exists() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->take(4);

        self::assertCount(3, $rows);
        self::assertSame(1, $rows[0]->valueOf('id'));
        self::assertSame(2, $rows[1]->valueOf('id'));
        self::assertSame(3, $rows[2]->valueOf('id'));
    }

    public function test_take_right() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->takeRight(1);

        self::assertCount(1, $rows);
        self::assertSame(3, $rows[0]->valueOf('id'));
    }

    public function test_take_right_all() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->takeRight(3);

        self::assertCount(3, $rows);
        self::assertSame(3, $rows[0]->valueOf('id'));
        self::assertSame(2, $rows[1]->valueOf('id'));
        self::assertSame(1, $rows[2]->valueOf('id'));
    }

    public function test_take_right_more_than_exists() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->takeRight(4);

        self::assertCount(3, $rows);
        self::assertSame(3, $rows[0]->valueOf('id'));
        self::assertSame(2, $rows[1]->valueOf('id'));
        self::assertSame(1, $rows[2]->valueOf('id'));
    }

    public function test_transforms_rows_to_array() : void
    {
        $rows = rows(
            row(
                int_entry('id', 1234),
                new BooleanEntry('deleted', false),
                new StringEntry('phase', null),
            ),
            row(
                int_entry('id', 4321),
                new BooleanEntry('deleted', true),
                new StringEntry('phase', 'launch'),
            )
        );

        self::assertEquals(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            $rows->toArray()
        );
    }

    public function test_transforms_rows_to_array_without_keys() : void
    {
        $rows = rows(
            row(
                int_entry('id', 1234),
                new BooleanEntry('deleted', false),
                new StringEntry('phase', null),
            ),
            row(
                int_entry('id', 4321),
                new BooleanEntry('deleted', true),
                new StringEntry('phase', 'launch'),
            )
        );

        self::assertEquals(
            [
                [1234, false, null],
                [4321, true, 'launch'],
            ],
            $rows->toArray(withKeys: false)
        );
    }
}
