<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\partition;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\rows_partitioned;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Partitions;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\Row;
use Flow\ETL\Row\Comparator;
use Flow\ETL\Row\Comparator\NativeComparator;
use Flow\ETL\Row\Comparator\WeakObjectComparator;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Rows;
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

        $this->assertEquals(rows($one, $two), $rows);
    }

    public function test_array_access_exists() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $this->assertTrue(isset($rows[0]));
        $this->assertFalse(isset($rows[3]));
    }

    public function test_array_access_get() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $this->assertSame(1, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
        $this->assertSame(3, $rows[2]->valueOf('id'));
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

    public function test_building_rows_from_array() : void
    {
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ]
        );

        $this->assertEquals(
            rows(
                row(
                    int_entry('id', 1234),
                    bool_entry('deleted', false),
                    null_entry('phase'),
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

        $this->assertCount(1, $chunk);
        $this->assertSame([1, 2, 3, 4, 5, 6, 7], $chunk[0]->reduceToArray('id'));
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

        $this->assertCount(2, $chunk);
        $this->assertSame([1, 2, 3, 4, 5], $chunk[0]->reduceToArray('id'));
        $this->assertSame([6, 7, 8, 9, 10], $chunk[1]->reduceToArray('id'));
    }

    public function test_drop() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->drop(1);

        $this->assertCount(2, $rows);
        $this->assertSame(2, $rows[0]->valueOf('id'));
        $this->assertSame(3, $rows[1]->valueOf('id'));
    }

    public function test_drop_all() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->drop(3);

        $this->assertCount(0, $rows);
    }

    public function test_drop_more_than_exists() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->drop(4);

        $this->assertCount(0, $rows);
    }

    public function test_drop_right() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->dropRight(1);

        $this->assertCount(2, $rows);
        $this->assertSame(1, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
    }

    public function test_drop_right_all() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->dropRight(3);

        $this->assertCount(0, $rows);
    }

    public function test_drop_right_more_than_available() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->dropRight(5);

        $this->assertCount(0, $rows);
    }

    public function test_drop_right_more_than_exists() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->dropRight(4);

        $this->assertCount(0, $rows);
    }

    public function test_empty_rows() : void
    {
        $this->assertTrue((rows())->empty());
        $this->assertFalse((rows(row(int_entry('id', 1))))->empty());
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

        $this->assertEquals(rows($two, $four), $rows->filter($evenRows));
        $this->assertEquals(rows($one, $three, $five), $rows->filter($oddRows));
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

        $this->assertEquals(
            rows(
                $one,
                $three
            ),
            $rows->find(fn (Row $row) : bool => $row->valueOf('name') === 'one')
        );
    }

    public function test_find_on_empty_rows() : void
    {
        $this->assertEquals(rows(), (rows())->find(fn (Row $row) => false));
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

        $this->assertSame($three, $rows->findOne(fn (Row $row) : bool => $row->valueOf('number') === 3));
        $this->assertNotSame($three1, $rows->findOne(fn (Row $row) : bool => $row->valueOf('number') === 3));
    }

    public function test_find_one_on_empty_rows() : void
    {
        $this->assertNull((rows())->findOne(fn (Row $row) => false));
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

        $this->assertNull($rows->findOne(fn (Row $row) : bool => $row->valueOf('number') === 5));
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

        $this->assertSame(
            [
                ['id' => 1234, 'name' => '1234-name-01'],
                ['id' => 1234, 'name' => '1234-name-02'],
                ['id' => 4567, 'name' => '4567-name-01'],
                ['id' => 4567, 'name' => '4567-name-02'],
            ],
            $rows->toArray()
        );
    }

    public function test_merge_empty_rows_with_partitioned_rows() : void
    {
        $rows1 = rows(row(int_entry('id', 1), str_entry('group', 'a')))->partitionBy(ref('group'))[0];
        $rows2 = rows();

        $this->assertEquals(
            \Flow\ETL\DSL\partitions(partition('group', 'a')),
            $rows1->merge($rows2)->partitions()
        );
        $this->assertCount(1, $rows1->merge($rows2));
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

        $this->assertEquals(
            \Flow\ETL\DSL\partitions(),
            $rows1->merge($rows2)->partitions()
        );
        $this->assertCount(2, $rows1->merge($rows2));
    }

    public function test_merge_rows_from_same_partition() : void
    {
        $rows1 = rows(row(int_entry('id', 1), str_entry('group', 'a')))->partitionBy(ref('group'))[0];
        $rows2 = rows(row(int_entry('id', 2), str_entry('group', 'a')))->partitionBy(ref('group'))[0];

        $this->assertEquals(
            \Flow\ETL\DSL\partitions(partition('group', 'a')),
            $rows1->merge($rows2)->partitions()
        );
        $this->assertCount(2, $rows1->merge($rows2));
    }

    public function test_merge_rows_from_same_partitions() : void
    {
        $rows1 = rows(row(int_entry('id', 1), str_entry('group', 'a'), str_entry('sub_group', '1')))
            ->partitionBy(ref('group'), ref('sub_group'))[0];

        $rows2 = rows(row(int_entry('id', 2), str_entry('group', 'a'), str_entry('sub_group', '1')))
            ->partitionBy(ref('sub_group'), ref('group'))[0];

        $this->assertEquals(
            \Flow\ETL\DSL\partitions(partition('group', 'a'), partition('sub_group', '1')),
            $rows1->merge($rows2)->partitions()
        );
        $this->assertCount(2, $rows1->merge($rows2));
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

        $this->assertEquals(
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
        $this->assertEquals(
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
        $this->assertEquals(
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
        $this->assertEquals(
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

    public function test_partitions() : void
    {
        $rows = (rows(
            row(int_entry('number', 1), str_entry('group', 'a')),
            row(int_entry('number', 2), str_entry('group', 'a')),
            row(int_entry('number', 3), str_entry('group', 'a')),
            row(int_entry('number', 4), str_entry('group', 'a')),
        ))->partitionBy('group');

        $this->assertEquals(
            new Partitions(partition('group', 'a')),
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

        $this->assertCount(2, $rows);
        $this->assertSame(1, $rows[0]->valueOf('id'));
        $this->assertSame(3, $rows[1]->valueOf('id'));
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

        $this->assertEquals($first, $rows->first());
    }

    public function test_reverse() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->reverse();

        $this->assertCount(3, $rows);
        $this->assertSame(3, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
        $this->assertSame(1, $rows[2]->valueOf('id'));
    }

    /**
     * @dataProvider rows_diff_left_provider
     */
    public function test_rows_diff_left(Rows $expected, Rows $left, Rows $right) : void
    {
        $this->assertEquals($expected->toArray(), $left->diffLeft($right)->toArray());
    }

    /**
     * @dataProvider rows_diff_right_provider
     */
    public function test_rows_diff_right(Rows $expected, Rows $left, Rows $right) : void
    {
        $this->assertEquals($expected->toArray(), $left->diffRight($right)->toArray());
    }

    public function test_rows_schema() : void
    {
        $rows = rows(
            row(int_entry('id', 1), str_entry('name', 'foo')),
            row(int_entry('id', 1), null_entry('name'), list_entry('list', [1, 2], type_list(type_int()))),
            row(int_entry('id', 1), str_entry('name', 'bar'), array_entry('tags', ['a', 'b'])),
            row(int_entry('id', 1), int_entry('name', 25)),
        );

        $this->assertEquals(
            new Schema(
                Definition::integer('id'),
                Definition::union('name', [StringEntry::class, NullEntry::class, IntegerEntry::class]),
                Definition::array('tags', true),
                Definition::list('list', new ListType(ListElement::integer()), true)
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

        $this->assertEquals(
            new Schema(Definition::list('list', new ListType(ListElement::integer()))),
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

        $this->assertTrue($unserialized[0]->isEqual($rows[0]));
        $this->assertTrue($unserialized[1]->isEqual($rows[1]));
        $this->assertTrue($unserialized[2]->isEqual($rows[2]));
    }

    /**
     * @dataProvider unique_rows_provider
     */
    public function test_rows_unique(Rows $expected, Rows $notUnique, Comparator $comparator = new NativeComparator()) : void
    {
        $this->assertEquals($expected, $notUnique->unique($comparator));
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

        $this->assertEquals(rows($one, $two, $three, $four, $five), $sort);
        $this->assertNotEquals($sort, $rows);
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

        $this->assertSame(
            [
                ['a' => 1, 'b' => 5], ['a' => 1, 'b' => 4], ['a' => 2, 'b' => 7], ['a' => 2, 'b' => 4], ['a' => 3, 'b' => 10], ['a' => 3, 'b' => 2],
            ],
            $ascending->toArray()
        );
        $this->assertSame(
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

        $this->assertEquals(rows($one, $two, $three, $four, $five), $ascending);
        $this->assertEquals(rows($five, $four, $three, $two, $one), $descending);
        $this->assertNotEquals($ascending, $rows);
        $this->assertNotEquals($descending, $rows);
    }

    public function test_sorts_entries_in_all_rows() : void
    {
        $rows = rows(
            row(
                $rowOneId = int_entry('id', 1),
                $rowOneDeleted = new BooleanEntry('deleted', true),
                $rowOnePhase = new NullEntry('phase'),
                $rowOneCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
            ),
            row(
                $rowTwoDeleted = new BooleanEntry('deleted', true),
                $rowTwoCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
                $rowTwoId = int_entry('id', 1),
                $rowTwoPhase = new NullEntry('phase'),
            ),
        );

        $sorted = $rows->sortEntries();

        $this->assertEquals(
            rows(
                row(
                    $rowOneCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
                    $rowOneDeleted = new BooleanEntry('deleted', true),
                    $rowOneId = int_entry('id', 1),
                    $rowOnePhase = new NullEntry('phase'),
                ),
                row(
                    $rowTwoCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
                    $rowTwoDeleted = new BooleanEntry('deleted', true),
                    $rowTwoId = int_entry('id', 1),
                    $rowTwoPhase = new NullEntry('phase'),
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

        $this->assertCount(1, $rows);
        $this->assertSame(1, $rows[0]->valueOf('id'));
    }

    public function test_take_all() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->take(3);

        $this->assertCount(3, $rows);
        $this->assertSame(1, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
        $this->assertSame(3, $rows[2]->valueOf('id'));
    }

    public function test_take_more_than_exists() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->take(4);

        $this->assertCount(3, $rows);
        $this->assertSame(1, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
        $this->assertSame(3, $rows[2]->valueOf('id'));
    }

    public function test_take_right() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->takeRight(1);

        $this->assertCount(1, $rows);
        $this->assertSame(3, $rows[0]->valueOf('id'));
    }

    public function test_take_right_all() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->takeRight(3);

        $this->assertCount(3, $rows);
        $this->assertSame(3, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
        $this->assertSame(1, $rows[2]->valueOf('id'));
    }

    public function test_take_right_more_than_exists() : void
    {
        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $rows = $rows->takeRight(4);

        $this->assertCount(3, $rows);
        $this->assertSame(3, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
        $this->assertSame(1, $rows[2]->valueOf('id'));
    }

    public function test_transforms_rows_to_array() : void
    {
        $rows = rows(
            row(
                int_entry('id', 1234),
                new BooleanEntry('deleted', false),
                new NullEntry('phase'),
            ),
            row(
                int_entry('id', 4321),
                new BooleanEntry('deleted', true),
                new StringEntry('phase', 'launch'),
            )
        );

        $this->assertEquals(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            $rows->toArray()
        );
    }
}
