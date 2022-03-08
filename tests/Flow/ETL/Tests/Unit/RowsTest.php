<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class RowsTest extends TestCase
{
    public function rows_diff_left_provider() : \Generator
    {
        yield 'one entry identical row' => [
            $expected = new Rows(),
            $left = new Rows(Row::create(new IntegerEntry('number', 1))),
            $right = new Rows(Row::create(new IntegerEntry('number', 1))),
        ];

        yield 'one entry right different - missing entry' => [
            $expected = new Rows(Row::create(new IntegerEntry('number', 1))),
            $left = new Rows(Row::create(new IntegerEntry('number', 1))),
            $right = new Rows(),
        ];

        yield 'one entry left different - missing entry' => [
            $expected = new Rows(),
            $left = new Rows(),
            $right = new Rows(Row::create(new IntegerEntry('number', 1))),
        ];

        yield 'one entry right different - different entry' => [
            $expected = new Rows(Row::create(new IntegerEntry('number', 1))),
            $left = new Rows(Row::create(new IntegerEntry('number', 1))),
            $right = new Rows(Row::create(new IntegerEntry('number', 2))),
        ];

        yield 'one entry left different - different entry' => [
            $expected = new Rows(Row::create(new IntegerEntry('number', 2))),
            $left = new Rows(Row::create(new IntegerEntry('number', 2))),
            $right = new Rows(Row::create(new IntegerEntry('number', 1))),
        ];
    }

    public function rows_diff_right_provider() : \Generator
    {
        yield 'one entry identical row' => [
            $expected = new Rows(),
            $left = new Rows(Row::create(new IntegerEntry('number', 1))),
            $right = new Rows(Row::create(new IntegerEntry('number', 1))),
        ];

        yield 'one entry right different - missing entry' => [
            $expected = new Rows(),
            $left = new Rows(Row::create(new IntegerEntry('number', 1))),
            $right = new Rows(),
        ];

        yield 'one entry left different - missing entry' => [
            $expected = new Rows(Row::create(new IntegerEntry('number', 1))),
            $left = new Rows(),
            $right = new Rows(Row::create(new IntegerEntry('number', 1))),
        ];

        yield 'one entry right different - different entry' => [
            $expected = new Rows(Row::create(new IntegerEntry('number', 2))),
            $left = new Rows(Row::create(new IntegerEntry('number', 1))),
            $right = new Rows(Row::create(new IntegerEntry('number', 2))),
        ];

        yield 'one entry left different - different entry' => [
            $expected = new Rows(Row::create(new IntegerEntry('number', 1))),
            $left = new Rows(Row::create(new IntegerEntry('number', 2))),
            $right = new Rows(Row::create(new IntegerEntry('number', 1))),
        ];
    }

    public function test_adding_multiple_rows() : void
    {
        $one = Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one'));
        $two = Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two'));
        $rows = (new Rows())->add($one, $two);

        $this->assertEquals(new Rows($one, $two), $rows);
    }

    public function test_array_access_exists() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $this->assertTrue(isset($rows[0]));
        $this->assertFalse(isset($rows[3]));
    }

    public function test_array_access_get() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $this->assertSame(1, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
        $this->assertSame(3, $rows[2]->valueOf('id'));
    }

    public function test_array_access_set() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('In order to add new rows use Rows::add(Row $row) : self');
        $rows = new Rows();
        $rows[0] = Row::create(new IntegerEntry('id', 1));
    }

    public function test_array_access_unset() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('In order to add new rows use Rows::remove(int $offset) : self');
        $rows = new Rows(Row::create(new IntegerEntry('id', 1)));
        unset($rows[0]);
    }

    public function test_chunks_smaller_than_1() : void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Rows())->chunks(-1);
    }

    public function test_chunks_with_less() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
            Row::create(new IntegerEntry('id', 4)),
            Row::create(new IntegerEntry('id', 5)),
            Row::create(new IntegerEntry('id', 6)),
            Row::create(new IntegerEntry('id', 7)),
        );

        $this->assertCount(1, $rows->chunks(10));
        $this->assertSame([1, 2, 3, 4, 5, 6, 7], $rows->chunks(10)[0]->reduceToArray('id'));
    }

    public function test_chunks_with_more_than_expected_in_chunk_rows() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
            Row::create(new IntegerEntry('id', 4)),
            Row::create(new IntegerEntry('id', 5)),
            Row::create(new IntegerEntry('id', 6)),
            Row::create(new IntegerEntry('id', 7)),
            Row::create(new IntegerEntry('id', 8)),
            Row::create(new IntegerEntry('id', 9)),
            Row::create(new IntegerEntry('id', 10)),
        );

        $this->assertCount(2, $rows->chunks(5));
        $this->assertSame([1, 2, 3, 4, 5], $rows->chunks(5)[0]->reduceToArray('id'));
        $this->assertSame([6, 7, 8, 9, 10], $rows->chunks(5)[1]->reduceToArray('id'));
    }

    public function test_drop() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->drop(1);

        $this->assertCount(2, $rows);
        $this->assertSame(2, $rows[0]->valueOf('id'));
        $this->assertSame(3, $rows[1]->valueOf('id'));
    }

    public function test_drop_all() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->drop(3);

        $this->assertCount(0, $rows);
    }

    public function test_drop_more_than_exists() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->drop(4);

        $this->assertCount(0, $rows);
    }

    public function test_drop_right() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->dropRight(1);

        $this->assertCount(2, $rows);
        $this->assertSame(1, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
    }

    public function test_drop_right_all() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->dropRight(3);

        $this->assertCount(0, $rows);
    }

    public function test_drop_right_more_than_exists() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->dropRight(4);

        $this->assertCount(0, $rows);
    }

    public function test_empty_rows() : void
    {
        $this->assertTrue((new Rows())->empty());
        $this->assertFalse((new Rows(Row::create(Entry::integer('id', 1))))->empty());
    }

    public function test_filters_out_rows() : void
    {
        $rows = new Rows(
            $one   = Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
            $two   = Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
            $three = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'three')),
            $four  = Row::create(new IntegerEntry('number', 4), new StringEntry('name', 'four')),
            $five   = Row::create(new IntegerEntry('number', 5), new StringEntry('name', 'five'))
        );

        $evenRows = fn (Row $row) : bool => $row->get('number')->value() % 2 === 0;
        $oddRows = fn (Row $row) : bool => $row->get('number')->value() % 2 === 1;

        $this->assertEquals(new Rows($two, $four), $rows->filter($evenRows));
        $this->assertEquals(new Rows($one, $three, $five), $rows->filter($oddRows));
    }

    public function test_find() : void
    {
        $rows = new Rows(
            $one   = Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
            $two   = Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
            $three = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'one')),
            $four  = Row::create(new IntegerEntry('number', 4), new StringEntry('name', 'four')),
            $three1 = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'three')),
        );

        $this->assertEquals(
            new Rows(
                $one,
                $three
            ),
            $rows->find(fn (Row $row) : bool => $row->valueOf('name') === 'one')
        );
    }

    public function test_find_on_empty_rows() : void
    {
        $this->assertEquals(new Rows(), (new Rows())->find(fn (Row $row) => false));
    }

    public function test_find_one() : void
    {
        $rows = new Rows(
            $one   = Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
            $two   = Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
            $three = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'three')),
            $four  = Row::create(new IntegerEntry('number', 4), new StringEntry('name', 'four')),
            $three1 = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'three')),
        );

        $this->assertSame($three, $rows->findOne(fn (Row $row) : bool => $row->valueOf('number') === 3));
        $this->assertNotSame($three1, $rows->findOne(fn (Row $row) : bool => $row->valueOf('number') === 3));
    }

    public function test_find_one_on_empty_rows() : void
    {
        $this->assertNull((new Rows())->findOne(fn (Row $row) => false));
    }

    public function test_find_without_results() : void
    {
        $rows = new Rows(
            $one   = Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
            $two   = Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
            $three = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'three')),
            $four  = Row::create(new IntegerEntry('number', 4), new StringEntry('name', 'four')),
            $three1 = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'three')),
        );

        $this->assertNull($rows->findOne(fn (Row $row) : bool => $row->valueOf('number') === 5));
    }

    public function test_first_on_empty_rows() : void
    {
        $this->expectException(RuntimeException::class);

        (new Rows())->first();
    }

    public function test_flat_map() : void
    {
        $rows = new Rows(
            Row::create(
                new IntegerEntry('id', 1234),
            ),
            Row::create(
                new IntegerEntry('id', 4567),
            )
        );

        $rows = $rows->flatMap(function (Row $row) : array {
            return [
                $row->add(new StringEntry('name', $row->valueOf('id') . '-name-01')),
                $row->add(new StringEntry('name', $row->valueOf('id') . '-name-02')),
            ];
        });

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

    public function test_group_by_multiple_entries() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('foo', 'aa'), Entry::string('bar', 'ab'), Entry::integer('value', 1000)),
            Row::create(Entry::integer('id', 2), Entry::string('foo', 'aa'), Entry::string('bar', 'cd'), Entry::integer('value', 1001)),
            Row::create(Entry::integer('id', 3), Entry::string('foo', 'bb'), Entry::string('bar', 'ab'), Entry::integer('value', 1002)),
            Row::create(Entry::integer('id', 4), Entry::string('foo', 'cc'), Entry::string('bar', 'bc'), Entry::integer('value', 1003)),
            Row::create(Entry::integer('id', 5), Entry::string('foo', 'cc'), Entry::string('bar', 'ab'), Entry::integer('value', 1004)),
            Row::create(Entry::integer('id', 6), Entry::string('foo', 'cc'), Entry::string('bar', 'bc'), Entry::integer('value', 1005)),
            Row::create(Entry::integer('id', 7), Entry::string('foo', 'aa'), Entry::string('bar', 'cd'), Entry::integer('value', 1006)),
        );

        $this->assertEquals(
            new Rows(
                Row::create(
                    Entry::array('entries', ['foo', 'bar']),
                    Entry::array('values', ['aa', 'ab']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 1), Entry::string('foo', 'aa'), Entry::string('bar', 'ab'), Entry::integer('value', 1000)),
                    )
                ),
                Row::create(
                    Entry::array('entries', ['foo', 'bar']),
                    Entry::array('values', ['aa', 'cd']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 2), Entry::string('foo', 'aa'), Entry::string('bar', 'cd'), Entry::integer('value', 1001)),
                        Entry::entries(Entry::integer('id', 7), Entry::string('foo', 'aa'), Entry::string('bar', 'cd'), Entry::integer('value', 1006))
                    )
                ),
                Row::create(
                    Entry::array('entries', ['foo', 'bar']),
                    Entry::array('values', ['bb', 'ab']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 3), Entry::string('foo', 'bb'), Entry::string('bar', 'ab'), Entry::integer('value', 1002)),
                    )
                ),
                Row::create(
                    Entry::array('entries', ['foo', 'bar']),
                    Entry::array('values', ['cc', 'bc']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 4), Entry::string('foo', 'cc'), Entry::string('bar', 'bc'), Entry::integer('value', 1003)),
                        Entry::entries(Entry::integer('id', 6), Entry::string('foo', 'cc'), Entry::string('bar', 'bc'), Entry::integer('value', 1005)),
                    )
                ),
                Row::create(
                    Entry::array('entries', ['foo', 'bar']),
                    Entry::array('values', ['cc', 'ab']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 5), Entry::string('foo', 'cc'), Entry::string('bar', 'ab'), Entry::integer('value', 1004)),
                    )
                )
            ),
            $rows->groupBy('foo', 'bar')->toRows()
        );
    }

    public function test_group_by_single_entry() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('foo', 'aa'), Entry::integer('value', 1000)),
            Row::create(Entry::integer('id', 2), Entry::string('foo', 'aa'), Entry::integer('value', 1001)),
            Row::create(Entry::integer('id', 3), Entry::string('foo', 'bb'), Entry::integer('value', 1002)),
            Row::create(Entry::integer('id', 4), Entry::string('foo', 'cc'), Entry::integer('value', 1003)),
            Row::create(Entry::integer('id', 5), Entry::string('foo', 'cc'), Entry::integer('value', 1004)),
            Row::create(Entry::integer('id', 6), Entry::string('foo', 'cc'), Entry::integer('value', 1005)),
            Row::create(Entry::integer('id', 7), Entry::string('foo', 'aa'), Entry::integer('value', 1006)),
        );

        $this->assertEquals(
            new Rows(
                Row::create(
                    Entry::array('entries', ['foo']),
                    Entry::array('values', ['aa']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 1), Entry::string('foo', 'aa'), Entry::integer('value', 1000)),
                        Entry::entries(Entry::integer('id', 2), Entry::string('foo', 'aa'), Entry::integer('value', 1001)),
                        Entry::entries(Entry::integer('id', 7), Entry::string('foo', 'aa'), Entry::integer('value', 1006)),
                    )
                ),
                Row::create(
                    Entry::array('entries', ['foo']),
                    Entry::array('values', ['bb']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 3), Entry::string('foo', 'bb'), Entry::integer('value', 1002)),
                    )
                ),
                Row::create(
                    Entry::array('entries', ['foo']),
                    Entry::array('values', ['cc']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 4), Entry::string('foo', 'cc'), Entry::integer('value', 1003)),
                        Entry::entries(Entry::integer('id', 5), Entry::string('foo', 'cc'), Entry::integer('value', 1004)),
                        Entry::entries(Entry::integer('id', 6), Entry::string('foo', 'cc'), Entry::integer('value', 1005)),
                    )
                )
            ),
            $rows->groupBy('foo')->toRows()
        );
    }

    public function test_group_by_single_entry_with_nulls_and_missing_entry() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('foo', 'aa'), Entry::integer('value', 1000)),
            Row::create(Entry::integer('id', 2), Entry::string('foo', 'aa'), Entry::integer('value', 1001)),
            Row::create(Entry::integer('id', 3), Entry::string('foo', 'bb'), Entry::integer('value', 1002)),
            Row::create(Entry::integer('id', 4), Entry::null('foo'), Entry::integer('value', 1003)),
            Row::create(Entry::integer('id', 5), Entry::string('foo', 'cc'), Entry::integer('value', 1004)),
            Row::create(Entry::integer('id', 6), Entry::string('foo', 'cc'), Entry::integer('value', 1005)),
            Row::create(Entry::integer('id', 7), Entry::null('foo'), Entry::integer('value', 1006)),
            Row::create(Entry::integer('id', 8), Entry::integer('value', 1007)),
        );

        $this->assertEquals(
            new Rows(
                Row::create(
                    Entry::array('entries', ['foo']),
                    Entry::array('values', ['aa']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 1), Entry::string('foo', 'aa'), Entry::integer('value', 1000)),
                        Entry::entries(Entry::integer('id', 2), Entry::string('foo', 'aa'), Entry::integer('value', 1001)),
                    )
                ),
                Row::create(
                    Entry::array('entries', ['foo']),
                    Entry::array('values', ['bb']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 3), Entry::string('foo', 'bb'), Entry::integer('value', 1002)),
                    )
                ),
                Row::create(
                    Entry::array('entries', ['foo']),
                    Entry::array('values', ['null']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 4), Entry::null('foo'), Entry::integer('value', 1003)),
                        Entry::entries(Entry::integer('id', 7), Entry::null('foo'), Entry::integer('value', 1006)),
                        Entry::entries(Entry::integer('id', 8), Entry::null('foo'), Entry::integer('value', 1007)),
                    )
                ),
                Row::create(
                    Entry::array('entries', ['foo']),
                    Entry::array('values', ['cc']),
                    Entry::collection(
                        'rows',
                        Entry::entries(Entry::integer('id', 5), Entry::string('foo', 'cc'), Entry::integer('value', 1004)),
                        Entry::entries(Entry::integer('id', 6), Entry::string('foo', 'cc'), Entry::integer('value', 1005)),
                    )
                ),
            ),
            $rows->groupBy('foo')->toRows()
        );
    }

    public function test_group_empty_rows() : void
    {
        $rows = new Rows();

        $this->assertEquals(
            new Rows(),
            $rows->groupBy('id')->toRows()
        );
    }

    public function test_group_with_empty_entries() : void
    {
        $this->expectException(InvalidArgumentException::class);

        $rows = new Rows();
        $rows->groupBy();
    }

    public function test_merge_row_with_another_row_that_has_duplicated_entries() : void
    {
        $this->expectExceptionMessage('Merged entries names must be unique, given: [id] + [id]');
        $this->expectException(InvalidArgumentException::class);

        Row::create(new IntegerEntry('id', 1))
            ->merge(Row::create(new IntegerEntry('id', 2)), $prefix = '');
    }

    public function test_merge_row_with_another_row_using_prefix() : void
    {
        $this->assertSame(
            [
                'id' => 1,
                '_id' => 2,
            ],
            Row::create(new IntegerEntry('id', 1))
                ->merge(Row::create(new IntegerEntry('id', 2)), $prefix = '_')
                ->toArray()
        );
    }

    public function test_merges_collection_together() : void
    {
        $rowsOne = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
        );
        $rowsTwo = new Rows(
            Row::create(new IntegerEntry('id', 3)),
            Row::create(new IntegerEntry('id', 4)),
            Row::create(new IntegerEntry('id', 5))
        );

        $rowsThree = new Rows(
            Row::create(new IntegerEntry('id', 6)),
            Row::create(new IntegerEntry('id', 7)),
        );

        $merged = $rowsOne->merge($rowsTwo, $rowsThree);

        $this->assertEquals(
            new Rows(
                Row::create(new IntegerEntry('id', 1)),
                Row::create(new IntegerEntry('id', 2)),
                Row::create(new IntegerEntry('id', 3)),
                Row::create(new IntegerEntry('id', 4)),
                Row::create(new IntegerEntry('id', 5)),
                Row::create(new IntegerEntry('id', 6)),
                Row::create(new IntegerEntry('id', 7))
            ),
            $merged
        );
    }

    public function test_offset_exists_with_non_int_offset() : void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Rows())->offsetExists('a');
    }

    public function test_offset_get_on_empty_rows() : void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Rows())[5];
    }

    public function test_remove() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->remove(1);

        $this->assertCount(2, $rows);
        $this->assertSame(1, $rows[0]->valueOf('id'));
        $this->assertSame(3, $rows[1]->valueOf('id'));
    }

    public function test_remove_on_empty_rows() : void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Rows())->remove(1);
    }

    public function test_returns_first_row() : void
    {
        $rows = new Rows(
            $first = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'three')),
            Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
            Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
        );

        $this->assertEquals($first, $rows->first());
    }

    public function test_reverse() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
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
        $this->assertEquals($expected, $left->diffLeft($right));
    }

    /**
     * @dataProvider rows_diff_right_provider
     */
    public function test_rows_diff_right(Rows $expected, Rows $left, Rows $right) : void
    {
        $this->assertEquals($expected, $left->diffRight($right));
    }

    public function test_rows_serialization() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
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
    public function test_rows_unique(Rows $expected, Rows $notUnique, ?Row\Comparator $comparator = null) : void
    {
        $this->assertEquals($expected, $notUnique->unique($comparator));
    }

    public function test_sort() : void
    {
        $rows = new Rows(
            $three = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'three')),
            $one   = Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
            $five   = Row::create(new IntegerEntry('number', 5), new StringEntry('name', 'five')),
            $two   = Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
            $four  = Row::create(new IntegerEntry('number', 4), new StringEntry('name', 'four')),
        );

        $sort = $rows->sort(function (Row $row, Row $nextRow) : int {
            return $row->valueOf('number') <=> $nextRow->valueOf('number');
        });

        $this->assertEquals(new Rows($one, $two, $three, $four, $five), $sort);
        $this->assertNotEquals($sort, $rows);
    }

    public function test_sort_rows_by_not_existing_column() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "c" does not exist');

        $rows = new Rows(
            Row::create(new IntegerEntry('a', 3), new IntegerEntry('b', 2)),
            Row::create(new IntegerEntry('a', 1), new IntegerEntry('b', 5)),
            Row::create(new IntegerEntry('a', 1), new IntegerEntry('b', 4)),
            Row::create(new IntegerEntry('a', 2), new IntegerEntry('b', 7)),
            Row::create(new IntegerEntry('a', 3), new IntegerEntry('b', 10)),
            Row::create(new IntegerEntry('a', 2), new IntegerEntry('b', 4)),
        );

        $rows->sortBy(Row\Sort::asc('c'), Row\Sort::desc('b'));
    }

    public function test_sort_rows_by_two_columns() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('a', 3), new IntegerEntry('b', 2)),
            Row::create(new IntegerEntry('a', 1), new IntegerEntry('b', 5)),
            Row::create(new IntegerEntry('a', 1), new IntegerEntry('b', 4)),
            Row::create(new IntegerEntry('a', 2), new IntegerEntry('b', 7)),
            Row::create(new IntegerEntry('a', 3), new IntegerEntry('b', 10)),
            Row::create(new IntegerEntry('a', 2), new IntegerEntry('b', 4)),
        );

        $ascending = $rows->sortBy(Row\Sort::asc('a'), Row\Sort::desc('b'));
        $descending = $rows->sortBy(Row\Sort::desc('a'), Row\Sort::asc('b'));

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
        $rows = new Rows(
            $three = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'three')),
            $one   = Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
            $five   = Row::create(new IntegerEntry('number', 5), new StringEntry('name', 'five')),
            $two   = Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
            $four  = Row::create(new IntegerEntry('number', 4), new StringEntry('name', 'four')),
        );

        $ascending = $rows->sortAscending('number');
        $descending = $rows->sortDescending('number');

        $this->assertEquals(new Rows($one, $two, $three, $four, $five), $ascending);
        $this->assertEquals(new Rows($five, $four, $three, $two, $one), $descending);
        $this->assertNotEquals($ascending, $rows);
        $this->assertNotEquals($descending, $rows);
    }

    public function test_sorts_entries_in_all_rows() : void
    {
        $rows = new Rows(
            Row::create(
                $rowOneId = new IntegerEntry('id', 1),
                $rowOneDeleted = new BooleanEntry('deleted', true),
                $rowOnePhase = new NullEntry('phase'),
                $rowOneCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
            ),
            Row::create(
                $rowTwoDeleted = new BooleanEntry('deleted', true),
                $rowTwoCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
                $rowTwoId = new IntegerEntry('id', 1),
                $rowTwoPhase = new NullEntry('phase'),
            ),
        );

        $sorted = $rows->sortEntries();

        $this->assertEquals(
            new Rows(
                Row::create(
                    $rowOneCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
                    $rowOneDeleted = new BooleanEntry('deleted', true),
                    $rowOneId = new IntegerEntry('id', 1),
                    $rowOnePhase = new NullEntry('phase'),
                ),
                Row::create(
                    $rowTwoCreatedAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-08-13 15:00')),
                    $rowTwoDeleted = new BooleanEntry('deleted', true),
                    $rowTwoId = new IntegerEntry('id', 1),
                    $rowTwoPhase = new NullEntry('phase'),
                )
            ),
            $sorted
        );
    }

    public function test_take() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->take(1);

        $this->assertCount(1, $rows);
        $this->assertSame(1, $rows[0]->valueOf('id'));
    }

    public function test_take_all() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->take(3);

        $this->assertCount(3, $rows);
        $this->assertSame(1, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
        $this->assertSame(3, $rows[2]->valueOf('id'));
    }

    public function test_take_more_than_exists() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->take(4);

        $this->assertCount(3, $rows);
        $this->assertSame(1, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
        $this->assertSame(3, $rows[2]->valueOf('id'));
    }

    public function test_take_right() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->takeRight(1);

        $this->assertCount(1, $rows);
        $this->assertSame(3, $rows[0]->valueOf('id'));
    }

    public function test_take_right_all() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->takeRight(3);

        $this->assertCount(3, $rows);
        $this->assertSame(3, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
        $this->assertSame(1, $rows[2]->valueOf('id'));
    }

    public function test_take_right_more_than_exists() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('id', 1)),
            Row::create(new IntegerEntry('id', 2)),
            Row::create(new IntegerEntry('id', 3)),
        );

        $rows = $rows->takeRight(4);

        $this->assertCount(3, $rows);
        $this->assertSame(3, $rows[0]->valueOf('id'));
        $this->assertSame(2, $rows[1]->valueOf('id'));
        $this->assertSame(1, $rows[2]->valueOf('id'));
    }

    public function test_transforms_rows_to_array() : void
    {
        $rows = new Rows(
            Row::create(
                new IntegerEntry('id', 1234),
                new BooleanEntry('deleted', false),
                new NullEntry('phase'),
            ),
            Row::create(
                new IntegerEntry('id', 4321),
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

    public function unique_rows_provider() : \Generator
    {
        yield 'simple identical rows' => [
            $expected = new Rows(Row::create(new IntegerEntry('number', 1))),
            $notUnique = new Rows(
                Row::create(new IntegerEntry('number', 1)),
                Row::create(new IntegerEntry('number', 1))
            ),
            $comparator = null,
        ];

        yield 'simple identical rows with objects' => [
            $expected = new Rows(Row::create(new ObjectEntry('object', new \stdClass()))),
            $notUnique = new Rows(
                Row::create(new ObjectEntry('object', $object = new \stdClass())),
                Row::create(new ObjectEntry('object', $object = new \stdClass()))
            ),
            $comparator = new Row\Comparator\WeakObjectComparator(),
        ];
    }
}
