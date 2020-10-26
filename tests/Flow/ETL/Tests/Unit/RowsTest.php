<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\CollectionEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class RowsTest extends TestCase
{
    /**
     * @dataProvider groups_entries_data_provider
     */
    public function test_groups_entries(string $collectionEntryName, Rows $initial, callable $groupBy, Rows $grouped) : void
    {
        $this->assertEquals($grouped, $initial->groupTo($collectionEntryName, $groupBy));
    }

    public function groups_entries_data_provider() : \Generator
    {
        $collectionEntryName = 'items';

        yield 'group odd and even numbers' => [
            $collectionEntryName,
            new Rows(
                $one   = Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
                $two   = Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
                $three = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'three')),
                $four  = Row::create(new IntegerEntry('number', 4), new StringEntry('name', 'four')),
                $five   = Row::create(new IntegerEntry('number', 5), new StringEntry('name', 'five'))
            ),
            fn (Row $row) => $row->get('number')->value() % 2 === 0 ? 'even' : 'odd',
            new Rows(
                Row::create(new CollectionEntry($collectionEntryName, $two->entries(), $four->entries())),
                Row::create(new CollectionEntry($collectionEntryName, $one->entries(), $three->entries(), $five->entries()))
            ),
        ];

        yield 'group by order number' => [
            $collectionEntryName,
            new Rows(
                $orderOneItemOne   = Row::create(new IntegerEntry('order-number', 1), new StringEntry('item', 'one')),
                $orderTwoItemOne   = Row::create(new IntegerEntry('order-number', 2), new StringEntry('item', 'one')),
                $orderOneItemTwo   = Row::create(new IntegerEntry('order-number', 1), new StringEntry('item', 'two')),
                $orderOneItemThree = Row::create(new IntegerEntry('order-number', 1), new StringEntry('item', 'three')),
                $orderThreeItemOne = Row::create(new IntegerEntry('order-number', 3), new StringEntry('item', 'one')),
                $orderTwoItemTwo   = Row::create(new IntegerEntry('order-number', 2), new StringEntry('item', 'two')),
            ),
            fn (Row $row) => $row->get('order-number')->value(),
            new Rows(
                Row::create(new CollectionEntry($collectionEntryName, $orderOneItemOne->entries(), $orderOneItemTwo->entries(), $orderOneItemThree->entries())),
                Row::create(new CollectionEntry($collectionEntryName, $orderThreeItemOne->entries())),
                Row::create(new CollectionEntry($collectionEntryName, $orderTwoItemOne->entries(), $orderTwoItemTwo->entries()))
            ),
        ];
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

    public function test_returns_first_row() : void
    {
        $rows = new Rows(
            $first = Row::create(new IntegerEntry('number', 3), new StringEntry('name', 'three')),
            Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
            Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
        );

        $this->assertEquals($first, $rows->first());
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

    /**
     * @dataProvider rows_diff_left_provider
     */
    public function test_rows_diff_left(Rows $expected, Rows $left, Rows $right) : void
    {
        $this->assertEquals($expected, $left->diffLeft($right));
    }

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

    /**
     * @dataProvider rows_diff_right_provider
     */
    public function test_rows_diff_right(Rows $expected, Rows $left, Rows $right) : void
    {
        $this->assertEquals($expected, $left->diffRight($right));
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
}
