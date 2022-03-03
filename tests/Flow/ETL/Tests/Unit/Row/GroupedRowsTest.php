<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\GroupedRows;
use PHPUnit\Framework\TestCase;

final class GroupedRowsTest extends TestCase
{
    public function test_creating_group_with_empty_entries() : void
    {
        $this->expectException(InvalidArgumentException::class);
        new GroupedRows();
    }

    public function test_grouping_rows_when_group_by_entry_is_missing() : void
    {
        $groupedRows = new GroupedRows('id', 'name');

        $groupedRows->add(Row::create(Entry::integer('id', 1)));
        $groupedRows->add(Row::create(Entry::integer('id', 2)));
        $groupedRows->add(Row::create(Entry::integer('id', 2), Entry::string('name', 'test')));
        $groupedRows->add(Row::create(Entry::integer('id', 2), Entry::string('name', 'test')));
        $groupedRows->add(Row::create(Entry::integer('id', 3), Entry::string('name', 'test')));

        $this->assertEquals(
            [
                ['entries' => ['id', 'name'], 'values' => ['1', 'null'], 'rows' => [['id' => 1, 'name' => null]]],
                ['entries' => ['id', 'name'], 'values' => ['2', 'null'], 'rows' => [['id' => 2, 'name' => null]]],
                ['entries' => ['id', 'name'], 'values' => ['2', 'test'], 'rows' => [['id' => 2, 'name' => 'test'], ['id' => 2, 'name' => 'test']]],
                ['entries' => ['id', 'name'], 'values' => ['3', 'test'], 'rows' => [['id' => 3, 'name' => 'test']]],
            ],
            $groupedRows->toRows()->toArray()
        );
    }
}
