<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\RowsGroup;
use PHPUnit\Framework\TestCase;

final class RowsGroupTest extends TestCase
{
    public function test_adding_row_to_group_with_missing_entries() : void
    {
        $this->expectException(InvalidArgumentException::class);

        $group = new RowsGroup(['id', 'name'], ['1', 'test']);

        $group->add(new Row(Entry::entries(Entry::integer('id', 1))));
    }

    public function test_adding_row_to_group_with_not_matching_values() : void
    {
        $this->expectException(InvalidArgumentException::class);

        $group = new RowsGroup(['id', 'name'], ['1', 'test']);

        $group->add(new Row(Entry::entries(Entry::integer('id', 1), Entry::string('name', 'not-test'))));
    }
}
