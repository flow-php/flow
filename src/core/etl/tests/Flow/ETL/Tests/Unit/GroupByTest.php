<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class GroupByTest extends TestCase
{
    public function test_group_by_array_entry() : void
    {
        $this->expectExceptionMessage('Grouping by non scalar values is not supported, given: array');
        $this->expectException(RuntimeException::class);

        $groupBy = new GroupBy('array');

        $groupBy->group(new Rows(
            Row::create(Entry::array('array', [1, 2, 3])),
            Row::create(Entry::array('array', [1, 2, 3])),
            Row::create(Entry::array('array', [4, 5, 6]))
        ));
    }

    public function test_group_by_missing_entry() : void
    {
        $groupBy = new GroupBy('type');

        $groupBy->group(new Rows(
            Row::create(Entry::string('type', 'a')),
            Row::create(Entry::string('not-type', 'b')),
            Row::create(Entry::string('type', 'c'))
        ));

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('type', 'a')),
                Row::create(Entry::null('type')),
                Row::create(Entry::string('type', 'c'))
            ),
            $groupBy->result(new FlowContext(Config::default()))
        );
    }

    public function test_group_by_with_empty_aggregations() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Aggregations can't be empty");
        $groupBy = new GroupBy();
        $groupBy->aggregate();
    }
}
