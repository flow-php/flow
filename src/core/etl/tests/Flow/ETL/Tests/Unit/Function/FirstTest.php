<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{first, int_entry, ref, str_entry, string_entry};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class FirstTest extends TestCase
{
    public function test_aggregation_firs_value() : void
    {
        $aggregator = first(ref('int'));

        $aggregator->aggregate(Row::create(int_entry('not_int', null)));
        $aggregator->aggregate(Row::create(str_entry('int', '10')));
        $aggregator->aggregate(Row::create(str_entry('int', '20')));
        $aggregator->aggregate(Row::create(str_entry('int', '55')));
        $aggregator->aggregate(Row::create(str_entry('int', '25')));

        self::assertSame(
            '10',
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_firs_value_when_nothing_aggregated() : void
    {
        $aggregator = first(ref('int'));

        self::assertEquals(
            string_entry('int_first', null),
            $aggregator->result()
        );
    }
}
