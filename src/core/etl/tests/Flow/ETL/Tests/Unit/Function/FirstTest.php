<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{first, int_entry, ref, str_entry, string_entry};
use Flow\ETL\Tests\FlowTestCase;

final class FirstTest extends FlowTestCase
{
    public function test_aggregation_firs_value() : void
    {
        $aggregator = first(ref('int'));

        $aggregator->aggregate(row(int_entry('not_int', null)));
        $aggregator->aggregate(row(str_entry('int', '10')));
        $aggregator->aggregate(row(str_entry('int', '20')));
        $aggregator->aggregate(row(str_entry('int', '55')));
        $aggregator->aggregate(row(str_entry('int', '25')));

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
