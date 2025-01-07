<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{last, ref, str_entry, string_entry};
use Flow\ETL\Tests\FlowTestCase;

final class LastTest extends FlowTestCase
{
    public function test_aggregation_last_value() : void
    {
        $aggregator = last(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')));
        $aggregator->aggregate(row(str_entry('int', '20')));
        $aggregator->aggregate(row(str_entry('int', '55')));
        $aggregator->aggregate(row(str_entry('int', '25')));
        $aggregator->aggregate(row(str_entry('not_int', null)));

        self::assertSame(
            '25',
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_last_value_when_nothing_aggregated() : void
    {
        $aggregator = last(ref('int'));

        self::assertEquals(
            string_entry('int_last', null),
            $aggregator->result()
        );
    }
}
