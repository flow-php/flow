<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy;

use Flow\ETL\GroupBy\Aggregators;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;

final class AggregatorsTest extends FlowTestCase
{
    public function test_references_are_empty_without_aggregators(): void
    {
        static::assertSame([], (new Aggregators())->references());
    }

    public function test_references_deduplicate_by_base_entry(): void
    {
        static::assertEquals(
            [ref('amount')],
            (new Aggregators(sum(ref('amount')), average(ref('amount'))))->references(),
        );
    }

    public function test_references_union_all_aggregators(): void
    {
        static::assertEquals(
            [ref('order_id'), ref('discount')],
            (new Aggregators(count(ref('order_id')), sum(ref('discount'))))->references(),
        );
    }

    public function test_references_are_null_when_any_aggregator_cannot_enumerate_them(): void
    {
        static::assertNull(
            (new Aggregators(count(ref('order_id')), sum(ref('discount'), exact: ref('flag'))))->references(),
        );
    }
}
