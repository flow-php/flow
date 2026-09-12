<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregators;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ListColumnsMother;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;

final class AggregatorsTest extends FlowTestCase
{
    public function test_first_is_refused_without_aggregators(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Aggregators are empty.');

        (new Aggregators())->first();
    }

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

    public function test_resolved_refuses_an_expand_in_an_aggregate(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'aggregate() cannot contain array_expand(), it turns one row into many rows. Expand with withEntry() first, then use the new column.',
        );

        (new Aggregators(sum(ref('n'), ref('flags')->expand()->equals(lit(true)))))->resolved(
            ListColumnsMother::numberAndFlagsSchema(),
        );
    }
}
