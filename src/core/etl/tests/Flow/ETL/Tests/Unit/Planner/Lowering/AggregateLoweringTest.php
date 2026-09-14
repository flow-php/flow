<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\GroupBy;
use Flow\ETL\Plan\Node\Aggregate;
use Flow\ETL\Planner\Lowering\AggregateLowering;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\PruneEntriesTransformer;

use function array_map;
use function Flow\ETL\DSL\hash_group_by;

final class AggregateLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [PruneEntriesTransformer::class, BucketingProcessor::class, GroupByAggregationProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new AggregateLowering())->steps(
                    new Aggregate(NodeMother::read(), new GroupBy('id'), hash_group_by()),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
