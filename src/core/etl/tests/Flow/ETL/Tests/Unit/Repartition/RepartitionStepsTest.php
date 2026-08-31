<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Repartition;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\RepartitionProcessor;
use Flow\ETL\Repartition\RepartitionSteps;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\hash_repartition;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

final class RepartitionStepsTest extends FlowTestCase
{
    public function test_an_algorithm_builder_pins_the_algorithm_for_this_operation(): void
    {
        $steps = RepartitionSteps::of(
            refs(ref('id')),
            config_builder()->build(),
            hash_repartition()->storage(new MemoryBuckets())->bucketsCount(8),
        );

        static::assertCount(2, $steps);
        static::assertInstanceOf(BucketingProcessor::class, $steps[0]);
        static::assertInstanceOf(RepartitionProcessor::class, $steps[1]);
    }

    public function test_config_supplies_the_algorithm_when_none_is_pinned(): void
    {
        $steps = RepartitionSteps::of(
            refs(ref('id')),
            config_builder()->repartition(hash_repartition()->storage(new MemoryBuckets()))->build(),
        );

        static::assertCount(2, $steps);
        static::assertInstanceOf(BucketingProcessor::class, $steps[0]);
        static::assertInstanceOf(RepartitionProcessor::class, $steps[1]);
    }
}
