<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Optimizer\Rule;

use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Offset;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Planning\Optimizer\Rule\PushOffsetIntoSource;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\PartitionedSourceMother;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class PushOffsetIntoSourceTest extends FlowTestCase
{
    public function test_an_offset_directly_above_a_file_read_is_pushed_and_removed(): void
    {
        $plan = (new PushOffsetIntoSource())->apply(
            NodeMother::plan(new Offset(NodeMother::read(PartitionedSourceMother::yearMonth()), 5)),
            config()->backend(),
        );

        static::assertSame(5, $plan->source()->criteria()->offset);
        static::assertSame($plan->source(), $plan->root->children()[0]);
    }

    public function test_stacked_offsets_add_up(): void
    {
        static::assertSame(
            7,
            (new PushOffsetIntoSource())
                ->apply(
                    NodeMother::plan(
                        new Offset(new Offset(NodeMother::read(PartitionedSourceMother::yearMonth()), 3), 4),
                    ),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->offset,
        );
    }

    public function test_an_offset_passes_a_select(): void
    {
        static::assertSame(
            5,
            (new PushOffsetIntoSource())
                ->apply(
                    NodeMother::plan(
                        new Offset(
                            NodeMother::select(NodeMother::read(PartitionedSourceMother::yearMonth()), 'year'),
                            5,
                        ),
                    ),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->offset,
        );
    }

    public function test_an_offset_does_not_pass_a_filter(): void
    {
        $plan = NodeMother::plan(
            new Offset(
                new Node\Filter(NodeMother::read(PartitionedSourceMother::yearMonth()), ref('value')->equals(lit('a'))),
                5,
            ),
        );

        static::assertSame($plan, (new PushOffsetIntoSource())->apply($plan, config()->backend()));
    }

    public function test_an_offset_does_not_pass_a_limit(): void
    {
        $plan = NodeMother::plan(
            new Offset(NodeMother::limit(NodeMother::read(PartitionedSourceMother::yearMonth()), 3), 1),
        );

        static::assertSame($plan, (new PushOffsetIntoSource())->apply($plan, config()->backend()));
    }

    public function test_an_offset_over_a_source_that_is_no_file_extractor_stays(): void
    {
        $plan = NodeMother::plan(new Offset(NodeMother::read(), 5));

        static::assertSame($plan, (new PushOffsetIntoSource())->apply($plan, config()->backend()));
    }

    public function test_an_offset_of_one_consumer_never_narrows_another(): void
    {
        $read = NodeMother::read(PartitionedSourceMother::yearMonth());
        $plan = new LogicalPlan(
            new Outputs(new Result(new Offset($read, 5)), new Write($read, to_memory(new ArrayMemory()))),
        );

        static::assertSame($plan, (new PushOffsetIntoSource())->apply($plan, config()->backend()));
    }

    public function test_the_default_optimizer_pushes_the_offset_then_the_limit_counted_after_it(): void
    {
        $criteria = Optimizer::default()
            ->optimize(
                NodeMother::plan(NodeMother::limit(
                    new Offset(NodeMother::read(PartitionedSourceMother::yearMonth()), 100),
                    10,
                )),
                config()->backend(),
            )
            ->source()
            ->criteria();

        static::assertSame(100, $criteria->offset);
        static::assertSame(10, $criteria->limit);
    }
}
