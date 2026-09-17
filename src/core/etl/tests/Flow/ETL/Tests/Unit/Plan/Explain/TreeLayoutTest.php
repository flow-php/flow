<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Explain\Outline;
use Flow\ETL\Plan\Explain\TreeLayout;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node\Collect;
use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Offset;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Rename;
use Flow\ETL\Plan\Node\RenameEach;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Until;
use Flow\ETL\Plan\Node\WithColumn;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rename_replace;
use function Flow\ETL\DSL\to_memory;

final class TreeLayoutTest extends FlowTestCase
{
    public function test_details_sit_under_their_node_inside_its_branch(): void
    {
        $plan = new Result(NodeMother::limit(new Collect(NodeMother::read()->withLimit(5)), 5));

        static::assertSame(<<<'PLAN'
            #4 Result
            │  Rows fetch() returns and run() streams
            └─ #3 Limit
               │  Limit: 5
               └─ #2 Collect
                  │  Buffers all rows before passing them on
                  └─ #1 Read
                        Extractor: ArrayExtractor
                        Limit: 5
            PLAN, (new TreeLayout())->render((new Outline())->of($plan)));
    }

    public function test_a_shared_node_is_referenced_by_number_and_name(): void
    {
        $filter = new Filter(NodeMother::read(), ref('id')->isNotNull());
        $plan = new Outputs(new Result($filter), new Sinks(new Write($filter, to_memory(new ArrayMemory()))));

        static::assertSame(<<<'PLAN'
            Outputs
            ├─ #3 Result
            │  │  Rows fetch() returns and run() streams
            │  └─ #2 Filter
            │     │  Condition: IsNotNull
            │     └─ #1 Read
            │           Extractor: ArrayExtractor
            └─ #4 Write
               │  Loader: MemoryLoader
               └─ #2 Filter (shared)
            PLAN, (new TreeLayout())->render((new Outline())->of($plan)));
    }

    public function test_every_child_but_the_last_keeps_the_rail_open(): void
    {
        $read = NodeMother::read();
        $plan = new Outputs(
            new Result($read),
            new Sinks(
                new Write($read, to_memory(new ArrayMemory())),
                new Write(NodeMother::select($read), to_memory(new ArrayMemory())),
            ),
        );

        static::assertSame(<<<'PLAN'
            Outputs
            ├─ #2 Result
            │  │  Rows fetch() returns and run() streams
            │  └─ #1 Read
            │        Extractor: ArrayExtractor
            ├─ #3 Write
            │  │  Loader: MemoryLoader
            │  └─ #1 Read (shared)
            └─ #5 Write
               │  Loader: MemoryLoader
               └─ #4 Select
                  └─ #1 Read (shared)
            PLAN, (new TreeLayout())->render((new Outline())->of($plan)));
    }

    public function test_with_declarations_a_single_chain_prints_one_line_per_node_with_its_declarations(): void
    {
        $plan = new LogicalPlan(new Result(NodeMother::limit(NodeMother::select(NodeMother::read()), 5)));

        static::assertSame(<<<'PLAN'
            #4 Result  preserving · transparent · streaming
            │  Rows fetch() returns and run() streams
            └─ #3 Limit  reducing · transparent · streaming
               │  Limit: 5
               └─ #2 Select  preserving · transparent · streaming
                  └─ #1 Read  source · transparent · streaming
                        Extractor: ArrayExtractor
            PLAN, (new TreeLayout(declarations: true))->render((new Outline())->of($plan->root)));
    }

    public function test_with_declarations_a_subtree_two_consumers_share_is_printed_once(): void
    {
        $filter = new Filter(NodeMother::read(), ref('id')->isNotNull());
        $plan = new LogicalPlan(
            new Outputs(new Result($filter), new Sinks(new Write($filter, to_memory(new ArrayMemory())))),
        );

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque · streaming
            ├─ #3 Result  preserving · transparent · streaming
            │  │  Rows fetch() returns and run() streams
            │  └─ #2 Filter  reducing · transparent · streaming
            │     │  Condition: IsNotNull
            │     └─ #1 Read  source · transparent · streaming
            │           Extractor: ArrayExtractor
            └─ #4 Write  preserving · opaque · streaming
               │  Loader: MemoryLoader
               └─ #2 Filter (shared)
            PLAN, (new TreeLayout(declarations: true))->render((new Outline())->of($plan->root)));
    }

    public function test_with_declarations_payloads_and_redefinitions(): void
    {
        $read = NodeMother::read();
        $plan = new LogicalPlan(new Result(
            new Until(
                new Offset(
                    new Rename(
                        new WithColumn(new RenameEach($read, [rename_replace('_', '-')]), int_schema('n'), lit(1)),
                        'n',
                        'm',
                    ),
                    2,
                ),
                lit(true),
            ),
        ));

        static::assertSame(<<<'PLAN'
            #7 Result  preserving · transparent · streaming
            │  Rows fetch() returns and run() streams
            └─ #6 Until  reducing · transparent · streaming
               │  Until: Literal
               └─ #5 Offset  reducing · transparent · streaming
                  │  Skip: 2
                  └─ #4 Rename  preserving · transparent · streaming · redefines m
                     │  Rename: n → m
                     └─ #3 WithColumn  preserving · transparent · streaming · redefines n
                        │  Column: n = Literal
                        └─ #2 RenameEach  preserving · transparent · streaming · redefines unknown
                           └─ #1 Read  source · transparent · streaming
                                 Extractor: ArrayExtractor
            PLAN, (new TreeLayout(declarations: true))->render((new Outline())->of($plan->root)));
    }

    public function test_with_declarations_a_frame_shows_its_own_plan_under_it(): void
    {
        $plan = new LogicalPlan(new Result(
            new CrossJoin(NodeMother::read(), NodeMother::frame(NodeMother::plan(NodeMother::read()))),
        ));

        static::assertSame(<<<'PLAN'
            #6 Result  preserving · transparent · streaming
            │  Rows fetch() returns and run() streams
            └─ #5 CrossJoin  expanding · opaque · streaming · redefines unknown
               ├─ #1 Read  source · transparent · streaming
               │     Extractor: ArrayExtractor
               └─ #4 SideInput  preserving · opaque · streaming
                  └─ #3 Result  preserving · transparent · streaming
                     │  Rows fetch() returns and run() streams
                     └─ #2 Read  source · transparent · streaming
                           Extractor: ArrayExtractor
            PLAN, (new TreeLayout(declarations: true))->render((new Outline())->of($plan->root)));
    }

    public function test_with_declarations_a_pushed_limit_shows_on_the_read_line(): void
    {
        $plan = new LogicalPlan(new Result(NodeMother::read()->withLimit(3)));

        static::assertStringContainsString(
            "Read  source · transparent · streaming\n      Extractor: ArrayExtractor\n      Limit: 3",
            (new TreeLayout(declarations: true))->render((new Outline())->of($plan->root)),
        );
    }

    public function test_with_declarations_a_transaction_root_child_lists_its_writes(): void
    {
        $read = NodeMother::read();
        $plan = new LogicalPlan(
            new Outputs(
                new Result($read),
                new Sinks(
                    new Transaction(
                        new RecordingTransaction(),
                        new Write($read, to_memory(new ArrayMemory())),
                        new Write(NodeMother::select($read), to_memory(new ArrayMemory())),
                    ),
                ),
            ),
        );

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque · streaming
            ├─ #2 Result  preserving · transparent · streaming
            │  │  Rows fetch() returns and run() streams
            │  └─ #1 Read  source · transparent · streaming
            │        Extractor: ArrayExtractor
            └─ #6 Transaction  preserving · opaque · streaming
               ├─ #3 Write  preserving · opaque · streaming
               │  │  Loader: MemoryLoader
               │  └─ #1 Read (shared)
               └─ #5 Write  preserving · opaque · streaming
                  │  Loader: MemoryLoader
                  └─ #4 Select  preserving · transparent · streaming
                     └─ #1 Read (shared)
            PLAN, (new TreeLayout(declarations: true))->render((new Outline())->of($plan->root)));
    }
}
