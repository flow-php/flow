<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Explain;

use Flow\ETL\Planning\Explain\LogicalOutline;
use Flow\ETL\Planning\Explain\TreeLayout;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node\Collect;
use Flow\ETL\Planning\Logical\Node\CrossJoin;
use Flow\ETL\Planning\Logical\Node\Filter;
use Flow\ETL\Planning\Logical\Node\Offset;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Rename;
use Flow\ETL\Planning\Logical\Node\RenameEach;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Transaction;
use Flow\ETL\Planning\Logical\Node\Until;
use Flow\ETL\Planning\Logical\Node\WithColumn;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Logical\Trigger;
use Flow\ETL\Planning\Logical\Writes;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\RenderedNodeMother;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rename_replace;
use function Flow\ETL\DSL\to_memory;

final class TreeLayoutTest extends FlowTestCase
{
    public function test_a_leaf_has_no_branches(): void
    {
        static::assertSame([], (new TreeLayout())->branches(RenderedNodeMother::named('Read', 1), '│  '));
    }

    public function test_branches_keep_the_rail_open_for_every_child_but_the_last(): void
    {
        $first = RenderedNodeMother::named('Read', 2);
        $second = RenderedNodeMother::named('Read', 3);
        $third = RenderedNodeMother::named('Read', 4);

        static::assertSame(
            [
                [$first,  '│  ├─ ', '│  │  '],
                [$second, '│  ├─ ', '│  │  '],
                [$third,  '│  └─ ', '│     '],
            ],
            (new TreeLayout())->branches(RenderedNodeMother::named('Read', 1, [$first, $second, $third]), '│  '),
        );
    }

    public function test_details_sit_under_their_node_inside_its_branch(): void
    {
        static::assertSame(<<<'PLAN'
            #4 Result
            │  Rows this plan hands out: to the trigger, or to the node reading it
            └─ #3 Limit
               │  Limit: 5
               └─ #2 Collect
                  └─ #1 Read
                        Extractor: ArrayExtractor
                        Limit: 5
            PLAN, (new TreeLayout())->render((new LogicalOutline())->of(new Result(NodeMother::limit(
            new Collect(NodeMother::read()->withLimit(5)),
            5,
        )))));
    }

    public function test_a_shared_node_is_referenced_by_number_and_name(): void
    {
        $filter = new Filter(NodeMother::read(), ref('id')->isNotNull());

        static::assertSame(<<<'PLAN'
            Outputs
            ├─ #3 Result
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #2 Filter
            │     │  Condition: id IS NOT NULL
            │     └─ #1 Read
            │           Extractor: ArrayExtractor
            └─ #4 Write
               │  Loader: MemoryLoader
               └─ #2 Filter (shared)
            PLAN, (new TreeLayout())->render((new LogicalOutline())->of(
            new Outputs(new Result($filter), new Write($filter, to_memory(new ArrayMemory()))),
        )));
    }

    public function test_every_child_but_the_last_keeps_the_rail_open(): void
    {
        $read = NodeMother::read();

        static::assertSame(<<<'PLAN'
            Outputs
            ├─ #2 Result
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #1 Read
            │        Extractor: ArrayExtractor
            ├─ #3 Write
            │  │  Loader: MemoryLoader
            │  └─ #1 Read (shared)
            └─ #5 Write
               │  Loader: MemoryLoader
               └─ #4 Select
                  │  Keeps only: id
                  └─ #1 Read (shared)
            PLAN, (new TreeLayout())->render((new LogicalOutline())->of(
            new Outputs(
                new Result($read),
                new Write($read, to_memory(new ArrayMemory())),
                new Write(NodeMother::select($read), to_memory(new ArrayMemory())),
            ),
        )));
    }

    public function test_with_declarations_a_single_chain_prints_one_line_per_node_with_its_declarations(): void
    {
        static::assertSame(<<<'PLAN'
            #4 Result  preserving · transparent
            │  Rows this plan hands out: to the trigger, or to the node reading it
            └─ #3 Limit  reducing · transparent
               │  Limit: 5
               └─ #2 Select  preserving · transparent · redefines all but id
                  │  Keeps only: id
                  └─ #1 Read  source · transparent
                        Extractor: ArrayExtractor
            PLAN, (new TreeLayout())->render((new LogicalOutline(
            declarations: true,
        ))->of((new LogicalPlan(new Result(NodeMother::limit(NodeMother::select(NodeMother::read()), 5))))->root)));
    }

    public function test_with_declarations_a_subtree_two_consumers_share_is_printed_once(): void
    {
        $filter = new Filter(NodeMother::read(), ref('id')->isNotNull());

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque
            ├─ #3 Result  preserving · transparent
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #2 Filter  reducing · transparent
            │     │  Condition: id IS NOT NULL
            │     └─ #1 Read  source · transparent
            │           Extractor: ArrayExtractor
            └─ #4 Write  preserving · opaque
               │  Loader: MemoryLoader
               └─ #2 Filter (shared)
            PLAN, (new TreeLayout())->render((new LogicalOutline(declarations: true))->of((new LogicalPlan(
            new Outputs(new Result($filter), new Write($filter, to_memory(new ArrayMemory()))),
        ))->root)));
    }

    public function test_with_declarations_payloads_and_redefinitions(): void
    {
        static::assertSame(<<<'PLAN'
            #7 Result  preserving · transparent
            │  Rows this plan hands out: to the trigger, or to the node reading it
            └─ #6 Until  reducing · transparent
               │  Until: true
               └─ #5 Offset  reducing · transparent
                  │  Skip: 2
                  └─ #4 Rename  preserving · transparent · redefines m, n
                     │  Rename: n → m
                     └─ #3 WithColumn  preserving · transparent · redefines n
                        │  Column: n = 1
                        └─ #2 RenameEach  preserving · transparent · redefines unknown
                           └─ #1 Read  source · transparent
                                 Extractor: ArrayExtractor
            PLAN, (new TreeLayout())->render((new LogicalOutline(declarations: true))->of((new LogicalPlan(new Result(
            new Until(
                new Offset(
                    new Rename(
                        new WithColumn(
                            new RenameEach(NodeMother::read(), [rename_replace('_', '-')]),
                            int_schema('n'),
                            lit(1),
                        ),
                        'n',
                        'm',
                    ),
                    2,
                ),
                lit(true),
            ),
        )))->root)));
    }

    public function test_with_declarations_a_joined_frame_is_part_of_the_tree(): void
    {
        static::assertSame(<<<'PLAN'
            #4 Result  preserving · transparent
            │  Rows this plan hands out: to the trigger, or to the node reading it
            └─ #3 CrossJoin  expanding · opaque · redefines unknown
               │  Type: cross
               ├─ #1 Read  source · transparent
               │     Extractor: ArrayExtractor
               └─ #2 Read  source · transparent
                     Extractor: ArrayExtractor
            PLAN, (new TreeLayout())->render((new LogicalOutline(declarations: true))->of((new LogicalPlan(new Result(
            new CrossJoin(NodeMother::read(), NodeMother::joinRight(NodeMother::plan(NodeMother::read()))),
        )))->root)));
    }

    public function test_with_declarations_a_pushed_limit_shows_on_the_read_line(): void
    {
        static::assertStringContainsString(
            "Read  source · transparent\n      Extractor: ArrayExtractor\n      Limit: 3",
            (new TreeLayout())->render((new LogicalOutline(
                declarations: true,
            ))->of((new LogicalPlan(new Result(NodeMother::read()->withLimit(3))))->root)),
        );
    }

    public function test_with_declarations_a_run_trigger_keeps_a_result_beside_the_write(): void
    {
        $read = NodeMother::read();

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque
            ├─ #3 Result  preserving · transparent
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #2 Select  preserving · transparent · redefines all but id
            │     │  Keeps only: id
            │     └─ #1 Read  source · transparent
            │           Extractor: ArrayExtractor
            └─ #4 Write  preserving · opaque
               │  Loader: MemoryLoader
               └─ #1 Read (shared)
            PLAN, (new TreeLayout())->render((new LogicalOutline(declarations: true))->of(Trigger::run->plan(
            NodeMother::select($read),
            new Writes(new Write($read, to_memory(new ArrayMemory()))),
        )->root)));
    }

    public function test_with_declarations_a_transaction_root_child_lists_its_writes(): void
    {
        $read = NodeMother::read();

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque
            ├─ #2 Result  preserving · transparent
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #1 Read  source · transparent
            │        Extractor: ArrayExtractor
            └─ #6 Transaction  preserving · opaque
               ├─ #3 Write  preserving · opaque
               │  │  Loader: MemoryLoader
               │  └─ #1 Read (shared)
               └─ #5 Write  preserving · opaque
                  │  Loader: MemoryLoader
                  └─ #4 Select  preserving · transparent · redefines all but id
                     │  Keeps only: id
                     └─ #1 Read (shared)
            PLAN, (new TreeLayout())->render((new LogicalOutline(declarations: true))->of((new LogicalPlan(
            new Outputs(
                new Result($read),
                new Transaction(
                    new RecordingTransaction(),
                    new Write($read, to_memory(new ArrayMemory())),
                    new Write(NodeMother::select($read), to_memory(new ArrayMemory())),
                ),
            ),
        ))->root)));
    }
}
