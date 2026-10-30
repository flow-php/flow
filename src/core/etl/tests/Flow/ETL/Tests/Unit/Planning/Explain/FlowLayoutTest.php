<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Explain;

use Flow\ETL\Planning\Explain\FlowLayout;
use Flow\ETL\Planning\Explain\LogicalOutline;
use Flow\ETL\Planning\Logical\Node\Collect;
use Flow\ETL\Planning\Logical\Node\CrossJoin;
use Flow\ETL\Planning\Logical\Node\Filter;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class FlowLayoutTest extends FlowTestCase
{
    public function test_a_chain_is_printed_from_its_source_to_its_result(): void
    {
        static::assertSame(<<<'PLAN'
            #1 Read
            │  Extractor: ArrayExtractor
            │  Limit: 5
            └─ #2 Collect
               └─ #3 Limit
                  │  Limit: 5
                  └─ #4 Result
                        Rows this plan hands out: to the trigger, or to the node reading it
            PLAN, (new FlowLayout())->render((new LogicalOutline())->of(new Result(NodeMother::limit(
            new Collect(NodeMother::read()->withLimit(5)),
            5,
        )))));
    }

    public function test_a_node_read_by_several_consumers_branches_and_outputs_is_left_out(): void
    {
        $filter = new Filter(NodeMother::read(), ref('id')->isNotNull());

        static::assertSame(<<<'PLAN'
            #1 Read
            │  Extractor: ArrayExtractor
            └─ #2 Filter
               │  Condition: id IS NOT NULL
               ├─ #3 Result
               │     Rows this plan hands out: to the trigger, or to the node reading it
               └─ #4 Write
                     Loader: MemoryLoader
            PLAN, (new FlowLayout())->render((new LogicalOutline())->of(
            new Outputs(new Result($filter), new Write($filter, to_memory(new ArrayMemory()))),
        )));
    }

    public function test_every_reader_of_a_node_hangs_under_it(): void
    {
        $read = NodeMother::read();

        static::assertSame(<<<'PLAN'
            #1 Read
            │  Extractor: ArrayExtractor
            ├─ #2 Result
            │     Rows this plan hands out: to the trigger, or to the node reading it
            ├─ #3 Write
            │     Loader: MemoryLoader
            └─ #4 Select
               │  Keeps only: id
               └─ #5 Write
                     Loader: MemoryLoader
            PLAN, (new FlowLayout())->render((new LogicalOutline())->of(
            new Outputs(
                new Result($read),
                new Write($read, to_memory(new ArrayMemory())),
                new Write(NodeMother::select($read), to_memory(new ArrayMemory())),
            ),
        )));
    }

    public function test_every_source_starts_its_own_tree_and_a_node_reached_again_is_shared(): void
    {
        static::assertSame(<<<'PLAN'
            #1 Read
            │  Extractor: ArrayExtractor
            └─ #3 CrossJoin
               │  Type: cross
               │  Changes columns known only at run time
               └─ #4 Result
                     Rows this plan hands out: to the trigger, or to the node reading it
            #2 Read
            │  Extractor: ArrayExtractor
            └─ #3 CrossJoin (shared)
            PLAN, (new FlowLayout())->render((new LogicalOutline())->of(new Result(
            new CrossJoin(NodeMother::read(), NodeMother::joinRight(NodeMother::plan(NodeMother::read()))),
        ))));
    }
}
