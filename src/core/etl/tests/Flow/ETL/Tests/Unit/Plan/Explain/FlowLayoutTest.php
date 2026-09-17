<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Explain\FlowLayout;
use Flow\ETL\Plan\Explain\Outline;
use Flow\ETL\Plan\Node\Collect;
use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class FlowLayoutTest extends FlowTestCase
{
    public function test_a_chain_is_printed_from_its_source_to_its_result(): void
    {
        $plan = new Result(NodeMother::limit(new Collect(NodeMother::read()->withLimit(5)), 5));

        static::assertSame(<<<'PLAN'
            #1 Read
            │  Extractor: ArrayExtractor
            │  Limit: 5
            └─ #2 Collect
               │  Buffers all rows before passing them on
               └─ #3 Limit
                  │  Limit: 5
                  └─ #4 Result
                        Rows fetch() returns and run() streams
            PLAN, (new FlowLayout())->render((new Outline())->of($plan)));
    }

    public function test_a_node_read_by_several_consumers_branches_and_outputs_is_left_out(): void
    {
        $filter = new Filter(NodeMother::read(), ref('id')->isNotNull());
        $plan = new Outputs(new Result($filter), new Sinks(new Write($filter, to_memory(new ArrayMemory()))));

        static::assertSame(<<<'PLAN'
            #1 Read
            │  Extractor: ArrayExtractor
            └─ #2 Filter
               │  Condition: IsNotNull
               ├─ #3 Result
               │     Rows fetch() returns and run() streams
               └─ #4 Write
                     Loader: MemoryLoader
            PLAN, (new FlowLayout())->render((new Outline())->of($plan)));
    }

    public function test_every_reader_of_a_node_hangs_under_it(): void
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
            #1 Read
            │  Extractor: ArrayExtractor
            ├─ #2 Result
            │     Rows fetch() returns and run() streams
            ├─ #3 Write
            │     Loader: MemoryLoader
            └─ #4 Select
               └─ #5 Write
                     Loader: MemoryLoader
            PLAN, (new FlowLayout())->render((new Outline())->of($plan)));
    }

    public function test_every_source_starts_its_own_tree_and_a_node_reached_again_is_shared(): void
    {
        $plan = new Result(new CrossJoin(NodeMother::read(), NodeMother::frame(NodeMother::plan(NodeMother::read()))));

        static::assertSame(<<<'PLAN'
            #1 Read
            │  Extractor: ArrayExtractor
            └─ #5 CrossJoin
               │  Defines columns known only at run time
               └─ #6 Result
                     Rows fetch() returns and run() streams
            #2 Read
            │  Extractor: ArrayExtractor
            └─ #3 Result
               │  Rows fetch() returns and run() streams
               └─ #4 SideInput
                  └─ #5 CrossJoin (shared)
            PLAN, (new FlowLayout())->render((new Outline())->of($plan)));
    }
}
