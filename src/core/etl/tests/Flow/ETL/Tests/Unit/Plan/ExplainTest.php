<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Explain;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Offset;
use Flow\ETL\Plan\Node\Rename;
use Flow\ETL\Plan\Node\RenameEach;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\SinkMultiple;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Until;
use Flow\ETL\Plan\Node\WithColumn;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rename_replace;
use function Flow\ETL\DSL\to_memory;

final class ExplainTest extends FlowTestCase
{
    public function test_a_single_chain_prints_one_line_per_node_with_its_declarations(): void
    {
        $plan = new LogicalPlan(new Result(NodeMother::limit(NodeMother::select(NodeMother::read()), 5)));

        static::assertSame(<<<'PLAN'
            #1 Result  preserving · transparent · streaming
              #2 Limit(5)  reducing · transparent · streaming
                #3 Select  preserving · transparent · streaming
                  #4 Read(ArrayExtractor, scan: limit=∅ files=OnlyFiles)  source · transparent · streaming
            PLAN, (new Explain())->of($plan));
    }

    public function test_a_subtree_two_consumers_share_is_printed_once(): void
    {
        $filter = new Filter(NodeMother::read(), ref('id')->isNotNull());
        $plan = new LogicalPlan(
            new SinkMultiple(new Result($filter), new Write($filter, to_memory(new ArrayMemory()))),
        );

        static::assertSame(<<<'PLAN'
            #1 SinkMultiple  preserving · opaque · streaming
              #2 Result  preserving · transparent · streaming
                #3 Filter(IsNotNull)  reducing · transparent · streaming
                  #4 Read(ArrayExtractor, scan: limit=∅ files=OnlyFiles)  source · transparent · streaming
              #5 Write(MemoryLoader)  preserving · opaque · streaming
                #3 (shared)
            PLAN, (new Explain())->of($plan));
    }

    public function test_payloads_and_redefinitions(): void
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
            #1 Result  preserving · transparent · streaming
              #2 Until(Literal)  reducing · transparent · streaming
                #3 Offset(2)  reducing · transparent · streaming
                  #4 Rename(n → m)  preserving · transparent · streaming · redefines m
                    #5 WithColumn(n = Literal)  preserving · transparent · streaming · redefines n
                      #6 RenameEach  preserving · transparent · streaming · redefines unknown
                        #7 Read(ArrayExtractor, scan: limit=∅ files=OnlyFiles)  source · transparent · streaming
            PLAN, (new Explain())->of($plan));
    }

    public function test_a_frame_is_a_leaf_with_its_own_plan_hidden(): void
    {
        $plan = new LogicalPlan(new Result(
            new CrossJoin(NodeMother::read(), NodeMother::frame(NodeMother::plan(NodeMother::read()))),
        ));

        static::assertSame(<<<'PLAN'
            #1 Result  preserving · transparent · streaming
              #2 CrossJoin  expanding · opaque · streaming · redefines unknown
                #3 Read(ArrayExtractor, scan: limit=∅ files=OnlyFiles)  source · transparent · streaming
                #4 Frame  preserving · opaque · streaming
            PLAN, (new Explain())->of($plan));
    }

    public function test_a_pushed_scan_shows_on_the_read_line(): void
    {
        $plan = new LogicalPlan(new Result(NodeMother::scannableRead()->withLimit(3)));

        static::assertStringContainsString('scan: limit=3 files=OnlyFiles', (new Explain())->of($plan));
    }

    public function test_a_transaction_root_child_lists_its_writes(): void
    {
        $read = NodeMother::read();
        $plan = new LogicalPlan(
            new SinkMultiple(
                new Result($read),
                new Transaction(
                    new RecordingTransaction(),
                    new Write($read, to_memory(new ArrayMemory())),
                    new Write(NodeMother::select($read), to_memory(new ArrayMemory())),
                ),
            ),
        );

        static::assertSame(<<<'PLAN'
            #1 SinkMultiple  preserving · opaque · streaming
              #2 Result  preserving · transparent · streaming
                #3 Read(ArrayExtractor, scan: limit=∅ files=OnlyFiles)  source · transparent · streaming
              #4 Transaction  preserving · opaque · streaming
                #5 Write(MemoryLoader)  preserving · opaque · streaming
                  #3 (shared)
                #6 Write(MemoryLoader)  preserving · opaque · streaming
                  #7 Select  preserving · transparent · streaming
                    #3 (shared)
            PLAN, (new Explain())->of($plan));
    }
}
