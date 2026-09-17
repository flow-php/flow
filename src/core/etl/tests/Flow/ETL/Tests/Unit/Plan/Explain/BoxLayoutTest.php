<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Explain\BoxLayout;
use Flow\ETL\Plan\Explain\Outline;
use Flow\ETL\Plan\Node\Collect;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Rename;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class BoxLayoutTest extends FlowTestCase
{
    public function test_a_single_node_is_one_box(): void
    {
        $plan = NodeMother::read();

        static::assertSame(<<<'PLAN'
            ┌───────────────────────────┐
            │          #1 Read          │
            │   ────────────────────    │
            │ Extractor: ArrayExtractor │
            └───────────────────────────┘
            PLAN, (new BoxLayout())->render((new Outline())->of($plan)));
    }

    public function test_a_chain_stacks_boxes_joined_at_their_centers(): void
    {
        $plan = new Result(NodeMother::limit(new Collect(NodeMother::read()->withLimit(5)), 5));

        static::assertSame(<<<'PLAN'
            ┌───────────────────────────┐
            │         #4 Result         │
            │   ────────────────────    │
            │ Rows fetch() returns and  │
            │       run() streams       │
            └─────────────┬─────────────┘
            ┌─────────────┴─────────────┐
            │         #3 Limit          │
            │   ────────────────────    │
            │         Limit: 5          │
            └─────────────┬─────────────┘
            ┌─────────────┴─────────────┐
            │        #2 Collect         │
            │   ────────────────────    │
            │  Buffers all rows before  │
            │      passing them on      │
            └─────────────┬─────────────┘
            ┌─────────────┴─────────────┐
            │          #1 Read          │
            │   ────────────────────    │
            │ Extractor: ArrayExtractor │
            │         Limit: 5          │
            └───────────────────────────┘
            PLAN, (new BoxLayout())->render((new Outline())->of($plan)));
    }

    public function test_a_later_child_hangs_off_the_right_edge_and_a_shared_node_is_a_leaf(): void
    {
        $filter = new Filter(NodeMother::read(), ref('id')->isNotNull());
        $plan = new Outputs(new Result($filter), new Sinks(new Write($filter, to_memory(new ArrayMemory()))));

        static::assertSame(<<<'PLAN'
            ┌───────────────────────────┐
            │          Outputs          ├──────────────┐
            └─────────────┬─────────────┘              │
            ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
            │         #3 Result         ││         #4 Write          │
            │   ────────────────────    ││   ────────────────────    │
            │ Rows fetch() returns and  ││   Loader: MemoryLoader    │
            │       run() streams       ││                           │
            └─────────────┬─────────────┘└─────────────┬─────────────┘
            ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
            │         #2 Filter         ││         #2 Filter         │
            │   ────────────────────    ││         (shared)          │
            │   Condition: IsNotNull    ││                           │
            └─────────────┬─────────────┘└───────────────────────────┘
            ┌─────────────┴─────────────┐
            │          #1 Read          │
            │   ────────────────────    │
            │ Extractor: ArrayExtractor │
            └───────────────────────────┘
            PLAN, (new BoxLayout())->render((new Outline())->of($plan)));
    }

    public function test_a_middle_child_branches_off_the_same_line(): void
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
            ┌───────────────────────────┐
            │          Outputs          ├──────────────┬────────────────────────────┐
            └─────────────┬─────────────┘              │                            │
            ┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
            │         #2 Result         ││         #3 Write          ││         #5 Write          │
            │   ────────────────────    ││   ────────────────────    ││   ────────────────────    │
            │ Rows fetch() returns and  ││   Loader: MemoryLoader    ││   Loader: MemoryLoader    │
            │       run() streams       ││                           ││                           │
            └─────────────┬─────────────┘└─────────────┬─────────────┘└─────────────┬─────────────┘
            ┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
            │          #1 Read          ││          #1 Read          ││         #4 Select         │
            │   ────────────────────    ││         (shared)          ││                           │
            │ Extractor: ArrayExtractor ││                           ││                           │
            └───────────────────────────┘└───────────────────────────┘└─────────────┬─────────────┘
                                                                      ┌─────────────┴─────────────┐
                                                                      │          #1 Read          │
                                                                      │         (shared)          │
                                                                      └───────────────────────────┘
            PLAN, (new BoxLayout())->render((new Outline())->of($plan)));
    }

    public function test_long_text_wraps_and_a_word_longer_than_the_box_is_split(): void
    {
        $plan = new Rename(NodeMother::read(), 'id', 'identifier_of_the_customer_order');

        static::assertSame(<<<'PLAN'
            ┌───────────────────────────┐
            │         #2 Rename         │
            │   ────────────────────    │
            │       Rename: id →        │
            │ identifier_of_the_custome │
            │          r_order          │
            │     Defines columns:      │
            │ identifier_of_the_custome │
            │          r_order          │
            └─────────────┬─────────────┘
            ┌─────────────┴─────────────┐
            │          #1 Read          │
            │   ────────────────────    │
            │ Extractor: ArrayExtractor │
            └───────────────────────────┘
            PLAN, (new BoxLayout())->render((new Outline())->of($plan)));
    }

    public function test_span_counts_the_leaves_under_an_entry(): void
    {
        $read = NodeMother::read();
        $plan = new Outputs(
            new Result($read),
            new Sinks(new Write($read, to_memory(new ArrayMemory())), new Write($read, to_memory(new ArrayMemory()))),
        );

        static::assertSame(3, (new BoxLayout())->span((new Outline())->of($plan)));
        static::assertSame(1, (new BoxLayout())->span((new Outline())->of($read)));
    }

    public function test_wrap_keeps_short_text_on_one_line(): void
    {
        static::assertSame(['Limit: 5'], (new BoxLayout())->wrap('Limit: 5'));
    }
}
