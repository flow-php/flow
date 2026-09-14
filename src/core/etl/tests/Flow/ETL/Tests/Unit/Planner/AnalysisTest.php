<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner;

use Flow\ETL\Exception\DataDependentSchemaException;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Plan\Described;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Plan\Node\JoinEach;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Planner\Analysis;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Planner\Lowering\ReadLowering;
use Flow\ETL\Planner\Lowering\ResultLowering;
use Flow\ETL\Planner\Lowering\SelectLowering;
use Flow\ETL\Planner\Lowerings;
use Flow\ETL\Planner\PipelineSplit;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\EmptyLowering;
use Flow\ETL\Tests\Double\StaticDataFrameFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function array_map;
use function count;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;

final class AnalysisTest extends FlowTestCase
{
    public function test_a_nodes_schema_is_the_fold_of_its_bound_steps(): void
    {
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        $read = new Read(new CountingExtractor(schema(int_schema('id'), str_schema('name'))));

        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            $analysis->of($read, NodeMother::context())->schema,
        );
        static::assertEquals(
            schema(str_schema('name')),
            $analysis->of(
                NodeMother::select(NodeMother::select($read, 'id', 'name'), 'name'),
                NodeMother::context(),
            )->schema,
        );
    }

    public function test_a_loader_passes_through_the_fold_unbound(): void
    {
        $output = [];
        $loader = to_array($output);
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());

        $analyzed = $analysis->of(
            new Write(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))), $loader),
            NodeMother::context(),
        );

        static::assertSame([$loader], $analyzed->steps);
        static::assertSame([$loader], $analyzed->bound);
        static::assertEquals(schema(int_schema('id')), $analyzed->schema);
    }

    public function test_bound_steps_are_the_result_of_bind_not_the_lowered_instances(): void
    {
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());

        $analyzed = $analysis->of(
            new Node\Collect(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
            NodeMother::context(),
        );

        static::assertInstanceOf(CollectingProcessor::class, $analyzed->steps[0]);
        static::assertInstanceOf(CollectingProcessor::class, $analyzed->bound[0]);
        static::assertNotSame($analyzed->steps[0], $analyzed->bound[0]);
    }

    public function test_the_same_node_object_is_analysed_once(): void
    {
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        $node = NodeMother::select(NodeMother::read());

        static::assertSame($analysis->of($node, NodeMother::context()), $analysis->of($node, NodeMother::context()));
    }

    public function test_every_child_is_analysed_including_a_side_input(): void
    {
        $analysis = new Analysis(
            new Lowerings(
                new ReadLowering(),
                new ResultLowering(),
                new SelectLowering(),
                new EmptyLowering(CrossJoin::class),
            ),
            new PipelineSplit(),
        );
        $read = NodeMother::read();
        $frameRoot = NodeMother::select(NodeMother::read());
        $frame = NodeMother::frame(NodeMother::plan($frameRoot));

        $analysis->of(new CrossJoin($read, $frame), NodeMother::context());

        static::assertNotNull($analysis->analyzed($read));
        static::assertNotNull($analysis->analyzed($frame));
        static::assertNotNull($analysis->analyzed($frameRoot));
        static::assertNotNull($analysis->analyzed($frameRoot->children()[0]));
    }

    public function test_a_frame_is_analysed_with_the_childs_context(): void
    {
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        $childContext = NodeMother::context(config());
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::read()), $childContext);

        $nested = $analysis->of($frame, NodeMother::context(config()))->nestedOrFail();

        static::assertSame($childContext, $nested->root()->context());
    }

    public function test_a_frame_carries_its_own_plan_as_the_nested_plan(): void
    {
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::select(NodeMother::read(from_array([[
            'id' => 1,
        ]], schema(int_schema('id')))))));

        $nested = $analysis->of($frame, NodeMother::context())->nestedOrFail();

        static::assertSame(
            [SelectEntriesTransformer::class],
            array_map(static fn($step) => $step::class, $nested->root()->segments()->steps()),
        );
        static::assertInstanceOf(Described::class, $nested);
        static::assertEquals(schema(int_schema('id')), $nested->schema);
        static::assertEquals(schema(int_schema('id')), $analysis->analyzed($frame)?->schema);
    }

    public function test_a_refusal_anywhere_makes_the_whole_plan_raw(): void
    {
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        $joinEach = new JoinEach(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            new StaticDataFrameFactory(df()->read(from_array([['id' => 1]], schema(int_schema('id'))))),
            join_on(['id' => 'id']),
            JoinType::inner,
        );
        $above = NodeMother::select($joinEach);

        $analysis->of($above, NodeMother::context());

        $refusal = $analysis->refusal();

        static::assertNotNull($refusal);
        static::assertSame(DataDependentSchemaException::class, $refusal->exception::class);
        static::assertStringContainsString('JoinEachRowsTransformer', $refusal->exception->getMessage());
        static::assertStringContainsString(
            "its right side is built from each left batch's row values",
            $refusal->exception->getMessage(),
        );
        static::assertNull($analysis->analyzed($joinEach)?->schema);
        static::assertSame($analysis->analyzed($joinEach)?->steps, $analysis->analyzed($joinEach)?->bound);
        $aboveAnalyzed = $analysis->analyzed($above);

        static::assertNotNull($aboveAnalyzed);
        static::assertNull($aboveAnalyzed->schema);
        static::assertCount(1, $aboveAnalyzed->steps);
    }

    public function test_the_first_refusal_is_kept(): void
    {
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        $factory = new StaticDataFrameFactory(df()->read(from_array([['id' => 1]], schema(int_schema('id')))));
        $first = new JoinEach(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            $factory,
            join_on(['id' => 'id']),
            JoinType::inner,
        );

        $analysis->of(new JoinEach($first, $factory, join_on(['id' => 'id']), JoinType::left), NodeMother::context());

        $refusal = $analysis->refusal();

        static::assertNotNull($refusal);
        static::assertSame(DataDependentSchemaException::class, $refusal->exception::class);
        static::assertStringContainsString('JoinEachRowsTransformer', $refusal->exception->getMessage());
        static::assertSame($refusal, $analysis->refusal());
    }

    public function test_analyzed_returns_null_for_a_node_that_was_never_walked(): void
    {
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        $analysis->of(NodeMother::read(), NodeMother::context());

        static::assertNull($analysis->analyzed(NodeMother::read()));
    }

    public function test_analyzed_with_a_null_schema_is_distinct_from_a_node_never_walked(): void
    {
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        $refusing = new JoinEach(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            new StaticDataFrameFactory(df()->read(from_array([['id' => 1]], schema(int_schema('id'))))),
            join_on(['id' => 'id']),
            JoinType::inner,
        );
        $analysis->of($refusing, NodeMother::context());

        static::assertNotNull($analysis->analyzed($refusing));
        static::assertNull($analysis->analyzed($refusing)?->schema);
        static::assertNull($analysis->analyzed(NodeMother::read()));
    }

    public function test_the_analysis_reads_no_row(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('id')), rows(schema(int_schema('id')), row(['id' => 1])));
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());

        $analysis->of(NodeMother::select(new Read($extractor)), NodeMother::context());

        static::assertSame(0, $extractor->extractCalls);
    }

    public function test_a_frame_child_beyond_the_first_is_handed_to_the_lowering_as_a_frame_output(): void
    {
        $seen = [];
        $lowering = new /** @implements Lowering<CrossJoin> */ class($seen) implements Lowering {
            /**
             * @param list<int> $seen
             */
            public function __construct(
                private array &$seen,
            ) {}

            public function handles(): string
            {
                return CrossJoin::class;
            }

            public function steps(Node $node, FlowContext $context, array $frames): array
            {
                $this->seen[] = count($frames);

                return [];
            }
        };
        $analysis = new Analysis(
            new Lowerings(new ReadLowering(), new ResultLowering(), $lowering),
            new PipelineSplit(),
        );
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::read(from_array([[
            'id' => 1,
        ]], schema(int_schema('id'))))));

        $analysis->of(
            new CrossJoin(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))), $frame),
            NodeMother::context(),
        );

        static::assertSame([1], $seen);
    }
}
