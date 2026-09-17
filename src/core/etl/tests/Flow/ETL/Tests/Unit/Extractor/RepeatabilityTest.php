<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\Repeatability;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Tests\Double\EmptyExtractor;
use Flow\ETL\Tests\Double\RepeatableExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\to_memory;

final class RepeatabilityTest extends FlowTestCase
{
    public function test_an_extractor_that_does_not_declare_the_capability_is_refused(): void
    {
        static::assertFalse((new Repeatability())->of(new EmptyExtractor()));
    }

    public function test_a_wrapper_is_refused_when_a_nested_extractor_is(): void
    {
        static::assertFalse((new Repeatability())->of(
            new RepeatableExtractor(true, new RepeatableExtractor(true, new RepeatableExtractor(false))),
        ));
    }

    public function test_a_wrapper_is_refused_when_its_inner_extractor_is(): void
    {
        static::assertFalse((new Repeatability())->of(new RepeatableExtractor(true, new EmptyExtractor())));
    }

    public function test_a_wrapper_whose_children_all_repeat_repeats(): void
    {
        static::assertTrue((new Repeatability())->of(
            new RepeatableExtractor(true, new RepeatableExtractor(true), new RepeatableExtractor(true)),
        ));
    }

    public function test_it_answers_the_extractors_own_verdict(): void
    {
        static::assertTrue((new Repeatability())->of(new RepeatableExtractor(true)));
        static::assertFalse((new Repeatability())->of(new RepeatableExtractor(false)));
    }

    public function test_a_plan_whose_every_source_repeats_repeats(): void
    {
        static::assertTrue((new Repeatability())->ofPlan(NodeMother::plan(NodeMother::read())));
    }

    public function test_a_plan_is_refused_when_a_join_side_input_cannot_repeat(): void
    {
        static::assertFalse((new Repeatability())->ofPlan(NodeMother::plan(NodeMother::join(
            NodeMother::read(),
            NodeMother::frame(NodeMother::plan(NodeMother::nonRepeatableRead())),
        ))));
    }

    public function test_a_plan_is_refused_when_a_cross_join_side_input_cannot_repeat(): void
    {
        static::assertFalse((new Repeatability())->ofPlan(NodeMother::plan(NodeMother::crossJoin(
            NodeMother::read(),
            NodeMother::frame(NodeMother::plan(NodeMother::nonRepeatableRead())),
        ))));
    }

    public function test_a_plan_whose_join_side_input_repeats_repeats(): void
    {
        static::assertTrue((new Repeatability())->ofPlan(NodeMother::plan(NodeMother::join(
            NodeMother::read(),
            NodeMother::frame(NodeMother::plan(NodeMother::read())),
        ))));
    }

    public function test_a_plan_whose_consumers_share_a_source_repeats(): void
    {
        $read = NodeMother::read();

        static::assertTrue((new Repeatability())->ofPlan(LogicalPlan::of(
            $read,
            new Sinks(new Write($read, to_memory(new ArrayMemory()))),
        )));
    }

    public function test_a_plan_is_refused_when_a_sink_reads_a_non_repeatable_side_input(): void
    {
        static::assertFalse((new Repeatability())->ofPlan(new LogicalPlan(
            new Outputs(
                new Result(NodeMother::read()),
                new Sinks(
                    new Write(
                        NodeMother::join(
                            NodeMother::read(),
                            NodeMother::frame(NodeMother::plan(NodeMother::nonRepeatableRead())),
                        ),
                        to_memory(new ArrayMemory()),
                    ),
                ),
            ),
        )));
    }

    public function test_a_frame_repeats_when_every_source_it_reads_repeats(): void
    {
        static::assertTrue((new Repeatability())->of(from_data_frame(df()->read(from_array([['id' => 1]])))));
    }

    public function test_a_frame_over_a_source_that_cannot_repeat_is_refused(): void
    {
        static::assertFalse((new Repeatability())->of(from_data_frame(df()->read(new RepeatableExtractor(false)))));
    }

    public function test_a_frame_joining_a_frame_that_cannot_repeat_is_refused(): void
    {
        static::assertFalse((new Repeatability())->of(from_data_frame(
            df()
                ->read(from_array([['id' => 1]]))
                ->join(df()->read(new RepeatableExtractor(false)), join_on(['id' => 'id'], 'r_')),
        )));
    }
}
