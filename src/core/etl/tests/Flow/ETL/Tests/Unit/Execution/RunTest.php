<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Execution;

use ArrayObject;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Execution\Run;
use Flow\ETL\Executor;
use Flow\ETL\Plan\Node\Transform;
use Flow\ETL\Planner;
use Flow\ETL\Planner\Lowerings;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\RecordingRule;
use Flow\ETL\Tests\Double\RunDrainingTransformer;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\RunMother;
use RuntimeException;

use function array_map;
use function array_sum;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class RunTest extends FlowTestCase
{
    public function test_run_yields_the_batches_of_the_planned_pipeline(): void
    {
        $batches = iterator_to_array(RunMother::default()->of(
            NodeMother::plan(NodeMother::limit(NodeMother::read(from_array([['id' => 1], ['id' => 2]])), 1)),
            NodeMother::context(),
        ));

        static::assertCount(1, $batches);
        static::assertSame([['id' => 1]], $batches[0]->toArray());
    }

    public function test_a_second_run_of_the_same_logical_plan_gets_fresh_steps(): void
    {
        $run = RunMother::default();
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::read(from_array([['id' => 1], ['id' => 2]])), 1));
        $context = NodeMother::context();

        iterator_to_array($run->of($plan, $context));

        static::assertSame([['id' => 1]], iterator_to_array($run->of($plan, $context))[0]->toArray());
    }

    public function test_every_run_plans_its_own(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();
        $run = new Run(new Planner(Lowerings::default(), new RecordingRule('plan', $log)), new Executor());
        $plan = NodeMother::plan(NodeMother::read());

        iterator_to_array($run->of($plan, NodeMother::context()));
        iterator_to_array($run->of($plan, NodeMother::context()));

        static::assertCount(2, $log);
    }

    public function test_an_abandoned_run_leaves_no_consumed_step_for_the_next_run(): void
    {
        $run = RunMother::default();
        $plan = NodeMother::plan(NodeMother::limit(
            NodeMother::read(from_rows(
                rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
                rows(schema(int_schema('id')), row(['id' => 3]), row(['id' => 4])),
            )),
            3,
        ));
        $context = NodeMother::context();

        // the reference keeps the generator parked, so its steps stay consumed
        $parked = $run->of($plan, $context);
        $parked->current();

        static::assertSame(
            3,
            array_sum(array_map(
                static fn(Rows $rows): int => $rows->count(),
                iterator_to_array($run->of($plan, $context), false),
            )),
        );
    }

    public function test_a_re_entrant_run_throws_cyclic_plan_on_run(): void
    {
        $run = RunMother::default();
        $context = NodeMother::context();
        $read = NodeMother::read();

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('Cannot run this plan: it reads from a DataFrame that reads back from it.');

        iterator_to_array($run->of(
            NodeMother::plan(new Transform($read, new RunDrainingTransformer($run, NodeMother::plan($read), $context))),
            $context,
        ));
    }

    public function test_the_run_guard_is_disarmed_across_the_yield(): void
    {
        $run = RunMother::default();
        $plan = NodeMother::plan(NodeMother::read(from_array([['id' => 1], ['id' => 2], ['id' => 3]])));
        $context = NodeMother::context();

        // the reference keeps the generator parked across the read below - inlining it lets PHP
        // destroy the generator, which clears the flag and makes this test vacuous
        $parked = $run->of($plan, $context);
        $parked->current();

        static::assertCount(1, iterator_to_array($run->of($plan, $context)));
    }

    public function test_in_builds_a_run_over_the_configs_planner(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();
        $run = Run::in(
            config_builder()->planner(new Planner(Lowerings::default(), new RecordingRule('plan', $log)))->build(),
        );

        $batches = iterator_to_array($run->of(NodeMother::plan(NodeMother::read()), NodeMother::context()));

        static::assertSame([['id' => 1]], $batches[0]->toArray());
        static::assertSame(['plan'], $log->getArrayCopy());
    }

    public function test_a_drained_run_reports_one_balanced_dataframe_span(): void
    {
        $telemetry = new MemoryTelemetryContext();

        iterator_to_array(RunMother::default()->of(NodeMother::plan(NodeMother::read()), $telemetry->flowContext));

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertNotTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
    }

    public function test_an_executor_failure_reports_the_span_as_failed_and_rethrows(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $failure = new RuntimeException('source exploded');

        try {
            iterator_to_array(RunMother::default()->of(
                NodeMother::plan(new Transform(NodeMother::read(), new ThrowingTransformer($failure))),
                $telemetry->flowContext,
            ));

            static::fail('Expected the executor failure to be rethrown.');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
    }

    public function test_an_abandoned_run_closes_its_span(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $generator = RunMother::default()->of(
            NodeMother::plan(NodeMother::read(from_rows(
                rows(schema(int_schema('id')), row(['id' => 1])),
                rows(schema(int_schema('id')), row(['id' => 2])),
            ))),
            $telemetry->flowContext,
        );

        $generator->current();
        unset($generator);

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertNotTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
    }

    public function test_a_planning_failure_reports_a_started_and_a_failed_span_and_rethrows(): void
    {
        $telemetry = new MemoryTelemetryContext();

        try {
            RunMother::default()->plan(
                NodeMother::plan(NodeMother::select(NodeMother::read(), 'nope')),
                $telemetry->flowContext,
            );

            static::fail('Expected the bind failure to be rethrown.');
        } catch (SchemaDefinitionNotFoundException $e) {
            static::assertSame('Schema definition for entry "nope" not found.', $e->getMessage());
        }

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
    }

    public function test_a_side_input_run_during_an_armed_outer_run_does_not_trip_the_guard(): void
    {
        $side = new RunDrainingTransformer(
            RunMother::default(),
            NodeMother::plan(NodeMother::read()),
            NodeMother::context(),
        );

        iterator_to_array(RunMother::default()->of(
            NodeMother::plan(new Transform(NodeMother::read(), $side)),
            NodeMother::context(),
        ));

        static::assertCount(1, $side->drained);
        static::assertSame([['id' => 1]], $side->drained[0]->toArray());
    }
}
