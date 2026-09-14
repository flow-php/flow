<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner;

use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Exception\SideRootFailure;
use Flow\ETL\Plan\Node\Transform;
use Flow\ETL\Planner\Analysis;
use Flow\ETL\Planner\Lowerings;
use Flow\ETL\Planner\PipelineSplit;
use Flow\ETL\Planner\SideFeed;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\Double\UndescribableRowLessExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use RuntimeException;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class SideFeedTest extends FlowTestCase
{
    public function test_rows_fed_to_the_pipeline_reach_the_tail_loader(): void
    {
        $context = NodeMother::context();
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $select = NodeMother::select($read);
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        $analysis->of($select, $context);
        $spy = new SpyLoader();

        $feed = (new SideFeed($analysis, $context))->of([$select], [$spy], $read, $context->errorHandler(), 7);
        $feed->load(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])), $context);
        $feed->closure($context);

        static::assertSame([2], $spy->loadedRowCounts());
        static::assertSame(1, $spy->closureCount);
    }

    public function test_a_host_without_a_schema_feeds_an_empty_schema(): void
    {
        $context = NodeMother::context();
        $read = NodeMother::read(new UndescribableRowLessExtractor());
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        $analysis->of($read, $context);
        $spy = new SpyLoader();

        $feed = (new SideFeed($analysis, $context))->of([], [$spy], $read, $context->errorHandler(), 0);
        $feed->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);
        $feed->closure($context);

        static::assertNotNull($analysis->refusal());
        static::assertSame([1], $spy->loadedRowCounts());
    }

    public function test_a_throw_only_handler_surfaces_a_failing_step(): void
    {
        $context = NodeMother::context()->withErrorHandler(new IgnoreError());
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $transform = new Transform($read, new ThrowingTransformer(new RuntimeException('boom')));
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        $analysis->of($transform, $context);

        $feed = (new SideFeed($analysis, $context))->of([$transform], [new SpyLoader()], $read, new ThrowError(), 0);

        $this->expectException(SideRootFailure::class);

        $feed->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);
    }
}
