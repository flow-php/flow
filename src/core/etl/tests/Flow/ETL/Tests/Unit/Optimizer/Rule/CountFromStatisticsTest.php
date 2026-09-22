<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Optimizer\Rule;

use Flow\ETL\Cardinality;
use Flow\ETL\DataFrame;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Optimizer\Rule\CountFromStatistics;
use Flow\ETL\Plan\Trigger;
use Flow\ETL\Tests\Double\DeclaringExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_memory;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;

final class CountFromStatisticsTest extends FlowTestCase
{
    public function test_a_count_over_a_plain_array_read_reads_the_number_instead(): void
    {
        $plan = (new CountFromStatistics())->apply(
            df()->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))->explain(Trigger::count)->logical,
            NodeMother::context(),
        );

        static::assertEquals(
            from_rows(rows(schema(int_schema('count')), row(['count' => 3]))),
            $plan->source()->extractor(),
        );
    }

    public function test_a_count_over_a_plain_sequence_read_reads_the_number_instead(): void
    {
        $plan = (new CountFromStatistics())->apply(
            df()->read(from_sequence_number('id', 1, 10))->explain(Trigger::count)->logical,
            NodeMother::context(),
        );

        static::assertEquals(
            from_rows(rows(schema(int_schema('count')), row(['count' => 10]))),
            $plan->source()->extractor(),
        );
    }

    public function test_a_count_over_a_plain_parquet_read_reads_the_number_instead(): void
    {
        $memory = memory_filesystem();
        df()
            ->read(from_sequence_number('id', 1, 25))
            ->write(to_parquet(path('memory://count/file.parquet'), filesystem: $memory))
            ->run();

        $plan = (new CountFromStatistics())->apply(
            df()
                ->read(from_parquet(path('memory://count/file.parquet'), filesystem: $memory))
                ->explain(Trigger::count)
                ->logical,
            NodeMother::context(),
        );

        static::assertEquals(
            from_rows(rows(schema(int_schema('count')), row(['count' => 25]))),
            $plan->source()->extractor(),
        );
    }

    public function test_a_count_over_a_plain_floe_read_reads_the_number_instead(): void
    {
        $memory = memory_filesystem();
        df()
            ->read(from_sequence_number('id', 1, 25))
            ->write(to_floe(path('memory://count/file.floe'), filesystem: $memory))
            ->run();

        $plan = (new CountFromStatistics())->apply(
            df()
                ->read(from_floe(path('memory://count/file.floe'), filesystem: $memory))
                ->explain(Trigger::count)
                ->logical,
            NodeMother::context(),
        );

        static::assertEquals(
            from_rows(rows(schema(int_schema('count')), row(['count' => 25]))),
            $plan->source()->extractor(),
        );
    }

    public static function inexact_rows(): Generator
    {
        yield 'approximate' => [Cardinality::approximately(10)];
        yield 'estimate equal to its bound, with an error' => [new Cardinality(10, 10)];
        yield 'upper bound alone' => [Cardinality::atMost(10)];
        yield 'unknown' => [Cardinality::unknown()];
    }

    #[DataProvider('inexact_rows')]
    public function test_a_source_that_does_not_know_its_rows_exactly_is_counted_by_running(Cardinality $rows): void
    {
        $plan = Trigger::count->plan(NodeMother::read(new DeclaringExtractor(new Statistics($rows))));

        static::assertSame($plan, (new CountFromStatistics())->apply($plan, NodeMother::context()));
    }

    public function test_a_read_with_a_pushed_limit_is_counted_by_running(): void
    {
        $plan = Trigger::count->plan(NodeMother::read()->withLimit(1));

        static::assertSame($plan, (new CountFromStatistics())->apply($plan, NodeMother::context()));
    }

    public function test_a_read_with_a_pushed_partition_filter_is_counted_by_running(): void
    {
        $plan = Trigger::count->plan(NodeMother::read()->withPathFilter(new RejectingFilter()));

        static::assertSame($plan, (new CountFromStatistics())->apply($plan, NodeMother::context()));
    }

    public static function frames_counted_by_running(): Generator
    {
        yield 'filter' => [df()->read(from_array([['id' => 1], ['id' => 2]]))->filter(ref('id')->equals(lit(1)))];
        yield 'limit' => [df()->read(from_array([['id' => 1], ['id' => 2]]))->limit(1)];
        yield 'join' => [df()
            ->read(from_array([['id' => 1]]))
            ->join(df()->read(from_array([['id' => 1]])), join_on(['id' => 'id']))];
        yield 'a transform that changes the rows' => [df()
            ->read(from_array([['id' => 1], ['id' => 1]]))
            ->dropDuplicates('id')];
        yield 'a transform that keeps the rows' => [df()
            ->read(from_array([['id' => 1]]))
            ->withEntry('copy', ref('id'))];
        yield 'a sink' => [df()->read(from_array([['id' => 1]]))->write(to_memory(new ArrayMemory()))];
    }

    #[DataProvider('frames_counted_by_running')]
    public function test_a_frame_that_does_more_than_read_is_counted_by_running(DataFrame $frame): void
    {
        $plan = $frame->explain(Trigger::count)->logical;

        static::assertSame($plan, (new CountFromStatistics())->apply($plan, NodeMother::context()));
    }

    public function test_a_plan_that_does_not_count_is_left_alone(): void
    {
        $plan = df()->read(from_array([['id' => 1]]))->explain(Trigger::rows)->logical;

        static::assertSame($plan, (new CountFromStatistics())->apply($plan, NodeMother::context()));
    }
}
