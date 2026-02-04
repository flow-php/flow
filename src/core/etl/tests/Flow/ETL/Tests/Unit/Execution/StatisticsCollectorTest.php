<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Execution;

use function Flow\ETL\DSL\{analyze, flow_context, int_entry, row, rows, str_entry};
use Flow\Clock\FakeClock;
use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\Dataset\Report;
use Flow\ETL\Dataset\Statistics\{Columns, HighResolutionTime};
use Flow\ETL\Execution\StatisticsCollector;
use Flow\ETL\Tests\FlowTestCase;

final class StatisticsCollectorTest extends FlowTestCase
{
    public function test_capture_collects_column_statistics_when_enabled() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(analyze()->withColumnStatistics(), $context);

        $collector->capture(rows(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
            row(int_entry('id', 2), str_entry('name', 'Bob')),
        ));

        $collector->end();
        $report = $collector->report();

        self::assertNotNull($report);
        self::assertInstanceOf(Columns::class, $report->statistics()->columns);
        self::assertSame(1, $report->statistics()->columns->get('id')->min());
        self::assertSame(2, $report->statistics()->columns->get('id')->max());
    }

    public function test_capture_collects_schema_when_enabled() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(analyze()->withSchema(), $context);

        $collector->capture(rows(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
        ));

        $collector->end();
        $report = $collector->report();

        self::assertNotNull($report);
        self::assertNotNull($report->schema());
        self::assertSame(2, $report->schema()->count());
        self::assertNotNull($report->schema()->findDefinition('id'));
        self::assertNotNull($report->schema()->findDefinition('name'));
    }

    public function test_capture_increments_row_count_correctly() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(true, $context);

        $collector->capture(rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
        ));
        $collector->capture(rows(
            row(int_entry('id', 3)),
        ));

        $collector->end();
        $report = $collector->report();

        self::assertNotNull($report);
        self::assertSame(3, $report->statistics()->totalRows());
    }

    public function test_capture_is_noop_when_analyze_is_false() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(false, $context);

        $collector->capture(rows(
            row(int_entry('id', 1)),
        ));

        $collector->end();
        self::assertNull($collector->report());
    }

    public function test_column_statistics_is_null_when_not_enabled() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(analyze(), $context);

        $collector->capture(rows(
            row(int_entry('id', 1)),
        ));

        $collector->end();
        $report = $collector->report();

        self::assertNotNull($report);
        self::assertNull($report->statistics()->columns);
    }

    public function test_report_returns_null_when_analyze_is_false() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(false, $context);
        $collector->end();

        self::assertNull($collector->report());
    }

    public function test_report_returns_null_when_analyze_is_null() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(null, $context);
        $collector->end();

        self::assertNull($collector->report());
    }

    public function test_report_returns_report_when_analyze_is_true() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(true, $context);
        $collector->end();

        self::assertInstanceOf(Report::class, $collector->report());
    }

    public function test_report_returns_report_when_using_analyze_instance() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(analyze(), $context);
        $collector->end();

        self::assertInstanceOf(Report::class, $collector->report());
    }

    public function test_report_returns_report_with_correct_execution_time() : void
    {
        $clock = new FakeClock(new \DateTimeImmutable('2025-01-01 10:00:00 UTC'));
        $config = (new ConfigBuilder())->clock($clock)->build();
        $context = flow_context($config);
        $collector = new StatisticsCollector(true, $context);

        $clock->modify('+5 minutes');
        $collector->capture(rows(row(int_entry('id', 1))));

        $clock->modify('+5 minutes');
        $collector->end();
        $report = $collector->report();

        self::assertNotNull($report);
        self::assertEquals(
            new \DateTimeImmutable('2025-01-01 10:00:00 UTC'),
            $report->statistics()->executionTime->startedAt
        );
        self::assertEquals(
            new \DateTimeImmutable('2025-01-01 10:10:00 UTC'),
            $report->statistics()->executionTime->finishedAt
        );
        self::assertSame(600, $report->statistics()->executionTime->inSeconds());
    }

    public function test_report_returns_report_with_high_resolution_time() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(true, $context);

        $collector->capture(rows(row(int_entry('id', 1))));

        $collector->end();
        $report = $collector->report();

        self::assertNotNull($report);
        self::assertInstanceOf(HighResolutionTime::class, $report->statistics()->executionTime->highResolutionTime);
    }

    public function test_report_returns_report_with_memory_consumption() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(true, $context);

        $collector->capture(rows(row(int_entry('id', 1))));

        $collector->end();
        $report = $collector->report();

        self::assertNotNull($report);
        self::assertGreaterThanOrEqual(0, $report->statistics()->memory->initial()->inBytes());
    }

    public function test_schema_is_null_when_not_enabled() : void
    {
        $context = flow_context();
        $collector = new StatisticsCollector(analyze(), $context);

        $collector->capture(rows(
            row(int_entry('id', 1)),
        ));

        $collector->end();
        $report = $collector->report();

        self::assertNotNull($report);
        self::assertNull($report->schema());
    }
}
