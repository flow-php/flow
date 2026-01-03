<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Execution;

use function Flow\ETL\DSL\{analyze, int_entry, row, rows, str_entry};
use Flow\Clock\FakeClock;
use Flow\ETL\Dataset\Report;
use Flow\ETL\Dataset\Statistics\{Columns, HighResolutionTime};
use Flow\ETL\Execution\ReportCollector;
use Flow\ETL\Tests\FlowTestCase;

final class ReportCollectorTest extends FlowTestCase
{
    public function test_capture_collects_column_statistics_when_enabled() : void
    {
        $collector = new ReportCollector(analyze()->withColumnStatistics());

        $collector->capture(rows(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
            row(int_entry('id', 2), str_entry('name', 'Bob')),
        ));

        $report = $collector->report();

        self::assertNotNull($report);
        self::assertInstanceOf(Columns::class, $report->statistics()->columns);
        self::assertSame(1, $report->statistics()->columns->get('id')->min());
        self::assertSame(2, $report->statistics()->columns->get('id')->max());
    }

    public function test_capture_collects_schema_when_enabled() : void
    {
        $collector = new ReportCollector(analyze()->withSchema());

        $collector->capture(rows(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
        ));

        $report = $collector->report();

        self::assertNotNull($report);
        self::assertNotNull($report->schema());
        self::assertSame(2, $report->schema()->count());
        self::assertNotNull($report->schema()->findDefinition('id'));
        self::assertNotNull($report->schema()->findDefinition('name'));
    }

    public function test_capture_increments_row_count_correctly() : void
    {
        $collector = new ReportCollector(true);

        $collector->capture(rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
        ));
        $collector->capture(rows(
            row(int_entry('id', 3)),
        ));

        $report = $collector->report();

        self::assertNotNull($report);
        self::assertSame(3, $report->statistics()->totalRows());
    }

    public function test_capture_is_noop_when_analyze_is_false() : void
    {
        $collector = new ReportCollector(false);

        $collector->capture(rows(
            row(int_entry('id', 1)),
        ));

        self::assertNull($collector->report());
    }

    public function test_column_statistics_is_null_when_not_enabled() : void
    {
        $collector = new ReportCollector(analyze());

        $collector->capture(rows(
            row(int_entry('id', 1)),
        ));

        $report = $collector->report();

        self::assertNotNull($report);
        self::assertNull($report->statistics()->columns);
    }

    public function test_report_returns_null_when_analyze_is_false() : void
    {
        $collector = new ReportCollector(false);

        self::assertNull($collector->report());
    }

    public function test_report_returns_null_when_analyze_is_null() : void
    {
        $collector = new ReportCollector(null);

        self::assertNull($collector->report());
    }

    public function test_report_returns_report_when_analyze_is_true() : void
    {
        $collector = new ReportCollector(true);

        self::assertInstanceOf(Report::class, $collector->report());
    }

    public function test_report_returns_report_when_using_analyze_instance() : void
    {
        $collector = new ReportCollector(analyze());

        self::assertInstanceOf(Report::class, $collector->report());
    }

    public function test_report_returns_report_with_correct_execution_time() : void
    {
        $clock = new FakeClock(new \DateTimeImmutable('2025-01-01 10:00:00 UTC'));
        $collector = new ReportCollector(true, $clock);

        $clock->modify('+5 minutes');
        $collector->capture(rows(row(int_entry('id', 1))));

        $clock->modify('+5 minutes');
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
        $collector = new ReportCollector(true);

        $collector->capture(rows(row(int_entry('id', 1))));

        $report = $collector->report();

        self::assertNotNull($report);
        self::assertInstanceOf(HighResolutionTime::class, $report->statistics()->executionTime->highResolutionTime);
    }

    public function test_report_returns_report_with_memory_consumption() : void
    {
        $collector = new ReportCollector(true);

        $collector->capture(rows(row(int_entry('id', 1))));

        $report = $collector->report();

        self::assertNotNull($report);
        self::assertGreaterThanOrEqual(0, $report->statistics()->memory->initial()->inBytes());
    }

    public function test_schema_is_null_when_not_enabled() : void
    {
        $collector = new ReportCollector(analyze());

        $collector->capture(rows(
            row(int_entry('id', 1)),
        ));

        $report = $collector->report();

        self::assertNotNull($report);
        self::assertNull($report->schema());
    }
}
