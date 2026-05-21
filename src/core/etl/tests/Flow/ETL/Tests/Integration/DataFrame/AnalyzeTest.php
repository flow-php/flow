<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateTimeImmutable;
use Flow\Clock\FakeClock;
use Flow\ETL\Dataset\Report;
use Flow\ETL\Dataset\Statistics\Columns;
use Flow\ETL\Dataset\Statistics\HighResolutionTime;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class AnalyzeTest extends FlowIntegrationTestCase
{
    public function test_analyzing_csv_file_with_auto_cast(): void
    {
        $config = config_builder()->clock($clock = new FakeClock())->build();

        $clock->set(new DateTimeImmutable('2025-01-01 00:00:00 UTC'));
        $report = df($config)
            ->read(from_array([
                [
                    'Index' => 1,
                    'Date' => '2024-01-19',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 2,
                    'Date' => '2024-01-20',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 3,
                    'Date' => '2024-01-21',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 4,
                    'Date' => '2024-01-22',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 5,
                    'Date' => '2024-01-23',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
            ]))
            ->autoCast()
            ->collect()
            ->run(static function (Rows $rows, FlowContext $context): void {
                $clock = $context->config->clock();

                if ($clock instanceof FakeClock) {
                    $clock->modify('+5 minutes');
                }
            }, analyze()->withSchema()->withColumnStatistics());

        static::assertNotNull($report);
        static::assertSame(5, $report->statistics()->totalRows());
        static::assertEquals(
            schema(
                int_schema('Index'),
                date_schema('Date'),
                float_schema('Close'),
                float_schema('Volume'),
                float_schema('Open'),
                float_schema('High'),
                float_schema('Low'),
            ),
            $report->schema(),
        );
        static::assertSame(7, $report->schema()?->count());
        static::assertGreaterThan(0, $report->statistics()->memory->max()->inBytes());
        static::assertInstanceOf(DateTimeImmutable::class, $report->statistics()->executionTime->startedAt);
        static::assertInstanceOf(DateTimeImmutable::class, $report->statistics()->executionTime->finishedAt);
        static::assertGreaterThanOrEqual(
            $report->statistics()->executionTime->startedAt,
            $report->statistics()->executionTime->finishedAt,
        );
        static::assertEquals(5 * 60, $report->statistics()->executionTime->inSeconds());
        static::assertInstanceOf(HighResolutionTime::class, $report->statistics()->executionTime->highResolutionTime);
        static::assertgreaterThan(0, $report->statistics()->executionTime->highResolutionTime->toSeconds());
        static::assertInstanceOf(Columns::class, $report->statistics()->columns);
    }

    public function test_analyzing_csv_file_with_limit(): void
    {
        $report = df()
            ->read(from_array([
                [
                    'Index' => '1',
                    'Date' => '2024-01-19',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => '2',
                    'Date' => '2024-01-20',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => '3',
                    'Date' => '2024-01-21',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => '4',
                    'Date' => '2024-01-22',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => '5',
                    'Date' => '2024-01-23',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
            ]))
            ->limit(2)
            ->run(analyze: analyze()->withSchema()->withColumnStatistics());

        static::assertNotNull($report);
        static::assertSame(2, $report->statistics()->totalRows());
        static::assertEquals(
            schema(
                str_schema('Index'),
                str_schema('Date'),
                str_schema('Close'),
                str_schema('Volume'),
                str_schema('Open'),
                str_schema('High'),
                str_schema('Low'),
            ),
            $report->schema(),
        );
        static::assertSame(7, $report->schema()?->count());
    }

    public function test_analyzing_csv_file_without_column_stats(): void
    {
        $config = config_builder()->clock($clock = new FakeClock())->build();

        $clock->set(new DateTimeImmutable('2025-01-01 00:00:00 UTC'));
        $report = df($config)
            ->read(from_array([
                [
                    'Index' => 1,
                    'Date' => '2024-01-19',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 2,
                    'Date' => '2024-01-20',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 3,
                    'Date' => '2024-01-21',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 4,
                    'Date' => '2024-01-22',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 5,
                    'Date' => '2024-01-23',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
            ]))
            ->autoCast()
            ->collect()
            ->run(static function (Rows $rows, FlowContext $context): void {
                $clock = $context->config->clock();

                if ($clock instanceof FakeClock) {
                    $clock->modify('+5 minutes');
                }
            }, analyze()->withSchema());

        static::assertNotNull($report);
        static::assertSame(5, $report->statistics()->totalRows());
        static::assertEquals(
            schema(
                int_schema('Index'),
                date_schema('Date'),
                float_schema('Close'),
                float_schema('Volume'),
                float_schema('Open'),
                float_schema('High'),
                float_schema('Low'),
            ),
            $report->schema(),
        );
        static::assertSame(7, $report->schema()?->count());
        static::assertNull($report->statistics()->columns);
    }

    public function test_analyzing_csv_file_without_schema(): void
    {
        $config = config_builder()->clock($clock = new FakeClock())->build();

        $clock->set(new DateTimeImmutable('2025-01-01 00:00:00 UTC'));
        $report = df($config)
            ->read(from_array([
                [
                    'Index' => 1,
                    'Date' => '2024-01-19',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 2,
                    'Date' => '2024-01-20',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 3,
                    'Date' => '2024-01-21',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 4,
                    'Date' => '2024-01-22',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
                [
                    'Index' => 5,
                    'Date' => '2024-01-23',
                    'Close' => '2029.3',
                    'Volume' => '166078.0',
                    'Open' => '2027.4',
                    'High' => '2041.9',
                    'Low' => '2022.2',
                ],
            ]))
            ->autoCast()
            ->collect()
            ->run(static function (Rows $rows, FlowContext $context): void {
                $clock = $context->config->clock();

                if ($clock instanceof FakeClock) {
                    $clock->modify('+5 minutes');
                }
            }, analyze());

        static::assertNotNull($report);
        static::assertSame(5, $report->statistics()->totalRows());
        static::assertNull($report->schema());
        static::assertNull($report->statistics()->columns);
    }

    public function test_analyzing_partitioned_datasets(): void
    {
        $report = df()->read(from_text(__DIR__
        . '/Fixtures/Partitioning/multi_partition_pruning_test/year=*/month=*/day=*/*.txt'))->run(
            analyze: analyze()->withSchema()->withColumnStatistics(),
        );

        static::assertNotNull($report);
        static::assertSame(7, $report->statistics()->totalRows());
        static::assertEquals(
            schema(str_schema('year'), str_schema('month'), str_schema('day'), str_schema('text')),
            $report->schema(),
        );
    }

    public function test_run_explicit_analyze_overrides_config(): void
    {
        $config = config_builder()->analyze(analyze())->build();

        $report = df($config)->read(from_array([['id' => 1]]))->run(analyze: analyze()->withSchema());

        static::assertNotNull($report);
        static::assertNotNull($report->schema());
    }

    public function test_run_uses_analyze_from_config(): void
    {
        $config = config_builder()->analyze(analyze())->build();

        $report = df($config)->read(from_array([['id' => 1]]))->run();

        // @mago-ignore analysis:impossible-type-comparison
        static::assertInstanceOf(Report::class, $report);
        // @mago-ignore analysis:mixed-method-access
        static::assertSame(1, $report->statistics()->totalRows());
    }
}
