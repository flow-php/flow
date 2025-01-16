<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\{
    config_builder,
    datetime_schema,
    df,
    float_schema,
    from_array,
    int_schema,
    schema,
    str_schema};
use Flow\Clock\FakeClock;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\{FlowContext, Rows};

final class AnalyzeTest extends FlowIntegrationTestCase
{
    public function test_analyzing_csv_file_with_auto_cast() : void
    {
        $config = config_builder()->clock($clock = new FakeClock())->build();

        $clock->set(new \DateTimeImmutable('2025-01-01 00:00:00 UTC'));
        $report = df($config)
            ->read(from_array([
                ['Index' => 1, 'Date' => '2024-01-19', 'Close' => '2029.3', 'Volume' => '166078.0', 'Open' => '2027.4', 'High' => '2041.9', 'Low' => '2022.2'],
                ['Index' => 2, 'Date' => '2024-01-20', 'Close' => '2029.3', 'Volume' => '166078.0', 'Open' => '2027.4', 'High' => '2041.9', 'Low' => '2022.2'],
                ['Index' => 3, 'Date' => '2024-01-21', 'Close' => '2029.3', 'Volume' => '166078.0', 'Open' => '2027.4', 'High' => '2041.9', 'Low' => '2022.2'],
                ['Index' => 4, 'Date' => '2024-01-22', 'Close' => '2029.3', 'Volume' => '166078.0', 'Open' => '2027.4', 'High' => '2041.9', 'Low' => '2022.2'],
                ['Index' => 5, 'Date' => '2024-01-23', 'Close' => '2029.3', 'Volume' => '166078.0', 'Open' => '2027.4', 'High' => '2041.9', 'Low' => '2022.2'],
            ]))
            ->autoCast()
            ->collect()
            ->run(function (Rows $rows, FlowContext $context) : void {
                $clock = $context->config->clock();

                if ($clock instanceof FakeClock) {
                    $clock->modify('+5 minutes');
                }

            }, analyze: true);

        self::assertNotNull($report);
        self::assertSame(5, $report->statistics()->totalRows());
        self::assertEquals(
            schema(
                int_schema('Index'),
                datetime_schema('Date'),
                float_schema('Close'),
                float_schema('Volume'),
                float_schema('Open'),
                float_schema('High'),
                float_schema('Low'),
            ),
            $report->schema()
        );
        self::assertSame(7, $report->schema()->count());
        self::assertInstanceOf(\DateTimeImmutable::class, $report->statistics()->executionTime->startedAt);
        self::assertInstanceOf(\DateTimeImmutable::class, $report->statistics()->executionTime->finishedAt);
        self::assertGreaterThanOrEqual($report->statistics()->executionTime->startedAt, $report->statistics()->executionTime->finishedAt);
        self::assertEquals(5 * 60, $report->statistics()->executionTime->inSeconds());
    }

    public function test_analyzing_csv_file_with_limit() : void
    {
        $report = df()
            ->read(from_array([
                ['Index' => '1', 'Date' => '2024-01-19', 'Close' => '2029.3', 'Volume' => '166078.0', 'Open' => '2027.4', 'High' => '2041.9', 'Low' => '2022.2'],
                ['Index' => '2', 'Date' => '2024-01-20', 'Close' => '2029.3', 'Volume' => '166078.0', 'Open' => '2027.4', 'High' => '2041.9', 'Low' => '2022.2'],
                ['Index' => '3', 'Date' => '2024-01-21', 'Close' => '2029.3', 'Volume' => '166078.0', 'Open' => '2027.4', 'High' => '2041.9', 'Low' => '2022.2'],
                ['Index' => '4', 'Date' => '2024-01-22', 'Close' => '2029.3', 'Volume' => '166078.0', 'Open' => '2027.4', 'High' => '2041.9', 'Low' => '2022.2'],
                ['Index' => '5', 'Date' => '2024-01-23', 'Close' => '2029.3', 'Volume' => '166078.0', 'Open' => '2027.4', 'High' => '2041.9', 'Low' => '2022.2'],
            ]))
            ->limit(2)
            ->run(analyze: true);

        self::assertNotNull($report);
        self::assertSame(2, $report->statistics()->totalRows());
        self::assertEquals(
            schema(
                str_schema('Index'),
                str_schema('Date'),
                str_schema('Close'),
                str_schema('Volume'),
                str_schema('Open'),
                str_schema('High'),
                str_schema('Low'),
            ),
            $report->schema()
        );
        self::assertSame(7, $report->schema()->count());
    }

    public function test_analyzing_partitioned_datasets() : void
    {
        $report = df()
            ->read(from_text(__DIR__ . '/Fixtures/Partitioning/multi_partition_pruning_test/year=*/month=*/day=*/*.txt'))
            ->run(analyze: true);

        self::assertNotNull($report);
        self::assertSame(7, $report->statistics()->totalRows());
        self::assertEquals(
            schema(
                str_schema('year'),
                str_schema('month'),
                str_schema('day'),
                str_schema('text'),
            ),
            $report->schema()
        );
    }
}
