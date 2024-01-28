<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class AnalyzeTest extends IntegrationTestCase
{
    public function test_analyzing_csv_file_with_auto_cast() : void
    {
        $report = df()
            ->read(from_csv(__DIR__ . '/Fixtures/Analyze/goldstock.csv'))
            ->autoCast()
            ->run(analyze: true);

        $this->assertSame(2511, $report->statistics()->totalRows());
        $this->assertEquals(
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
        $this->assertSame(7, $report->schema()->count());
    }

    public function test_analyzing_csv_file_with_limit() : void
    {
        $report = df()
            ->read(from_csv(__DIR__ . '/Fixtures/Analyze/goldstock.csv'))
            ->limit(100)
            ->run(analyze: true);

        $this->assertSame(100, $report->statistics()->totalRows());
        $this->assertEquals(
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
        $this->assertSame(7, $report->schema()->count());
    }

    public function test_analyzing_partitioned_datasets() : void
    {
        $report = df()
            ->read(from_text(__DIR__ . '/Fixtures/Partitioning/multi_partition_pruning_test/year=*/month=*/day=*/*.txt'))
            ->run(analyze: true);

        $this->assertSame(7, $report->statistics()->totalRows());
        $this->assertEquals(
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
