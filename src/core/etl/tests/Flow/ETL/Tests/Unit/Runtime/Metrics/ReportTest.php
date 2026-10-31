<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Metrics;

use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Runtime\Metrics\Processes;
use Flow\ETL\Runtime\Metrics\SourceStatistics;
use Flow\ETL\Runtime\Metrics\Statistics\Columns;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ColumnMother;
use Flow\ETL\Tests\Mother\ReportMother;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Memory\Unit;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function json_decode;
use function json_encode;

use const INF;
use const JSON_THROW_ON_ERROR;
use const NAN;

final class ReportTest extends FlowTestCase
{
    public function test_normalize_holds_what_the_typed_getters_hold_and_survives_json(): void
    {
        $source = new SourceStatistics('ArrayExtractor', new Statistics(Cardinality::exact(2)), 2, true);
        $report = ReportMother::of(new Processes(Unit::fromBytes(25), [Unit::fromBytes(
            40,
        )]), 2, schema(int_schema('id')), [$source]);

        $normalized = $report->normalize();

        static::assertSame(
            [
                'schema' => schema(int_schema('id'))->normalize(),
                'statistics' => [
                    'total_rows' => 2,
                    'execution_time' => $report->statistics()->executionTime->normalize(),
                    'heap' => $report->statistics()->heap->normalize(),
                    'processes' => ['parent' => 25, 'workers' => [40], 'worker_peak' => 40, 'peak_sum' => 65],
                    'columns' => null,
                ],
                'sources' => [$source->normalize()],
            ],
            $normalized,
        );
        // JSON keeps the values, not a float's ".0"
        static::assertEquals($normalized, json_decode(
            json_encode($normalized, JSON_THROW_ON_ERROR),
            true,
            flags: JSON_THROW_ON_ERROR,
        ));
    }

    public function test_a_report_without_schema_or_sources_normalizes_them_to_null(): void
    {
        $normalized = ReportMother::of(new Processes(Unit::fromBytes(25), []))->normalize();

        static::assertNull($normalized['schema']);
        static::assertNull($normalized['sources']);
    }

    public function test_a_report_holding_infinite_and_nan_values_encodes_as_json(): void
    {
        $definition = float_schema('f');
        $columns = new Columns();
        $columns->add($definition, ColumnMother::of($definition, [INF, NAN, -INF]));
        $report = ReportMother::of(
            new Processes(Unit::fromBytes(25), []),
            columns: $columns,
            sources: [new SourceStatistics('ArrayExtractor', new Statistics(new Cardinality(null, 5, INF)), 0, true)],
        );

        $json = json_encode($report->normalize(), JSON_THROW_ON_ERROR);

        static::assertStringContainsString('"rows_error":null', $json);
        static::assertStringContainsString('"relative_error":null', $json);
        static::assertStringContainsString('"max":"', $json);
    }
}
