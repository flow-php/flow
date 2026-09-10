<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Schema;

use Flow\Benchmarks\Schema\FooterSchemaScenario;
use Flow\Benchmarks\Schema\RowsSchemaScenario;
use Flow\Benchmarks\Schema\SchemaInferenceScenario;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class SchemaScenariosTest extends TestCase
{
    public const ROWS = 100;

    public static function batches(): Generator
    {
        foreach ([1, 10, 100] as $batch) {
            yield (string) $batch => [$batch];
        }
    }

    #[DataProvider('batches')]
    public function test_the_rows_scenario_builds_the_requested_batch_size(int $batch): void
    {
        RowsSchemaScenario::clear();
        $scenario = new RowsSchemaScenario($batch);
        $scenario->warm();

        static::assertCount($batch, $scenario->rows());
    }

    #[DataProvider('batches')]
    public function test_the_rows_scenario_memoises_its_batch(int $batch): void
    {
        RowsSchemaScenario::clear();
        $scenario = new RowsSchemaScenario($batch);

        static::assertSame($scenario->rows(), $scenario->rows());
    }

    public function test_the_footer_and_inference_scenarios_agree_on_the_schema(): void
    {
        static::assertEquals(
            (new FooterSchemaScenario(self::ROWS))->run(),
            (new SchemaInferenceScenario(self::ROWS))->run(),
        );
    }
}
