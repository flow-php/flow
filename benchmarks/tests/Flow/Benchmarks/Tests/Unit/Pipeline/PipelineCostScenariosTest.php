<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Pipeline;

use Flow\Benchmarks\Pipeline\ColumnMix;
use Flow\Benchmarks\Pipeline\ColumnMixScenario;
use Flow\Benchmarks\Pipeline\FetchScenario;
use Flow\Benchmarks\Pipeline\LimitPushdownScenario;
use Flow\Benchmarks\Pipeline\OrdersSchema;
use Flow\Benchmarks\Pipeline\PipelineWidthScenario;
use Flow\Benchmarks\Pipeline\PlanDepthScenario;
use Flow\Benchmarks\Pipeline\Source;
use Flow\Benchmarks\Pipeline\SourceFixture;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_keys;
use function array_slice;

final class PipelineCostScenariosTest extends TestCase
{
    public const ROWS = 100;

    public static function mixes(): Generator
    {
        foreach (ColumnMix::cases() as $mix) {
            yield $mix->value => [$mix];
        }
    }

    public static function widths(): Generator
    {
        foreach ([4, 8, 11] as $width) {
            yield (string) $width => [$width];
        }
    }

    public static function depths(): Generator
    {
        foreach ([1, 10, 50] as $depth) {
            yield (string) $depth => [$depth];
        }
    }

    public static function limits(): Generator
    {
        yield 'none' => [null];

        yield '10' => [10];
    }

    #[DataProvider('mixes')]
    public function test_the_column_mix_scenario_projects_exactly_its_mix(ColumnMix $mix): void
    {
        (new SourceFixture(Source::csv, self::ROWS))->warm();
        $scenario = new ColumnMixScenario(Source::csv, $mix, self::ROWS);
        $scenario->run();

        static::assertSame($mix->columns(), $scenario->columns());
    }

    #[DataProvider('widths')]
    public function test_the_width_scenario_projects_exactly_that_many_columns(int $width): void
    {
        (new SourceFixture(Source::csv, self::ROWS))->warm();
        $scenario = new PipelineWidthScenario(Source::csv, $width, self::ROWS);
        $scenario->run();

        static::assertCount($width, $scenario->columns());
        static::assertSame(
            array_slice(array_keys(OrdersSchema::of(Source::csv)->definitions()), 0, $width),
            $scenario->columns(),
        );
    }

    #[DataProvider('depths')]
    public function test_the_plan_depth_scenario_runs(int $depth): void
    {
        (new SourceFixture(Source::csv, self::ROWS))->warm();
        static::assertCount(
            count(OrdersSchema::of(Source::csv)->definitions()) + $depth,
            (new PlanDepthScenario(Source::csv, $depth, self::ROWS))->run()->definitions(),
        );
    }

    public function test_the_fetch_scenario_runs(): void
    {
        (new SourceFixture(Source::csv, self::ROWS))->warm();
        $rows = (new FetchScenario(Source::csv, self::ROWS))->run();

        static::assertCount(self::ROWS, $rows);
        static::assertSame(['order_id', 'customer'], $rows->first()->names());
    }

    #[DataProvider('limits')]
    public function test_the_limit_pushdown_scenario_keeps_its_limit_in_the_plan(?int $limit): void
    {
        (new SourceFixture(Source::csv, self::ROWS))->warm();
        $scenario = new LimitPushdownScenario(Source::csv, $limit, self::ROWS);
        $scenario->run();

        static::assertSame($limit, $scenario->limit());
    }
}
