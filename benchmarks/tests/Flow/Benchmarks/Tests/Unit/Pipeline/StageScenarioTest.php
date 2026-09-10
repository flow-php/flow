<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Pipeline;

use Flow\Benchmarks\Datasets\Paths;
use Flow\Benchmarks\Pipeline\Source;
use Flow\Benchmarks\Pipeline\SourceFixture;
use Flow\Benchmarks\Pipeline\Stage;
use Flow\Benchmarks\Pipeline\StageScenario;
use Flow\Benchmarks\Tests\Context\WrittenFile;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

final class StageScenarioTest extends TestCase
{
    public const ROWS = 100;

    public static function laddered(): Generator
    {
        yield 'csv' => [Source::csv];

        yield 'parquet' => [Source::parquet];
    }

    #[DataProvider('laddered')]
    public function test_the_read_select_stage_runs_without_writing(Source $source): void
    {
        (new SourceFixture($source, self::ROWS))->warm();

        (new StageScenario($source, Stage::read_select, self::ROWS))->run();

        static::assertSame([], glob(Paths::var() . '/stage_' . $source->value . '_*.floe') ?: []);
    }

    #[DataProvider('laddered')]
    public function test_the_read_select_write_stage_writes_the_five_projected_columns(Source $source): void
    {
        (new SourceFixture($source, self::ROWS))->warm();

        $written = new WrittenFile(Paths::var() . '/stage_' . $source->value . '_*.floe');
        (new StageScenario($source, Stage::read_select_write, self::ROWS))->run();

        $rows = data_frame()->read(from_floe($written->path()))->fetch();
        $written->remove();

        static::assertCount(self::ROWS, $rows);
        static::assertSame(['order_id', 'seller_id', 'created_at', 'customer', 'email'], $rows->first()->names());
    }
}
