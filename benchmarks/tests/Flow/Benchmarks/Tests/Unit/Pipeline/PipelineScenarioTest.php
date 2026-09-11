<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Pipeline;

use Flow\Benchmarks\Datasets\Paths;
use Flow\Benchmarks\Pipeline\PipelineScenario;
use Flow\Benchmarks\Pipeline\SchemaMode;
use Flow\Benchmarks\Pipeline\Source;
use Flow\Benchmarks\Pipeline\SourceFixture;
use Flow\Benchmarks\Tests\Context\WrittenFile;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

final class PipelineScenarioTest extends TestCase
{
    public const ROWS = 100;

    public static function sources(): Generator
    {
        foreach (Source::cases() as $source) {
            yield $source->value => [$source];
        }
    }

    #[DataProvider('sources')]
    public function test_it_writes_the_five_projected_columns_for_every_row(Source $source): void
    {
        (new SourceFixture($source, self::ROWS))->warm();

        $written = new WrittenFile(Paths::var() . '/pipeline_' . $source->value . '_*.floe');
        (new PipelineScenario($source, SchemaMode::declared, self::ROWS))->run();

        $rows = data_frame()->read(from_floe($written->path()))->fetch();
        $written->remove();

        static::assertCount(self::ROWS, $rows);
        static::assertSame(['order_id', 'seller_id', 'created_at', 'customer', 'contact'], $rows->first()->names());
    }

    #[DataProvider('sources')]
    public function test_the_inferred_and_declared_arms_produce_the_same_columns(Source $source): void
    {
        (new SourceFixture($source, self::ROWS))->warm();

        $declaredFile = new WrittenFile(Paths::var() . '/pipeline_' . $source->value . '_*.floe');
        (new PipelineScenario($source, SchemaMode::declared, self::ROWS))->run();
        $declared = data_frame()->read(from_floe($declaredFile->path()))->fetch()->first()->names();
        $declaredFile->remove();

        $inferredFile = new WrittenFile(Paths::var() . '/pipeline_' . $source->value . '_*.floe');
        (new PipelineScenario($source, SchemaMode::inferred, self::ROWS))->run();
        $inferred = data_frame()->read(from_floe($inferredFile->path()))->fetch()->first()->names();
        $inferredFile->remove();

        static::assertSame($declared, $inferred);
    }
}
