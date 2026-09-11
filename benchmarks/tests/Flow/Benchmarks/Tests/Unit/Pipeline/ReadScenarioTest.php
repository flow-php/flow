<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Pipeline;

use Flow\Benchmarks\Pipeline\ReadScenario;
use Flow\Benchmarks\Pipeline\SchemaMode;
use Flow\Benchmarks\Pipeline\Source;
use Flow\Benchmarks\Pipeline\SourceExtractor;
use Flow\Benchmarks\Pipeline\SourceFixture;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ReadScenarioTest extends TestCase
{
    public const ROWS = 100;

    public static function sources(): Generator
    {
        foreach (Source::cases() as $source) {
            yield $source->value => [$source];
        }
    }

    #[DataProvider('sources')]
    public function test_it_runs_for_every_source(Source $source): void
    {
        (new SourceFixture($source, self::ROWS))->warm();
        (new ReadScenario($source, self::ROWS))->run();

        static::assertTrue(true);
    }

    /**
     * Every published ladder delta is r2-r1 or r3-r2, which only subtracts if rung 1 reads through the
     * same declared extractor as the rungs above it.
     */
    #[DataProvider('sources')]
    public function test_it_reads_through_the_same_declared_extractor_as_the_other_rungs(Source $source): void
    {
        (new SourceFixture($source, self::ROWS))->warm();

        static::assertEquals(
            (new SourceExtractor($source, SchemaMode::declared, self::ROWS))->extractor(),
            (new ReadScenario($source, self::ROWS))->extractor(),
        );
    }
}
