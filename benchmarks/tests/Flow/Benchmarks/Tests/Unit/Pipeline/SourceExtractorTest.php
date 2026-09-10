<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Pipeline;

use Flow\Benchmarks\Pipeline\OrdersSchema;
use Flow\Benchmarks\Pipeline\SchemaMode;
use Flow\Benchmarks\Pipeline\Source;
use Flow\Benchmarks\Pipeline\SourceExtractor;
use Flow\Benchmarks\Pipeline\SourceFixture;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class SourceExtractorTest extends TestCase
{
    public const ROWS = 100;

    public static function sources(): Generator
    {
        foreach (Source::cases() as $source) {
            yield $source->value => [$source];
        }
    }

    #[DataProvider('sources')]
    public function test_declared_mode_attaches_the_frozen_schema(Source $source): void
    {
        (new SourceFixture($source, self::ROWS))->warm();

        static::assertEquals(
            OrdersSchema::of($source),
            (new SourceExtractor($source, SchemaMode::declared, self::ROWS))->extractor()->schema(),
        );
    }

    /**
     * The drift guard above cannot see this branch: because the frozen literal equals live inference,
     * comparing schemas passes whether or not withSchema() was applied. Comparing the extractors
     * themselves does distinguish them - withSchema() returns a clone carrying the schema.
     */
    #[DataProvider('sources')]
    public function test_inferred_mode_attaches_no_schema(Source $source): void
    {
        (new SourceFixture($source, self::ROWS))->warm();

        static::assertNotEquals(
            (new SourceExtractor($source, SchemaMode::declared, self::ROWS))->extractor(),
            (new SourceExtractor($source, SchemaMode::inferred, self::ROWS))->extractor(),
        );
    }

    /**
     * The frozen literal only prices the sampling pass honestly while it still equals what inference
     * produces. This is the drift guard: if inference changes, this fails loudly instead of the
     * declared arm quietly measuring something else.
     */
    #[DataProvider('sources')]
    public function test_the_frozen_schema_still_equals_what_the_source_infers(Source $source): void
    {
        (new SourceFixture($source, self::ROWS))->warm();

        static::assertEquals(
            OrdersSchema::of($source),
            (new SourceExtractor($source, SchemaMode::inferred, self::ROWS))->extractor()->schema(),
        );
    }
}
