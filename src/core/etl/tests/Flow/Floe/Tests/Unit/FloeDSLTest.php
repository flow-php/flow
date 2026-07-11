<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\FloeExtractor;
use Flow\Floe\FloeLoader;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;

final class FloeDSLTest extends TestCase
{
    public function test_from_floe_builds_extractor_from_path(): void
    {
        $extractor = from_floe(path('memory://a.floe'));

        static::assertInstanceOf(FloeExtractor::class, $extractor);
        static::assertSame('memory://a.floe', $extractor->source()->uri());
    }

    public function test_from_floe_resolves_string_path(): void
    {
        $extractor = from_floe(__DIR__ . '/example.floe');

        static::assertInstanceOf(FloeExtractor::class, $extractor);
        static::assertStringEndsWith('example.floe', $extractor->source()->path());
    }

    public function test_to_floe_builds_loader_from_path(): void
    {
        $loader = to_floe(path('memory://b.floe'));

        static::assertInstanceOf(FloeLoader::class, $loader);
        static::assertSame('memory://b.floe', $loader->destination()->uri());
    }

    public function test_to_floe_resolves_string_path(): void
    {
        $loader = to_floe(__DIR__ . '/out.floe');

        static::assertInstanceOf(FloeLoader::class, $loader);
        static::assertStringEndsWith('out.floe', $loader->destination()->path());
    }
}
