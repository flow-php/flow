<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration;

use PHPUnit\Framework\TestCase;

use function dirname;
use function file_get_contents;
use function Flow\Types\DSL\type_string;

final class ApiReferenceLoaderTest extends TestCase
{
    public function test_api_layout_loads_the_posthog_loader_of_the_website(): void
    {
        static::assertStringContainsString(
            '<script src="/posthog.js"></script>',
            type_string()->assert(file_get_contents(dirname(__DIR__, 7) . '/phpdoc/.phpdoc/template/layout.html.twig')),
        );
    }
}
