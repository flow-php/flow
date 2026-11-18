<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration;

use Flow\Website\Service\Documentation\Pages;
use Flow\Website\StaticSourceProvider\GuidesProvider;
use NorbertTech\StaticContentGeneratorBundle\Content\Source;
use PHPUnit\Framework\TestCase;

use function dirname;

final class GuidesProviderTest extends TestCase
{
    public function test_every_guide_is_a_source_of_the_static_build(): void
    {
        static::assertContainsEquals(
            new Source('guides_guide', ['slug' => 'read-large-csv-file-in-php']),
            (new GuidesProvider(new Pages(dirname(__DIR__, 5) . '/content/guides')))->all(),
        );
    }
}
