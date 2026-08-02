<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration;

use Flow\Website\Service\Examples;
use Flow\Website\StaticSourceProvider\ExamplesSourceProvider;
use NorbertTech\StaticContentGeneratorBundle\Content\Source;
use PHPUnit\Framework\TestCase;

use function array_map;
use function array_unique;
use function array_values;

final class ExamplesSourceProviderTest extends TestCase
{
    public function test_data_sources_are_emitted_for_example_with_options(): void
    {
        $sources = (new ExamplesSourceProvider(new Examples(__DIR__ . '/../Fixtures/WithDataFiles')))->all();

        static::assertContainsEquals(
            new Source('example_option_data', [
                'topic' => 'data_topic',
                'example' => 'option_example',
                'option' => 'option_1',
                'path' => 'input/nested/data.csv',
            ]),
            $sources,
        );
    }

    public function test_data_sources_are_emitted_for_example_without_options(): void
    {
        $sources = (new ExamplesSourceProvider(new Examples(__DIR__ . '/../Fixtures/WithDataFiles')))->all();

        static::assertContainsEquals(
            new Source('example_data', [
                'topic' => 'data_topic',
                'example' => 'plain_example',
                'path' => 'input/dataset.csv',
            ]),
            $sources,
        );
    }

    public function test_no_data_sources_are_emitted_when_example_ships_no_input(): void
    {
        $sources = (new ExamplesSourceProvider(new Examples(__DIR__ . '/../Fixtures/Valid')))->all();

        static::assertSame(
            ['example', 'example_playground'],
            array_values(array_unique(array_map(static fn(Source $source): string => $source->routerName(), $sources))),
        );
    }
}
