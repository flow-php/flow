<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Unit\Options;

use function Flow\Filesystem\DSL\path;
use Flow\CLI\Options\{FileFormat, FileFormatOption};
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Symfony\Component\Console\Input\{ArrayInput, InputDefinition, InputOption};

final class FileFormatOptionTest extends TestCase
{
    public function test_getting_format_from_option_for_path_with_extension() : void
    {
        $option = new InputOption('format', null, InputOption::VALUE_OPTIONAL);
        $definition = new InputDefinition([$option]);

        self::assertSame(FileFormat::JSON, (new FileFormatOption(path(__DIR__ . '/file.csv'), 'format'))->get(new ArrayInput(['--format' => 'json'], $definition)));
    }

    public function test_getting_format_from_option_for_path_without_extension() : void
    {
        $option = new InputOption('format', null, InputOption::VALUE_OPTIONAL);
        $definition = new InputDefinition([$option]);

        self::assertSame(FileFormat::JSON, (new FileFormatOption(path(__DIR__ . '/file'), 'format'))->get(new ArrayInput(['--format' => 'json'], $definition)));
    }

    public function test_getting_format_from_path() : void
    {
        $option = new InputOption('format', null, InputOption::VALUE_OPTIONAL);
        $definition = new InputDefinition([$option]);

        self::assertSame(FileFormat::CSV, (new FileFormatOption(path(__DIR__ . '/file.csv'), 'format'))->get(new ArrayInput([], $definition)));
    }

    public function test_getting_format_from_path_without_extension() : void
    {
        $option = new InputOption('format', null, InputOption::VALUE_OPTIONAL);
        $definition = new InputDefinition([$option]);

        $this->expectExceptionMessage("Option 'format' is required");
        (new FileFormatOption(path(__DIR__ . '/file'), 'format'))->get(new ArrayInput([], $definition));
    }
}
