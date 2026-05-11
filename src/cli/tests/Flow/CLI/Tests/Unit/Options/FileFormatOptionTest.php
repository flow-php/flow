<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Unit\Options;

use Flow\CLI\Options\FileFormat;
use Flow\CLI\Options\FileFormatOption;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputOption;

use function Flow\Filesystem\DSL\path;

final class FileFormatOptionTest extends TestCase
{
    public function test_getting_format_from_option_for_path_with_extension(): void
    {
        $option = new InputOption('format', null, InputOption::VALUE_OPTIONAL);
        $definition = new InputDefinition([$option]);

        static::assertSame(FileFormat::JSON, (new FileFormatOption(path(__DIR__ . '/file.csv'), 'format'))->get(
            new ArrayInput(['--format' => 'json'], $definition),
        ));
    }

    public function test_getting_format_from_option_for_path_without_extension(): void
    {
        $option = new InputOption('format', null, InputOption::VALUE_OPTIONAL);
        $definition = new InputDefinition([$option]);

        static::assertSame(FileFormat::JSON, (new FileFormatOption(path(__DIR__ . '/file'), 'format'))->get(
            new ArrayInput(['--format' => 'json'], $definition),
        ));
    }

    public function test_getting_format_from_path(): void
    {
        $option = new InputOption('format', null, InputOption::VALUE_OPTIONAL);
        $definition = new InputDefinition([$option]);

        static::assertSame(
            FileFormat::CSV,
            (new FileFormatOption(path(__DIR__ . '/file.csv'), 'format'))->get(new ArrayInput([], $definition)),
        );
    }

    public function test_getting_format_from_path_without_extension(): void
    {
        $option = new InputOption('format', null, InputOption::VALUE_OPTIONAL);
        $definition = new InputDefinition([$option]);

        $this->expectExceptionMessage("Option 'format' is required");
        (new FileFormatOption(path(__DIR__ . '/file'), 'format'))->get(new ArrayInput([], $definition));
    }
}
