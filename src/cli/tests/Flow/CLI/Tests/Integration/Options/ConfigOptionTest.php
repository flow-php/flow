<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration\Options;

use Flow\CLI\Options\ConfigOption;
use Flow\ETL\API\Config\ConfigBuilder;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputOption;

use function sprintf;

final class ConfigOptionTest extends TestCase
{
    public function test_getting_config_from_option(): void
    {
        $option = new InputOption('config', null, InputOption::VALUE_REQUIRED);
        $definition = new InputDefinition([$option]);

        $config = (new ConfigOption('config'))->get(new ArrayInput([
            '--config' => __DIR__ . '/Fixtures/.flow.config.php',
        ], $definition));

        static::assertInstanceOf(ConfigBuilder::class, $config);
        static::assertSame('execution-id', $config->build()->id());
    }

    public function test_getting_default_config(): void
    {
        $option = new InputOption('config', null, InputOption::VALUE_OPTIONAL);
        $definition = new InputDefinition([$option]);

        $config = (new ConfigOption('config'))->get(new ArrayInput([], $definition));

        static::assertInstanceOf(ConfigBuilder::class, $config);
        static::assertNotSame('execution-id', $config->build()->id());
    }

    public function test_throwing_exception_when_config_file_returns_a_built_config(): void
    {
        $option = new InputOption('config', null, InputOption::VALUE_REQUIRED);
        $definition = new InputDefinition([$option]);
        $path = __DIR__ . '/Fixtures/.flow.built-config.php';

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf(
            "File '%s' returns a built Config. Return the builder instead: return config_builder()->...; without ->build().",
            $path,
        ));

        (new ConfigOption('config'))->get(new ArrayInput(['--config' => $path], $definition));
    }

    public function test_throwing_exception_when_config_file_does_not_exist(): void
    {
        $option = new InputOption('config', null, InputOption::VALUE_REQUIRED);
        $definition = new InputDefinition([$option]);

        $this->expectException(InvalidArgumentException::class);

        (new ConfigOption('config'))->get(new ArrayInput(['--config' => 'non-existing-file.php'], $definition));
    }
}
