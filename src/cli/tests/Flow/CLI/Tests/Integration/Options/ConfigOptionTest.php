<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration\Options;

use Flow\CLI\Options\ConfigOption;
use Flow\ETL\Config;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputOption;

final class ConfigOptionTest extends TestCase
{
    public function test_getting_config_from_option(): void
    {
        $option = new InputOption('config', null, InputOption::VALUE_REQUIRED);
        $definition = new InputDefinition([$option]);

        $config = (new ConfigOption('config'))->get(new ArrayInput([
            '--config' => __DIR__ . '/Fixtures/.flow.config.php',
        ], $definition));

        static::assertInstanceOf(Config::class, $config);
        static::assertSame('execution-id', $config->id());
    }

    public function test_getting_default_config(): void
    {
        $option = new InputOption('config', null, InputOption::VALUE_OPTIONAL);
        $definition = new InputDefinition([$option]);

        $config = (new ConfigOption('config'))->get(new ArrayInput([], $definition));

        static::assertInstanceOf(Config::class, $config);
        static::assertNotSame('execution-id', $config->id());
    }

    public function test_throwing_exception_when_config_file_does_not_exist(): void
    {
        $option = new InputOption('config', null, InputOption::VALUE_REQUIRED);
        $definition = new InputDefinition([$option]);

        $this->expectException(InvalidArgumentException::class);

        (new ConfigOption('config'))->get(new ArrayInput(['--config' => 'non-existing-file.php'], $definition));
    }
}
