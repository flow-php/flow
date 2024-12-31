<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use function Flow\CLI\option_string_nullable;
use function Flow\Filesystem\DSL\path_real;
use Flow\ETL\Config;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Input\InputInterface;

final class ConfigOption
{
    public function __construct(private readonly string $optionName)
    {
    }

    public function get(InputInterface $input) : Config
    {
        $configPath = option_string_nullable($this->optionName, $input);

        if ($configPath === null) {
            return \Flow\ETL\DSL\config();
        }

        $path = path_real($configPath);

        $fs = new NativeLocalFilesystem();

        if ($fs->status($path) === null) {
            throw new InvalidArgumentException("File '{$path->path()}' does not exist.");
        }

        $config = require $path->path();

        if ($config instanceof Config\ConfigBuilder) {
            $config = $config->build();
        }

        if (!$config instanceof Config) {
            throw new InvalidArgumentException('File "{$path->path()}" does not return instance of "' . Config::class . '" or "' . Config\ConfigBuilder::class . '".');
        }

        return $config;
    }
}
