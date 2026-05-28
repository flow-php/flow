<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Flow\ETL\Config;
use Flow\ETL\Config\ConfigBuilder;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Input\InputInterface;

use function Flow\CLI\option_string_nullable;
use function Flow\ETL\DSL\config;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_instance_of;

final readonly class ConfigOption
{
    public function __construct(
        private string $optionName,
    ) {}

    public function get(InputInterface $input): Config
    {
        $configPath = option_string_nullable($this->optionName, $input);

        if ($configPath === null) {
            return config();
        }

        $path = path_real($configPath);

        $fs = new NativeLocalFilesystem();

        if ($fs->status($path) === null) {
            throw new InvalidArgumentException("File '{$path->path()}' does not exist.");
        }

        // @mago-expect analysis:mixed-assignment
        $result = require $path->path();

        if ($result instanceof ConfigBuilder) {
            return $result->build();
        }

        return type_instance_of(Config::class)->assert($result);
    }
}
