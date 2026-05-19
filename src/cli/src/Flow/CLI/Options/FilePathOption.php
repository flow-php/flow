<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Flow\ETL\Config;
use Flow\Filesystem\Path;
use InvalidArgumentException;
use Symfony\Component\Console\Input\InputInterface;

use function Flow\CLI\option_string;
use function Flow\Filesystem\DSL\path_real;

final readonly class FilePathOption
{
    public function __construct(
        private string $path,
    ) {}

    public function get(InputInterface $input): Path
    {
        return path_real(option_string($this->path, $input));
    }

    public function getExisting(InputInterface $input, Config $config): Path
    {
        $path = path_real(option_string($this->path, $input));

        if ($config->fstab()->for($path)->status($path) === null) {
            throw new InvalidArgumentException("File '{$path->path()}' does not exist.");
        }

        return $path;
    }

    public function getNotExisting(InputInterface $input, Config $config): Path
    {
        $path = path_real(option_string($this->path, $input));

        if ($config->fstab()->for($path)->status($path) !== null) {
            throw new InvalidArgumentException("File '{$path->path()}' already exist.");
        }

        return $path;
    }
}
