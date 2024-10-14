<?php

declare(strict_types=1);

namespace Flow\CLI\Arguments;

use function Flow\CLI\argument_string;
use function Flow\Filesystem\DSL\path_real;
use Flow\ETL\Config;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final class FilePathArgument
{
    public function __construct(private readonly string $path)
    {
    }

    public function getExisting(InputInterface $input, Config $config) : Path
    {
        $path = path_real(argument_string($this->path, $input));

        if ($config->fstab()->for($path)->status($path) === null) {
            throw new \InvalidArgumentException("File '{$path->path()}' does not exist.");
        }

        return $path;
    }
}
