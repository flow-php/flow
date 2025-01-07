<?php

declare(strict_types=1);

namespace Flow\CLI;

use Flow\ETL\Exception\{InvalidArgumentException, InvalidFileFormatException};
use Flow\ETL\{Config, DataFrame};
use Flow\Filesystem\Path;

final readonly class PipelineFactory
{
    public function __construct(
        private Path $path,
        private Config $config,
    ) {
    }

    public function fromJson() : DataFrame
    {
        if ($this->path->extension() !== 'json') {
            throw new InvalidFileFormatException('json', $this->path->extension() === false ? 'unknown' : $this->path->extension());
        }

        $fs = $this->config->fstab()->for($this->path);

        return DataFrame::fromJson($fs->readFrom($this->path)->content());
    }

    public function fromPHP() : DataFrame
    {
        if ($this->path->extension() !== 'php') {
            throw new InvalidFileFormatException('php', $this->path->extension() === false ? 'unknown' : $this->path->extension());
        }

        $pipeline = include $this->path->path();

        if (!$pipeline instanceof DataFrame) {
            throw InvalidArgumentException::because('Expecting Flow-PHP DataFrame, received: ' . (get_debug_type($pipeline)));
        }

        return $pipeline;
    }
}
