<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidFileFormatException;

final class PipelineFactory
{
    public function __construct(
        private readonly string $filename,
    ) {
    }

    public function run() : void
    {
        if (!\file_exists($this->filename)) {
            throw InvalidArgumentException::because("Input file ({$this->filename}) doesn't exists!");
        }

        if (!\str_ends_with($this->filename, '.php')) {
            throw new InvalidFileFormatException('php', \pathinfo($this->filename, \PATHINFO_EXTENSION));
        }

        $resource = \fopen($this->filename, 'rb');

        if (false === $resource) {
            throw InvalidArgumentException::because('Input file cannot be read!');
        }

        $content = \trim(\fread($resource, 5) ?: '');
        \fclose($resource);

        if (!\str_contains($content, '<?php')) {
            throw InvalidArgumentException::because('Input file must be a valid PHP one!');
        }

        $pipeline = include $this->filename;

        if (!$pipeline instanceof DataFrame) {
            throw InvalidArgumentException::because('Expecting Flow-PHP DataFrame, received: ' . (\is_object($pipeline) ? $pipeline::class : \gettype($pipeline)));
        }

        $pipeline->run();
    }
}
